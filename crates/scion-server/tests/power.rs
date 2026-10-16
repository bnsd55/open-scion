//! Power-loss suite: the real `scion-server` binary writes to an ext4 file
//! system on a loop device behind a `dm-flakey` table, and the test cuts
//! power by dropping, from one moment on, every write that the page cache
//! sends to the device. What a reply acknowledged was synced before the
//! reply went out, so it must survive the cut; what was still only in the
//! page cache is lost. After each cut, the ext4 journal replays and the
//! store reopens, and [`check`] (shared with the crash suite) checks the
//! same invariants as after a SIGKILL.
//!
//! The model: a cut loses the writes that the page cache had not sent to
//! the device yet, so the suite tests sync-before-reply. It does not model
//! a drive's volatile cache, which can also lose writes that reached the
//! device before the cut but were never flushed. The stronger model keeps
//! every write (dm-log-writes) and replays it to each FLUSH point.
//!
//! SHORTCUT: the cut is sync-before-reply, and the sync path tested is the
//! Linux one; macOS syncs differently (`F_FULLFSYNC`) and its CI job has no
//! device mapper, so there the crash suite is what runs. Upgrade to
//! dm-log-writes replay to each FLUSH point when a report says that
//! acknowledged writes were lost on hardware with a volatile cache.
//!
//! Needs root rights, which it takes through passwordless `sudo` (losetup,
//! dmsetup, mount, mkfs.ext4, fsck.ext4). It runs only when
//! `SCION_POWER_REQUIRE=1` is set, so `cargo test --workspace` skips it
//! with a message; its CI job sets the variable, and then a missing right
//! fails, not skips. As in the crash suite, `SCION_POWER_SEED=<n>` replays
//! a run and `SCION_POWER_ROUNDS=<n>` sets the number of cuts (default 4).
#![cfg(all(unix, target_os = "linux"))]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use common::*;

/// Whether the test may run: only with `SCION_POWER_REQUIRE=1`, its CI
/// job's mark. A run without root rights then fails, so the job cannot
/// pass by skipping.
fn may_run() -> bool {
    if std::env::var("SCION_POWER_REQUIRE").as_deref() != Ok("1") {
        println!("skipping the power-loss suite: set SCION_POWER_REQUIRE=1 to run it");
        return false;
    }
    if !root() {
        let ok = Command::new("sudo")
            .arg("-n")
            .arg("true")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        assert!(
            ok,
            "no root rights through sudo (need losetup, dmsetup, mount, mkfs.ext4, fsck.ext4)"
        );
    }
    true
}

/// Whether this process is root already.
fn root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false)
}

/// The command to run `program` with root rights: itself when already root,
/// else through passwordless `sudo`.
fn priv_cmd(program: &str) -> Command {
    if root() {
        return Command::new(program);
    }
    let mut sudo = Command::new("sudo");
    sudo.arg("-n").arg(program);
    sudo
}

/// Runs a privileged command, and panics with its output if it fails.
fn run(program: &str, args: &[&str]) {
    let out = priv_cmd(program).args(args).output().unwrap();
    if !out.status.success() {
        panic!(
            "{program} {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// The trimmed output of a privileged command.
fn run_out(program: &str, args: &[&str]) -> String {
    let out = priv_cmd(program).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{program} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// One ext4 image on a loop device, given to the store through a
/// device-mapper table, and its mount point.
struct Device {
    image: PathBuf,
    mount: PathBuf,
    loop_dev: String,
    name: String,
}

impl Device {
    /// Creates a fresh ext4 on a loop device, mounts it through a plain
    /// table, and gives the mount to the current user, so the server can
    /// create its data directory in it. `round` keeps the names apart.
    fn create(dir: &Path, round: usize) -> Self {
        let sub = dir.join(format!("r{round}"));
        fs::create_dir(&sub).unwrap();
        let image = sub.join("ext4.img");
        // A sparse file; the ext4 of one round holds far less than this.
        fs::File::create(&image)
            .unwrap()
            .set_len(512 << 20)
            .unwrap();
        let image = image.to_str().unwrap();
        run("mkfs.ext4", &["-q", "-F", image]);
        let loop_dev = run_out("losetup", &["--find", "--show", image]);
        let sectors = run_out("blockdev", &["--getsz", &loop_dev]);
        let name = format!("scion-power-{round}");
        run(
            "dmsetup",
            &[
                "create",
                &name,
                "--table",
                &format!("0 {sectors} linear {loop_dev} 0"),
            ],
        );
        let mount = sub.join("mnt");
        fs::create_dir(&mount).unwrap();
        let mount = mount.to_str().unwrap();
        run(
            "mount",
            &["-t", "ext4", &format!("/dev/mapper/{name}"), mount],
        );
        give_to_user(mount);
        Self {
            image: PathBuf::from(image),
            mount: PathBuf::from(mount),
            loop_dev,
            name,
        }
    }

    /// The cut: from now on the device drops every write. The table says
    /// one second up, then a day of dropped writes; reads keep working, so
    /// the server sees success for writes it never synced.
    fn cut(&self) {
        let sectors = run_out("blockdev", &["--getsz", &self.loop_dev]);
        let table = format!("0 {sectors} flakey {} 0 1 86400 drop_writes", self.loop_dev);
        run("dmsetup", &["suspend", &self.name]);
        run("dmsetup", &["load", &self.name, "--table", &table]);
        run("dmsetup", &["resume", &self.name]);
    }

    /// After the cut: unmount (the writes it still had are dropped, as at a
    /// power cut), drop the device, replay the ext4 journal, and mount the
    /// image again on a fresh loop device, for the store to reopen.
    fn remount(&self) {
        let image = self.image.to_str().unwrap();
        let mount = self.mount.to_str().unwrap();
        run("umount", &[mount]);
        run("dmsetup", &["remove", &self.name]);
        run("losetup", &["-d", &self.loop_dev]);
        run("fsck.ext4", &["-y", image]);
        run("mount", &["-t", "ext4", "-o", "loop", image, mount]);
        give_to_user(mount);
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(self.mount.parent().unwrap());
    }
}

/// Gives a mount point, which `mount` made root's, to the current user.
fn give_to_user(mount: &str) {
    let uid = run_out_plain("id", &["-u"]);
    let gid = run_out_plain("id", &["-g"]);
    run("chown", &[&format!("{uid}:{gid}"), mount]);
}

/// Like [`run_out`], but without root rights.
fn run_out_plain(program: &str, args: &[&str]) -> String {
    let out = Command::new(program).args(args).output().unwrap();
    assert!(out.status.success(), "{program} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// `SCION_POWER_SEED`, or a new seed. Printed, so a failed run can be run
/// again with it.
fn seed() -> u64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let seed = env(
        "SCION_POWER_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    println!("seed {seed}; replay with SCION_POWER_SEED={seed}");
    seed
}

/// The real server, several clients, and a power cut at three kinds of
/// moment, as in the crash suite: at a random moment, in the middle of a
/// large upload, and while a land is pending. Every reply the clients got
/// must survive: the cut takes what the page cache had not sent, so a
/// missing sync before a reply loses that reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reply_survives_a_power_cut() {
    if !may_run() {
        return;
    }
    let mut rng = Rng::new(seed());
    let rounds = usize::try_from(env("SCION_POWER_ROUNDS", 4)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("server.log");
    let mut ledger = Ledger::default();
    let mut checked = Checked::default();
    let mut cut_lands = 0;
    for round in 0..rounds {
        let dev = Device::create(dir.path(), round);
        let data = dev.mount.join("data");
        create_repo(&data);
        let server = Server::start(&data, &log);
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        let signals = Arc::new(Signals::default());
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let name = format!("p{round}c{c}");
                let rng = Rng::new(rng.next());
                tokio::spawn(client(server.url.clone(), name, rng, Arc::clone(&signals)))
            })
            .collect();
        match rng.below(3) {
            0 => {
                let run = 100 + rng.below(500);
                tokio::time::sleep(Duration::from_millis(u64::try_from(run).unwrap())).await;
            }
            1 => {
                wait_until(|| signals.large_uploads.load(Ordering::SeqCst) > 0).await;
                let delay = u64::try_from(rng.below(30)).unwrap();
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            _ => {
                let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
                wait_until(|| pending_lands(&db) > 0).await;
            }
        }
        signals.killed.store(true, Ordering::SeqCst);
        dev.cut();
        drop(server);
        for client in clients {
            let mut got = client.await.unwrap();
            ledger.blobs.append(&mut got.blobs);
            cut_lands += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        dev.remount();
        let server = Server::start(&data, &log);
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        drop(server);
    }
    println!(
        "{rounds} cuts; {} lands, {cut_lands} of them cut mid-call",
        ledger.lands.len(),
    );
    assert!(ledger.lands.len() > rounds, "the clients did little work");
}
