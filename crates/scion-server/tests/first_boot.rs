//! The first-boot runbook (docs/operations.md, "First boot"): the three
//! commands of a first start, in order, and what each promises - the
//! binary creates the data directory and serves, the health service
//! answers for its services and for liveness, and the first import
//! makes a repo the client reads.

mod common;

use common::*;
use std::os::unix::fs::PermissionsExt as _;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// The health service answers `SERVING` for the service name a probe
/// asks for, and for the empty name.
async fn health(url: &str, service: &str) -> ServingStatus {
    let channel = Channel::from_shared(url.to_string())
        .unwrap()
        .connect()
        .await
        .expect("the server listens");
    let mut client = HealthClient::new(channel);
    let reply = client
        .check(tonic::Request::new(HealthCheckRequest {
            service: service.to_owned(),
        }))
        .await
        .expect("the health service answers")
        .into_inner();
    ServingStatus::try_from(reply.status).expect("a known status")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_boot_serves_probes_and_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));

    // Command 1: the first boot refuses a directory that does not
    // exist (D25): it creates nothing and says so. The token went in a
    // file (the runbook's umask 077 and printf), not in argv.
    assert!(!data.exists(), "the directory does not exist yet");
    // A 0600 token file of our own: the serve path checks the token
    // before the data directory, so the refusal this run proves is the
    // data one, not the token's.
    let token = dir.path().join("first-boot-token");
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&token)
            .unwrap()
            .write_all(b"first-boot\n")
            .unwrap();
    }
    let refused = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap(), "serve", "--token-file"])
        .arg(&token)
        .env("NO_COLOR", "1")
        .output()
        .expect("the refused first boot runs");
    assert!(
        !refused.status.success(),
        "a missing data directory is refused"
    );
    let why = String::from_utf8_lossy(&refused.stderr);
    assert!(
        why.contains("does not exist; create it or pass --data"),
        "the refusal names the directory: {why}"
    );
    assert!(!data.exists(), "the refusal created nothing");

    // Command 1b: the operator makes the directory (0700, the store's
    // own rule), and the first boot serves. The runbook's umask 077
    // makes the mkdir private.
    std::fs::create_dir(&data).unwrap();
    let server = Server::start_on("First boot", &data, &log);
    // The one-server section's command is the same command: the guide's
    // own serve line, in both sections' words, is this spawn's argv.
    assert_eq!(
        Server::guide_argv("One server", &data),
        Server::guide_argv("First boot", &data),
        "the one-server command and the first boot's serve line agree"
    );
    let mode = std::fs::metadata(common::token_file())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "the token file is private");

    // Command 2: the readiness probe of the service, and the liveness
    // probe of the empty name.
    let named = health(&server.url, "scion.v1.Scion").await;
    assert_eq!(named, ServingStatus::Serving, "readiness is serving");
    let empty = health(&server.url, "").await;
    assert_eq!(empty, ServingStatus::Serving, "liveness is serving");

    // Command 3: the first import answers a repo the client can read.
    // A fresh server holds no repo yet; the operator's first import
    // makes one, beside the live server.
    let git_repo = dir.path().join("git");
    std::fs::create_dir_all(&git_repo).unwrap();
    git(&git_repo, &["init", "-q", "-b", "main"]);
    std::fs::write(git_repo.join("FIRST"), b"first boot\n").unwrap();
    git(&git_repo, &["add", "FIRST"]);
    git(&git_repo, &["commit", "-q", "-m", "first\n"]);
    let out = common::spawn_guard::across_spawn(|| {
        // The server marked this directory's metadata Postgres; the
        // import needs the same URL the server got (common's rule), or
        // the open refuses it.
        let mut import = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"));
        if scion_server::Backend::test_is_postgres() {
            import
                .arg("--postgres-url")
                .arg(common::postgres_url(&data));
        }
        import
            // `--repo` names the harness's repo (the runbook's
            // default is `main`).
            .args([
                "--data",
                data.to_str().unwrap(),
                "import",
                "--repo",
                "r",
                git_repo.to_str().unwrap(),
            ])
            .env("NO_COLOR", "1")
            .output()
            .expect("the import command runs")
    });
    assert!(
        out.status.success(),
        "the first import failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (_, mut objects) = connect(&server.url).await.unwrap();
    let _ = resolve_ref(&mut objects, "main")
        .await
        .expect("the imported ref reads back");
}
