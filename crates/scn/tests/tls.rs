//! `scn` against a server that serves TLS: a client with `--tls-ca`
//! connects, one without it, or with the wrong CA, does not.

use std::path::PathBuf;
use std::process::Command;

use scion_core::{Changeset, IdentityTime};
use scion_proto::runtime::listen::Limits;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::{
    Backend,
    test_support::{service, tree},
};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// The certificates of one test: a CA, the server certificate it signed, and
/// a second CA, so a test can trust the wrong one. All in a temp directory.
struct TestCerts {
    dir: TempDir,
    ca: PathBuf,
    ca_key: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    stranger_ca: PathBuf,
}

impl TestCerts {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let (ca, ca_key_path) = write_pem(&dir, "ca", &ca_cert.pem(), &ca_key.serialize_pem());

        let mut stranger_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        stranger_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        stranger_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let stranger_key = rcgen::KeyPair::generate().unwrap();
        let stranger_cert = stranger_params.self_signed(&stranger_key).unwrap();
        let (stranger_ca, _) = write_pem(
            &dir,
            "stranger-ca",
            &stranger_cert.pem(),
            &stranger_key.serialize_pem(),
        );

        let server_key = rcgen::KeyPair::generate().unwrap();
        let mut server_params = rcgen::CertificateParams::new(
            ["localhost", "127.0.0.1"]
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();
        let (server_cert, server_key) = write_pem(
            &dir,
            "server",
            &server_cert.pem(),
            &server_key.serialize_pem(),
        );

        Self {
            dir,
            ca,
            ca_key: ca_key_path,
            server_cert,
            server_key,
            stranger_ca,
        }
    }

    /// The runbook's rotation step: a new server certificate of the
    /// same CA, written over the server's cert and key files.
    fn rotate_server_certificate(&self) {
        let ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        let ca_key =
            rcgen::KeyPair::from_pem(&std::fs::read_to_string(&self.ca_key).unwrap()).unwrap();
        let server_key = rcgen::KeyPair::generate().unwrap();
        let mut server_params = rcgen::CertificateParams::new(
            ["localhost", "127.0.0.1"]
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();
        let (cert, key) = write_pem(
            &self.dir,
            "server",
            &server_cert.pem(),
            &server_key.serialize_pem(),
        );
        assert_eq!(
            cert, self.server_cert,
            "the rotation rewrites the same file"
        );
        assert_eq!(key, self.server_key, "the rotation rewrites the same file");
    }
}

/// Writes a certificate and its key into `dir`, as `name.pem` and
/// `name.key`. The key is 0600: it is a secret, and the server's checked
/// read refuses one that group or others can read.
fn write_pem(dir: &TempDir, name: &str, cert: &str, key: &str) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;
    let cert_path = dir.path().join(format!("{name}.pem"));
    std::fs::write(&cert_path, cert).unwrap();
    // A certificate is a shared file: the checked read refuses one that
    // group or others can write, whatever the umask of the run.
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let key_path = dir.path().join(format!("{name}.key"));
    std::fs::write(&key_path, key).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    (cert_path, key_path)
}

/// Stores a first changeset in the repo, and points `main` at it, so `scn
/// log main` has something to show.
fn land(store: &Backend) {
    let cs = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"tls\n".to_vec(),
        std::collections::BTreeMap::new(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (id, _) = tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?;
            let version = c.get_ref(repo, "main")?.map(|r| r.version);
            assert!(c.cas_ref(repo, "main", version, id)?);
            Ok(id)
        })
        .unwrap();
}

/// A server that serves TLS, and its https address. The shutdown ends
/// it: the rotation's restart holds the data directory one at a time.
async fn serve_tls(certs: &TestCerts, shutdown: tokio::sync::oneshot::Receiver<()>) -> String {
    let tls =
        scion_proto::runtime::tls::ServerTls::load(&certs.server_cert, &certs.server_key, None)
            .unwrap();
    let store = Backend::open_test(&certs.dir.path().join("data")).unwrap();
    let data = certs.dir.path().join("data");
    land(&store);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: data.clone(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: Some(tls),
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        async {
            let _ = shutdown.await;
        },
    ));
    format!("https://{addr}")
}

/// `scn --server URL --tls-ca FILE log` against an https server: its exit,
/// and whether it connected.
fn log(url: &str, ca: Option<&std::path::Path>) -> bool {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scn"));
    command.args(["--server", url, "--repo", REPO, "log", "main"]);
    if let Some(ca) = ca {
        command.args(["--tls-ca"]).arg(ca);
    }
    let out = command.env("SCION_TOKEN", TOKEN).output().unwrap();
    out.status.success()
}

/// A client with the CA of the server connects over https; one without a CA,
/// or with the wrong CA, does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_connects_over_https_with_its_ca() {
    let certs = TestCerts::new();
    let (_shutdown, shutdown) = tokio::sync::oneshot::channel::<()>();
    let url = serve_tls(&certs, shutdown).await;

    assert!(
        log(&url, Some(certs.ca.as_path())),
        "a client with the right CA connects"
    );
    assert!(!log(&url, None), "a client without a CA is refused");
    assert!(
        !log(&url, Some(certs.stranger_ca.as_path())),
        "a client with the wrong CA is refused"
    );
}

/// The certificate-rotation runbook (docs/operations.md, "Rotating the
/// server's certificate"): a new certificate of the same CA, written to
/// the server's cert and key files, and a restart - the client keeps
/// its CA file and connects unchanged. A certificate of a NEW CA is
/// the drill record of docs/drills/ (the clients' CA files must change
/// before the server moves, which no CI test can time on live clients).
#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_server_certificate_needs_no_client_change() {
    let certs = TestCerts::new();
    let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
    let url = serve_tls(&certs, shutdown).await;
    assert!(
        log(&url, Some(certs.ca.as_path())),
        "a client with the CA connects before the rotation"
    );

    // The runbook's step: write the new certificate (the same CA), and
    // restart the server with it.
    certs.rotate_server_certificate();
    let _ = stop.send(());
    // The old server holds the data directory until its shutdown
    // unwinds; the restart waits for the directory, like an operator's
    // stop-then-start does. A throwaway open is the wait: its success
    // says the lock is free, and its drop leaves it for the restart.
    let mut restarted = None;
    for _ in 0..100 {
        let data = certs.dir.path().join("data");
        match Backend::open_test(&data) {
            Ok(probe) => {
                drop(probe);
                let (stop2, shutdown2) = tokio::sync::oneshot::channel::<()>();
                let url = serve_tls(&certs, shutdown2).await;
                restarted = Some((url, stop2));
                break;
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    let (url, _stop2) = restarted.expect("the server restarts with the new certificate");

    // The client's promise: the CA file did not change, and the client
    // still connects.
    assert!(
        log(&url, Some(certs.ca.as_path())),
        "a client with the same CA connects after the rotation"
    );
}
