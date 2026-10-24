//! The `check:` hint lines of `scn`: the failures a first-time user hits
//! print the error and one advice line, through the real binary against a
//! real server, so a reword of an upstream error cannot silence the hints
//! without this file failing.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::net::TcpListener;

use scion_proto::runtime::listen::Limits;
use scion_server::{Backend, test_support::service};

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// A plain http server that takes `token`, and its address. Dropping the
/// `TempDir` stops it (the server dies with its data directory).
async fn serve_http(data: &TempDir, token: &'static str) -> String {
    let store = Backend::open_test(data.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        token,
        service::ServeOptions {
            data: data.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    format!("http://{addr}")
}

/// Runs `scn log main` against `url` with `token`, and answers its stderr.
fn scn_stderr(url: &str, token: &str) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", url, "--repo", REPO, "log", "main"])
        .env("SCION_TOKEN", token)
        .output()
        .unwrap();
    assert!(!out.status.success(), "scn must fail");
    String::from_utf8(out.stderr).unwrap()
}

/// A server that is not there: the error carries the advice to check the
/// address. The port is 1, which nothing serves on this node.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_server_hints_at_the_address() {
    let stderr = scn_stderr("http://127.0.0.1:1", TOKEN);
    assert!(
        stderr.contains("check: is a scion server or tier running at that address?"),
        "{stderr}"
    );
}

/// A wrong token: the error names the failure of the server, and the hint
/// says which flag to compare.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_token_hints_at_the_token() {
    let data = TempDir::new().unwrap();
    let url = serve_http(&data, TOKEN).await;
    let stderr = scn_stderr(&url, "wrong");
    assert!(
        stderr.contains(
            "check: compare --token (or SCION_TOKEN) with the token the server was given"
        ),
        "{stderr}"
    );
}

/// The certificates of one test: a CA, the server certificate it signed,
/// and a second CA, so a test can trust the wrong one. All in a temp
/// directory. (The same setup as tests/tls.rs, kept here so the hint test
/// stays self-contained.)
struct TestCerts {
    // The files must live while the server reads them; the fields are
    // kept for the lifetime, not read.
    #[expect(dead_code)]
    dir: TempDir,
    #[expect(dead_code)]
    ca: PathBuf,
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
        std::fs::remove_file(&ca_key_path).ok();

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
            server_cert,
            server_key,
            stranger_ca,
        }
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

/// A client that trusts the wrong CA: the hint names `--tls-ca`, and the
/// connect hint must not win.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_ca_hints_at_the_ca() {
    let certs = TestCerts::new();
    let data = TempDir::new().unwrap();
    let url = serve_tls(&data, &certs).await;
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &url,
            "--repo",
            REPO,
            "--tls-ca",
            certs.stranger_ca.to_str().unwrap(),
            "log",
            "main",
        ])
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(!out.status.success(), "scn must fail");
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains("check: --tls-ca must hold the CA (PEM) that signed the certificate of the server you trust"),
        "{stderr}"
    );
    assert!(!stderr.contains("scion server or tier"), "{stderr}");
}

async fn serve_tls(data: &TempDir, certs: &TestCerts) -> String {
    let tls =
        scion_proto::runtime::tls::ServerTls::load(&certs.server_cert, &certs.server_key, None)
            .unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: data.path().to_path_buf(),
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
        std::future::pending(),
    ));
    format!("https://{addr}")
}
