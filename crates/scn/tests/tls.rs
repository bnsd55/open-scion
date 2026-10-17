//! `scn` against a server that serves TLS: a client with `--tls-ca`
//! connects, one without it, or with the wrong CA, does not.

use std::path::PathBuf;
use std::process::Command;

use scion_core::{Changeset, IdentityTime};
use scion_proto::listen::Limits;
use scion_server::meta::MetaTx as _;
use scion_server::{Store, service, tree};
use tempfile::TempDir;
use tokio::net::TcpListener;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// The certificates of one test: a CA, the server certificate it signed, and
/// a second CA, so a test can trust the wrong one. All in a temp directory.
struct TestCerts {
    dir: TempDir,
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

        // The CA key has no use after signing, but its file sits in the same
        // directory; remove it, so no key of a CA is left around to confuse.
        std::fs::remove_file(&ca_key_path).ok();

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
/// `name.key`.
fn write_pem(dir: &TempDir, name: &str, cert: &str, key: &str) -> (PathBuf, PathBuf) {
    let cert_path = dir.path().join(format!("{name}.pem"));
    std::fs::write(&cert_path, cert).unwrap();
    let key_path = dir.path().join(format!("{name}.key"));
    std::fs::write(&key_path, key).unwrap();
    (cert_path, key_path)
}

/// Stores a first changeset in the repo, and points `main` at it, so `scn
/// log main` has something to show.
fn land(store: &Store) {
    let cs = Changeset {
        parents: Vec::new(),
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"tls\n".to_vec(),
        changes: std::collections::BTreeMap::new(),
    };
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (id, _) = tree::store_changeset(c, repo, &cs)?;
            let version = c.get_ref(repo, "main")?.map(|r| r.version);
            assert!(c.cas_ref(repo, "main", version, id)?);
            Ok(id)
        })
        .unwrap();
}

/// A server that serves TLS, and its https address.
async fn serve_tls(certs: &TestCerts) -> String {
    let tls =
        scion_proto::tls::ServerTls::load(&certs.server_cert, &certs.server_key, None).unwrap();
    let store = Store::open(&certs.dir.path().join("data")).unwrap();
    land(&store);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            limits: Limits::default(),
            quotas: scion_server::quotas::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: Some(tls),
        },
        None,
        std::future::pending(),
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
    let url = serve_tls(&certs).await;

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
