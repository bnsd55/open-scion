//! TLS for the connections of [`listen`](crate::runtime::listen): how a process that serves loads
//! its certificate and key, and how a process that calls trusts the
//! certificate of the process it calls, and presents its own for mTLS.
//!
//! The cryptography of rustls is ring (the `tls-ring` feature of tonic, the
//! one with the fewest crates). Certificates and keys are PEM files.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, bail};
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

/// The ring cryptography, in an `Arc`, as the builders of rustls want it.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(tokio_rustls::rustls::crypto::ring::default_provider())
}

/// Reads a PEM file that must stay whole — a certificate, a CA — through
/// one checked descriptor, and parses it. A swapped file changes whose
/// certificates a server accepts or whom a client trusts, so the read
/// refuses a symlink, checks the ancestors, and requires the owner and a
/// mode that no other account can write ([`read_shared_file`]).
fn shared_pem(path: &Path, what: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let bytes = crate::runtime::auth::read_shared_file(path)
        .map_err(|why| crate::runtime::auth::checked_read_error(path, why))?;
    CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<_, _>>()
        .with_context(|| format!("{} {} is not PEM", what, path.display()))
}

/// The TLS server side: the certificate and key that a process which serves
/// presents, and, with mTLS, the CA that a client certificate must come
/// from. Built once, for the connections of [`listen`](crate::runtime::listen).
pub struct ServerTls {
    config: Arc<ServerConfig>,
}

impl ServerTls {
    /// Loads the server certificate `cert` and key `key` (PEM files). With
    /// `client_ca`, every client must present a certificate that this CA
    /// signed (mTLS).
    ///
    /// # Errors
    ///
    /// A file that is missing or is not PEM of the right kind, or a
    /// certificate that does not match its key.
    pub fn load(cert: &Path, key: &Path, client_ca: Option<&Path>) -> anyhow::Result<Self> {
        let certs = shared_pem(cert, "the TLS certificate")?;
        let key_bytes = crate::runtime::auth::read_secret_file(key)
            .map_err(|why| crate::runtime::auth::checked_read_error(key, why))?;
        let key = PrivateKeyDer::from_pem_slice(&key_bytes)
            .with_context(|| format!("the TLS key {} is not PEM", key.display()))?;
        let builder = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .context("no supported TLS protocol version")?;
        let mut config = match client_ca {
            None => builder.with_no_client_auth(),
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                for certificate in shared_pem(ca, "the client CA")? {
                    roots
                        .add(certificate)
                        .context("the client CA is not a root certificate")?;
                }
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                        .build()
                        .context("the client CA does not make a client certificate verifier")?;
                builder.with_client_cert_verifier(verifier)
            }
        }
        .with_single_cert(certs, key)
        .context("the TLS certificate does not match the TLS key")?;
        // gRPC runs on HTTP/2, so the handshake must negotiate it: a client
        // of tonic refuses a connection without the ALPN protocol.
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// The configuration to serve TLS with. Built once, so every connection
    /// shares the certificate.
    #[must_use]
    pub fn config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.config)
    }
}

/// The TLS client side: the CA that signed the certificate of the process
/// this one calls, and, for mTLS, the certificate and key this process
/// presents.
#[derive(Clone)]
pub struct ClientTls {
    ca: Option<Vec<u8>>,
    identity: Option<(Vec<u8>, Vec<u8>)>,
}

impl ClientTls {
    /// Loads the CA, and the client certificate and key with mTLS, from PEM
    /// files. A CA that is `None` trusts no certificate, so an https address
    /// fails until a CA is given.
    ///
    /// # Errors
    ///
    /// A file that is missing or is not PEM, or one of the certificate and
    /// key without the other.
    pub fn load(
        ca: Option<&Path>,
        cert: Option<&Path>,
        key: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let identity = match (cert, key) {
            (None, None) => None,
            (Some(cert), Some(key)) => {
                let cert = crate::runtime::auth::read_shared_file(cert)
                    .map_err(|why| crate::runtime::auth::checked_read_error(cert, why))?;
                let key = crate::runtime::auth::read_secret_file(key)
                    .map_err(|why| crate::runtime::auth::checked_read_error(key, why))?;
                Some((cert, key))
            }
            _ => bail!("a client certificate needs both the certificate and the key"),
        };
        let ca = match ca {
            None => None,
            Some(ca) => Some(
                crate::runtime::auth::read_shared_file(ca)
                    .map_err(|why| crate::runtime::auth::checked_read_error(ca, why))?,
            ),
        };
        Ok(Self { ca, identity })
    }

    /// Applies the trust and the identity to `endpoint`, which serves
    /// `address`, when `address` is https. An http or unix address comes
    /// back as it is.
    ///
    /// # Errors
    ///
    /// An https address without a CA to trust, or a configuration that tonic
    /// refuses.
    pub fn apply_to(
        &self,
        address: &str,
        endpoint: tonic::transport::Endpoint,
    ) -> anyhow::Result<tonic::transport::Endpoint> {
        if !address.starts_with("https://") {
            return Ok(endpoint);
        }
        let Some(ca) = self.ca.as_deref() else {
            bail!("the address is https, but no CA is given to trust its certificate");
        };
        let mut tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca));
        if let Some((cert, key)) = &self.identity {
            tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        endpoint
            .tls_config(tls)
            .context("bad TLS client configuration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt as _};

    /// A self-signed certificate and its key, as PEM files in `dir`.
    fn pem_files(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
        use std::{fs, os::unix::fs::PermissionsExt as _};
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let cert_path = dir.path().join("tls.pem");
        let key_path = dir.path().join("tls.key");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        // The certificate is a shared file: the checked read refuses one
        // that group or others can write, whatever the umask of the run.
        std::fs::set_permissions(&cert_path, fs::Permissions::from_mode(0o644)).unwrap();
        // The key is a secret: the checked read refuses one that group or
        // others can read, whatever the umask of the run.
        std::fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn a_tls_key_the_group_or_others_can_read_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        std::fs::set_permissions(&key, fs::Permissions::from_mode(0o640)).unwrap();
        let err = match ServerTls::load(&cert, &key, None) {
            Ok(_) => panic!("a group-readable TLS key is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("read by group or others"), "{err}");
    }

    #[test]
    fn a_private_tls_key_loads_and_a_plain_certificate_loads() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        std::fs::set_permissions(&cert, fs::Permissions::from_mode(0o644)).unwrap();
        ServerTls::load(&cert, &key, None).unwrap();
    }

    #[test]
    fn a_symlinked_tls_key_is_refused_with_a_perfect_target() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        let link = dir.path().join("link.key");
        std::os::unix::fs::symlink(&key, &link).unwrap();
        let err = match ServerTls::load(&cert, &link, None) {
            Ok(_) => panic!("a symlinked TLS key is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("symlink"), "{err}");
    }

    #[test]
    fn a_client_key_the_group_can_read_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        std::fs::set_permissions(&key, fs::Permissions::from_mode(0o640)).unwrap();
        let err = match ClientTls::load(None, Some(&cert), Some(&key)) {
            Ok(_) => panic!("a group-readable client key is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("read by group or others"), "{err}");
    }

    /// A certificate that group or others can write is refused: a swapped
    /// certificate lets someone else be the server.
    #[test]
    fn a_certificate_the_group_or_others_can_write_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        std::fs::set_permissions(&cert, fs::Permissions::from_mode(0o664)).unwrap();
        let err = match ServerTls::load(&cert, &key, None) {
            Ok(_) => panic!("a group-writable certificate is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("written by group or others"), "{err}");
    }

    /// A symlinked certificate is refused at the open, even when its
    /// target has a perfect mode and owner.
    #[test]
    fn a_symlinked_certificate_is_refused_with_a_perfect_target() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        let link = dir.path().join("link.pem");
        std::os::unix::fs::symlink(&cert, &link).unwrap();
        let err = match ServerTls::load(&link, &key, None) {
            Ok(_) => panic!("a symlinked certificate is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("symlink"), "{err}");
    }

    /// A client CA that group or others can write is refused: a swapped
    /// CA changes whose client certificates the server accepts.
    #[test]
    fn a_client_ca_the_group_or_others_can_write_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = pem_files(&dir);
        let (ca, _) = pem_files(&dir);
        std::fs::set_permissions(&ca, fs::Permissions::from_mode(0o662)).unwrap();
        let err = match ServerTls::load(&cert, &key, Some(&ca)) {
            Ok(_) => panic!("a group-writable client CA is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("written by group or others"), "{err}");
    }

    /// The CA a client trusts is a shared file too: a swapped one changes
    /// whom the client believes it is talking to.
    #[test]
    fn a_client_tls_ca_the_group_or_others_can_write_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, _) = pem_files(&dir);
        std::fs::set_permissions(&ca, fs::Permissions::from_mode(0o662)).unwrap();
        let err = match ClientTls::load(Some(&ca), None, None) {
            Ok(_) => panic!("a group-writable CA is accepted"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("written by group or others"), "{err}");
    }
}
