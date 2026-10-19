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

/// The certificates of one PEM file, one or more.
fn certificates(path: &Path, what: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .with_context(|| format!("cannot read {} {}", what, path.display()))?
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
        let certs = certificates(cert, "the TLS certificate")?;
        let key = PrivateKeyDer::from_pem_file(key)
            .with_context(|| format!("cannot read the TLS key {}", key.display()))?;
        let builder = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .context("no supported TLS protocol version")?;
        let mut config = match client_ca {
            None => builder.with_no_client_auth(),
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                for certificate in certificates(ca, "the client CA")? {
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
                let cert = std::fs::read(cert).with_context(|| {
                    format!("cannot read the client certificate {}", cert.display())
                })?;
                let key = std::fs::read(key)
                    .with_context(|| format!("cannot read the client key {}", key.display()))?;
                Some((cert, key))
            }
            _ => bail!("a client certificate needs both the certificate and the key"),
        };
        let ca = match ca {
            None => None,
            Some(ca) => Some(
                std::fs::read(ca)
                    .with_context(|| format!("cannot read the CA {}", ca.display()))?,
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
