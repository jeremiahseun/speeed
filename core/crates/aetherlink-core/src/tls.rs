//! TLS 1.3 with certificate pinning.
//!
//! The host generates an ephemeral self-signed identity per session and puts
//! its fingerprint in the QR payload. The client verifies that fingerprint and
//! nothing else — there is no CA, no name to validate, and no reason to want
//! one on a link that exists for ninety seconds.
//!
//! This is what Noise `IK` would have given us, using one audited dependency
//! and the ARMv8 AES-GCM the hardware already accelerates.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{ring as provider, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};

use crate::Error;

/// SNI value. Meaningless on a pinned link, but TLS requires one.
pub const SERVER_NAME: &str = "aetherlink.local";

/// BLAKE3 over the DER-encoded end-entity certificate. 32 bytes, which fits a
/// QR payload comfortably alongside the SSID and passphrase.
pub type Fingerprint = [u8; 32];

fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(provider::default_provider())
}

/// An ephemeral host identity, valid for one session.
pub struct HostIdentity {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    fingerprint: Fingerprint,
}

impl HostIdentity {
    pub fn generate() -> Result<Self, Error> {
        let certified = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
            .map_err(|e| Error::Tls(format!("generating host identity: {e}")))?;
        let cert = certified.cert.der().clone();
        let fingerprint = *blake3::hash(cert.as_ref()).as_bytes();
        let key = PrivateKeyDer::try_from(certified.key_pair.serialize_der())
            .map_err(|e| Error::Tls(format!("encoding host key: {e}")))?;
        Ok(Self {
            cert,
            key,
            fingerprint,
        })
    }

    /// The value that travels in the QR code. The client pins this.
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    pub fn fingerprint_hex(&self) -> String {
        self.fingerprint
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    pub fn server_config(&self) -> Result<Arc<ServerConfig>, Error> {
        let mut config = ServerConfig::builder_with_provider(crypto_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| Error::Tls(format!("selecting TLS 1.3: {e}")))?
            .with_no_client_auth()
            .with_single_cert(vec![self.cert.clone()], self.key.clone_key())
            .map_err(|e| Error::Tls(format!("installing host certificate: {e}")))?;

        // No session tickets. We never resume — each session gets a fresh
        // ephemeral identity — and an unsolicited NewSessionTicket sits unread
        // in the sender's receive buffer, so closing its socket would RST the
        // connection and discard payload still in flight.
        config.send_tls13_tickets = 0;

        Ok(Arc::new(config))
    }
}

/// Accepts exactly one certificate: the one whose fingerprint came over the
/// out-of-band channel. Chain, expiry and hostname are all irrelevant here —
/// the pin is strictly stronger than any of them for a single-session link.
#[derive(Debug)]
struct PinnedCertVerifier {
    expected: blake3::Hash,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // blake3::Hash compares in constant time.
        if blake3::hash(end_entity.as_ref()) == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "server certificate does not match the pinned fingerprint".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // TLS 1.2 is not offered; reaching here means a downgrade attempt.
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::ServerDoesNotSupportTls12Or13,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Client config that trusts one pinned fingerprint and nothing else.
pub fn client_config(pinned: Fingerprint) -> Result<Arc<ClientConfig>, Error> {
    let provider = crypto_provider();
    let verifier = PinnedCertVerifier {
        expected: blake3::Hash::from(pinned),
        provider: provider.clone(),
    };
    let config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| Error::Tls(format!("selecting TLS 1.3: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_identity_is_distinct() {
        let a = HostIdentity::generate().unwrap();
        let b = HostIdentity::generate().unwrap();
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn fingerprint_hex_is_64_characters() {
        assert_eq!(
            HostIdentity::generate().unwrap().fingerprint_hex().len(),
            64
        );
    }

    #[test]
    fn configs_build_for_both_roles() {
        let id = HostIdentity::generate().unwrap();
        assert!(id.server_config().is_ok());
        assert!(client_config(id.fingerprint()).is_ok());
    }
}
