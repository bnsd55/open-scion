//! The Ed25519 keys of a server: it mints every read ticket that its
//! `GetRootTree` replies carry, and it verifies the ticket of every
//! `GetObjects` call against each key that it trusts.

use ed25519_dalek::{SigningKey, VerifyingKey};

/// The signing key of the tickets that this server mints, and the public
/// keys whose tickets this server verifies: its own, and the previous one
/// while a rotation replaces it. At most two, so a ticket names no key.
#[derive(Clone)]
pub struct Keys {
    signing: SigningKey,
    verify: Vec<VerifyingKey>,
}

impl Keys {
    /// The keys of `seed` (64 hex characters, the secret seed) and `old`,
    /// the public keys (64 hex characters) whose tickets stay valid during
    /// a rotation.
    ///
    /// # Errors
    ///
    /// Fails if a key is not 64 hex characters.
    pub fn load(seed: &str, old: &[String]) -> Result<Self, String> {
        let signing = SigningKey::from_bytes(&from_hex(seed, "ticket key")?);
        let mut verify = vec![signing.verifying_key()];
        for key in old {
            verify.push(
                VerifyingKey::from_bytes(&from_hex(key, "old ticket key")?)
                    .map_err(|_| "old ticket key is not a valid public key".to_owned())?,
            );
        }
        Ok(Self { signing, verify })
    }

    /// A fresh random key, for `ticket-key` to write out.
    ///
    /// # Panics
    ///
    /// Panics if the system randomness fails, which it does not.
    #[must_use]
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).expect("system randomness");
        let signing = SigningKey::from_bytes(&secret);
        Self {
            verify: vec![signing.verifying_key()],
            signing,
        }
    }

    /// The public key of the signing key, as 64 hex characters: what the
    /// tiers and the old server verify against.
    #[must_use]
    pub fn public_hex(&self) -> String {
        to_hex(&self.signing.verifying_key().to_bytes())
    }

    /// The keys that a ticket must verify against: one of them is enough.
    #[must_use]
    pub fn for_verify(&self) -> &[VerifyingKey] {
        &self.verify
    }

    /// The secret seed, as 64 hex characters: what `--ticket-key` reads.
    #[must_use]
    pub fn seed_hex(&self) -> String {
        to_hex(&self.signing.to_bytes())
    }

    /// Signs a ticket that lets `principal` read the objects of one root
    /// tree of `repo`, alive from now for the ticket's lifetime.
    ///
    /// # Errors
    ///
    /// Fails if a name is too long for a ticket, which the server's name
    /// rules make unreachable.
    pub fn sign(
        &self,
        repo: &str,
        principal: &str,
        root_tree: [u8; 32],
    ) -> Result<Vec<u8>, String> {
        scion_proto::ticket::sign(
            &self.signing,
            repo,
            principal,
            root_tree,
            scion_proto::ticket::now(),
        )
    }
}

/// The 32 bytes of `hex`, which must be 64 hex characters, as IDs print.
fn from_hex(hex: &str, name: &str) -> Result<[u8; 32], String> {
    blake3::Hash::from_hex(hex.trim())
        .map(|hash| *hash.as_bytes())
        .map_err(|_| format!("{name} must be 64 hex characters"))
}

/// The 32 bytes of `bytes`, as IDs print.
fn to_hex(bytes: &[u8]) -> String {
    blake3::Hash::from_bytes(bytes.try_into().expect("a key is 32 bytes"))
        .to_hex()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_key_round_trips_through_hex() {
        let keys = Keys::generate();
        let again = Keys::load(&to_hex(&keys.signing.to_bytes()), &[]).unwrap();
        assert_eq!(again.public_hex(), keys.public_hex());
        // Another key does not verify this key's tickets.
        let other = Keys::generate();
        let ticket = keys.sign("r", "alice", [1; 32]).unwrap();
        let now = scion_proto::ticket::now();
        assert!(scion_proto::ticket::parse(&ticket, &[keys.signing.verifying_key()], now,).is_ok());
        assert!(
            scion_proto::ticket::parse(&ticket, &[other.signing.verifying_key()], now,).is_err()
        );
    }

    #[test]
    fn a_key_that_is_not_64_hex_characters_is_refused() {
        assert!(Keys::load("", &[]).is_err());
        assert!(Keys::load("zz", &[]).is_err());
        assert!(Keys::load(&"a".repeat(63), &[]).is_err());
        assert!(Keys::load(&"a".repeat(65), &[]).is_err());
        // A good seed with a bad old key: refused too.
        let good = to_hex(&[7; 32]);
        assert!(Keys::load(&good, &["zz".to_owned()]).is_err());
        assert!(Keys::load(&good, &[]).is_ok());
    }
}
