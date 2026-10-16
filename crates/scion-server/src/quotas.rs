//! Per-principal limits: requests in flight and upload bytes in flight.
//!
//! The limits in [`service`](crate::service) and [`store`](crate::store)
//! bound the whole server. These bound one principal, so that one client
//! cannot take every queue slot or spool byte. A limit of `None` turns the
//! check off.
//!
//! The counts live in memory, one row per principal that a call named. A
//! guard takes its units on entry and gives them back on every exit path,
//! also when the client goes away or a job panics. The pending-land limit is
//! not counted here: a pending land outlives its call, so [`crate::land`]
//! counts its rows in the database.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use scion_proto::auth::Caller;

use crate::error::{Error, Result};

/// The per-principal limits, and the counts. A clone shares the counts.
#[derive(Clone, Debug, Default)]
pub struct Quotas {
    requests: Option<usize>,
    upload_bytes: Option<usize>,
    usage: Arc<Mutex<HashMap<String, Usage>>>,
}

impl Quotas {
    /// Limits each principal to `requests` calls in flight and
    /// `upload_bytes` upload bytes in flight. `None` turns a limit off.
    #[must_use]
    pub fn new(requests: Option<usize>, upload_bytes: Option<usize>) -> Self {
        Self {
            requests,
            upload_bytes,
            usage: Arc::default(),
        }
    }

    /// No limits. A server of one token runs this unless a flag says
    /// otherwise: every client is the same principal, so a cap would be a
    /// global one.
    #[must_use]
    pub fn off() -> Self {
        Self::new(None, None)
    }

    /// The upload-byte limit, for the checks of the later messages of an
    /// upload.
    #[must_use]
    pub fn upload_bytes(&self) -> Option<usize> {
        self.upload_bytes
    }

    /// Takes one of the in-flight requests of `caller`, until the [`Call`]
    /// drops.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal is over its limit.
    pub fn enter(&self, caller: &Caller) -> Result<Call> {
        let Some(limit) = self.requests else {
            return Ok(self.nothing(caller));
        };
        let mut usage = self.lock();
        let entry = usage.entry(caller.0.clone()).or_default();
        if entry.requests >= limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} has its {limit} requests in flight \
                 (--max-principal-requests {limit}); retry later",
                caller.0
            )));
        }
        entry.requests += 1;
        Ok(Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: None,
            bytes: 0,
            counts_requests: true,
        })
    }

    /// Takes `bytes` of the in-flight upload budget of `caller`. The caller
    /// adds the bytes of each later message with [`Call::add`], and gives
    /// everything back by dropping the guard.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal is over its limit.
    pub fn upload(&self, caller: &Caller, bytes: usize) -> Result<Call> {
        let limit = self.upload_bytes;
        let Some(limit) = limit else {
            return Ok(self.nothing(caller));
        };
        let mut usage = self.lock();
        let entry = usage.entry(caller.0.clone()).or_default();
        if entry.upload_bytes + bytes > limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} would have more than {limit} upload bytes in flight \
                 (--max-principal-upload-bytes {limit}); retry with fewer or smaller uploads",
                caller.0
            )));
        }
        entry.upload_bytes += bytes;
        Ok(Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: Some(limit),
            bytes,
            counts_requests: false,
        })
    }

    fn nothing(&self, caller: &Caller) -> Call {
        Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: None,
            bytes: 0,
            counts_requests: false,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Usage>> {
        self.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What one principal holds at the moment.
#[derive(Debug, Default)]
struct Usage {
    requests: usize,
    upload_bytes: usize,
}

/// What one call holds. Gives it back on drop, on every exit path.
#[derive(Debug)]
pub(crate) struct Call {
    usage: Arc<Mutex<HashMap<String, Usage>>>,
    principal: String,
    /// The limit for later [`Call::add`]s, when one is set.
    limit: Option<usize>,
    /// The in-flight bytes of the upload this guards, if any.
    bytes: usize,
    counts_requests: bool,
}

impl Call {
    /// Adds the bytes of one more upload message.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal would go over its
    /// limit. The bytes of the message are not taken.
    pub(crate) fn add(&mut self, bytes: usize) -> Result<()> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = usage.entry(self.principal.clone()).or_default();
        if entry.upload_bytes + bytes > limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} would have more than {limit} upload bytes in flight \
                 (--max-principal-upload-bytes {limit}); retry with fewer or smaller uploads",
                self.principal
            )));
        }
        entry.upload_bytes += bytes;
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for Call {
    /// Gives back what this call held, on every exit path.
    fn drop(&mut self) {
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = usage.get_mut(&self.principal) else {
            return;
        };
        if self.counts_requests {
            entry.requests = entry.requests.saturating_sub(1);
        }
        entry.upload_bytes = entry.upload_bytes.saturating_sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller(name: &str) -> Caller {
        Caller(name.into())
    }

    #[test]
    fn a_call_takes_one_slot_and_gives_it_back() {
        let quotas = Quotas::new(Some(1), None);
        let guard = quotas.enter(&caller("a")).unwrap();
        let status = quotas.enter(&caller("a")).unwrap_err();
        assert!(matches!(status, Error::ResourceExhausted(_)), "{status:?}");
        assert!(status.to_string().contains("--max-principal-requests 1"));
        drop(guard);
        quotas.enter(&caller("a")).unwrap();
    }

    #[test]
    fn principals_count_separately() {
        let quotas = Quotas::new(Some(1), None);
        let _a = quotas.enter(&caller("a")).unwrap();
        let _b = quotas.enter(&caller("b")).unwrap();
        quotas.enter(&caller("b")).unwrap_err();
        // The failed take took nothing.
        quotas.enter(&caller("b")).unwrap_err();
    }

    #[test]
    fn upload_bytes_add_up_and_come_back() {
        let quotas = Quotas::new(None, Some(10));
        let mut upload = quotas.upload(&caller("a"), 6).unwrap();
        upload.add(4).unwrap();
        assert!(upload.add(1).is_err());
        drop(upload);
        // The bytes came back, so a new upload fits again.
        let mut upload = quotas.upload(&caller("a"), 10).unwrap();
        assert!(upload.add(1).is_err());
        drop(upload);
        quotas.upload(&caller("a"), 10).unwrap();
    }

    #[test]
    fn principals_share_no_bytes() {
        let quotas = Quotas::new(None, Some(10));
        let _a = quotas.upload(&caller("a"), 10).unwrap();
        quotas.upload(&caller("b"), 10).unwrap();
    }

    #[test]
    fn no_limits_take_nothing() {
        let quotas = Quotas::off();
        let held: Vec<_> = (0..1000).map(|_| quotas.enter(&caller("a")).unwrap()).collect();
        let mut upload = quotas.upload(&caller("a"), usize::MAX - 1).unwrap();
        upload.add(usize::MAX - 1).unwrap();
        drop(upload);
        drop(held);
    }
}
