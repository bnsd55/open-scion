//! Who may do what: tokens to principals, and each principal's read and
//! write rules per repo.
//!
//! The article says Scion has access control on repositories and can apply
//! permissions to individual directories. It does not say how. The rules
//! here are this project's choice:
//!
//! - A principal reads a whole repo, or none of it.
//! - A write rule names a path prefix of a repo. A principal may change a
//!   path at or below one of its prefixes, by path parts: `src` covers `src`
//!   and `src/a`, not `srcx`. A rule with no prefix covers the whole repo. A
//!   rule covers every ref of its repo. A write rule also lets the principal
//!   read the repo.
//! - A principal with no rule for a repo cannot see it: every call answers
//!   as for a repo that does not exist.
//!
//! The rules come from an access file, read once at start ([`Access::parse`]).
//! It is UTF-8 text, one rule per line, with fields separated by one tab.
//! Empty lines and lines that start with `#` are skipped.
//!
//! ```text
//! token   alice   <the BLAKE3 digest of alice's token, 64 hex digits>
//! read    alice   main
//! write   alice   main    src/net
//! write   ci      main
//! ```
//!
//! The file holds digests, not tokens: `scion-server hash-token` prints
//! one. With `--token` instead, one principal, `default`, may read and
//! write every repo ([`Access::single`]).

use std::collections::HashMap;

use scion_core::RepoPath;
use scion_proto::auth::{Auth, Caller, DEFAULT_PRINCIPAL, token_digest};

use crate::db;
use crate::error::{Error, Result};

/// Longest principal name, in bytes.
pub const MAX_PRINCIPAL_BYTES: usize = 64;

impl From<&str> for Access {
    /// [`Access::single`]: one token that may do everything.
    fn from(token: &str) -> Self {
        Self::single(token)
    }
}

/// What one principal may do in one repo. It may read the repo.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Grant {
    /// It may change any path of the repo.
    write_all: bool,
    /// It may change a path at or below one of these.
    write: Vec<RepoPath>,
}

impl Grant {
    fn may_write(&self, path: &RepoPath) -> bool {
        self.write_all || self.write.iter().any(|prefix| at_or_below(path, prefix))
    }

    fn may_write_some(&self) -> bool {
        self.write_all || !self.write.is_empty()
    }
}

/// Whether `path` is `prefix` or a path below it, by parts.
fn at_or_below(path: &RepoPath, prefix: &RepoPath) -> bool {
    let (path, prefix) = (path.as_bytes(), prefix.as_bytes());
    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest[0] == b'/')
}

/// The tokens and rules of a server.
#[derive(Clone, Debug)]
pub struct Access {
    /// The digest of each token, and its principal.
    tokens: Vec<([u8; 32], String)>,
    /// By principal and repo.
    grants: HashMap<(String, String), Grant>,
    /// The one principal of `--token`, which may read and write every repo.
    everything: Option<String>,
}

impl Access {
    /// One token, whose principal ([`DEFAULT_PRINCIPAL`]) may read and write
    /// every repo.
    #[must_use]
    pub fn single(token: &str) -> Self {
        Self {
            tokens: vec![(token_digest(token), DEFAULT_PRINCIPAL.into())],
            grants: HashMap::new(),
            everything: Some(DEFAULT_PRINCIPAL.into()),
        }
    }

    /// Reads an access file (see the module docs).
    ///
    /// # Errors
    ///
    /// A message that names the first bad line and why: a line that is not
    /// a rule, a bad name, digest or path, two principals with one digest,
    /// two digests for one principal, or a rule for a principal with no
    /// token.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut access = Self {
            tokens: Vec::new(),
            grants: HashMap::new(),
            everything: None,
        };
        // Each principal that a rule names, and the line, for the error.
        let mut named = Vec::new();
        for (n, line) in (1..).zip(text.lines()) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let at = |why: String| format!("line {n}: {why}");
            let fields: Vec<&str> = line.split('\t').collect();
            match fields.as_slice() {
                ["token", principal, digest] => {
                    check_principal(principal).map_err(at)?;
                    let digest = blake3::Hash::from_hex(digest)
                        .map_err(|_| at("a token digest is 64 hex digits".into()))?;
                    let digest = *digest.as_bytes();
                    if access.tokens.iter().any(|(d, _)| *d == digest) {
                        return Err(at("another principal has this token".into()));
                    }
                    if access.tokens.iter().any(|(_, p)| p == principal) {
                        return Err(at(format!("principal {principal:?} has a token already")));
                    }
                    access.tokens.push((digest, (*principal).to_owned()));
                }
                ["read", principal, repo] => {
                    access.grant(principal, repo).map_err(at)?;
                    named.push((n, (*principal).to_owned()));
                }
                ["write", principal, repo, rest @ ..] if rest.len() <= 1 => {
                    let grant = access.grant(principal, repo).map_err(at)?;
                    match rest {
                        [] => grant.write_all = true,
                        [prefix] => {
                            let prefix = RepoPath::new(*prefix)
                                .map_err(|e| at(format!("bad path {prefix:?}: {e}")))?;
                            grant.write.push(prefix);
                        }
                        _ => unreachable!("matched at most one prefix"),
                    }
                    named.push((n, (*principal).to_owned()));
                }
                _ => {
                    return Err(at(
                        "expected `token`, `read` or `write` and its fields, separated by tabs"
                            .into(),
                    ));
                }
            }
        }
        for (n, principal) in named {
            if !access.tokens.iter().any(|(_, p)| *p == principal) {
                return Err(format!("line {n}: principal {principal:?} has no token"));
            }
        }
        Ok(access)
    }

    /// The grant of `principal` in `repo`, made if missing.
    fn grant(&mut self, principal: &str, repo: &str) -> Result<&mut Grant, String> {
        check_principal(principal)?;
        scion_proto::check::name("repo", repo)?;
        Ok(self
            .grants
            .entry((principal.to_owned(), repo.to_owned()))
            .or_default())
    }

    /// The check of every RPC: which token is which principal.
    #[must_use]
    pub fn auth(&self) -> Auth {
        Auth::digests(
            self.tokens
                .iter()
                .map(|(digest, principal)| (*digest, Caller(principal.clone()))),
        )
    }

    /// What `caller` may do in `repo`, or `None` if it may not see the repo.
    fn rights(&self, caller: &Caller, repo: &str) -> Option<Rights<'_>> {
        if self.everything.as_ref() == Some(&caller.0) {
            return Some(Rights::Everything);
        }
        let grant = self.grants.get(&(caller.0.clone(), repo.to_owned()))?;
        Some(Rights::Grant(grant))
    }

    /// Fails unless `caller` may read `repo`, with the error of a repo that
    /// does not exist. So a caller cannot tell whether a repo that it may
    /// not read exists, also by the time of the answer: this check reads no
    /// database.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`].
    pub fn check_read(&self, caller: &Caller, repo: &str) -> Result<()> {
        match self.rights(caller, repo) {
            Some(_) => Ok(()),
            None => Err(db::no_repo(repo)),
        }
    }

    /// Fails unless `caller` may change every path of `paths` in `repo`.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if it may not read `repo`.
    /// [`Error::PermissionDenied`] naming the first path, in the order
    /// given, that it may not change.
    pub fn check_write<'p>(
        &self,
        caller: &Caller,
        repo: &str,
        paths: impl IntoIterator<Item = &'p RepoPath>,
    ) -> Result<()> {
        let grant = match self.rights(caller, repo) {
            None => return Err(db::no_repo(repo)),
            Some(Rights::Everything) => return Ok(()),
            Some(Rights::Grant(grant)) => grant,
        };
        match paths.into_iter().find(|path| !grant.may_write(path)) {
            None => Ok(()),
            Some(path) => Err(Error::PermissionDenied(format!(
                "principal {:?} may not write {path:?} in repo {repo:?}",
                caller.0
            ))),
        }
    }

    /// Fails unless `caller` may change some path of `repo`: the check of an
    /// upload, which names no path.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if it may not read `repo`.
    /// [`Error::PermissionDenied`] if it may read it only.
    pub fn check_writer(&self, caller: &Caller, repo: &str) -> Result<()> {
        match self.rights(caller, repo) {
            None => Err(db::no_repo(repo)),
            Some(Rights::Everything) => Ok(()),
            Some(Rights::Grant(grant)) if grant.may_write_some() => Ok(()),
            Some(Rights::Grant(_)) => Err(Error::PermissionDenied(format!(
                "principal {:?} may not write in repo {repo:?}",
                caller.0
            ))),
        }
    }
}

/// What a caller may do in one repo.
enum Rights<'a> {
    Everything,
    Grant(&'a Grant),
}

/// A principal name: 1 to [`MAX_PRINCIPAL_BYTES`] ASCII letters, digits,
/// `.`, `_` or `-`.
fn check_principal(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= MAX_PRINCIPAL_BYTES
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "bad principal {name:?}: 1 to {MAX_PRINCIPAL_BYTES} ASCII letters, digits, '.', '_' or '-'"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller(name: &str) -> Caller {
        Caller(name.into())
    }

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    fn hex(token: &str) -> String {
        blake3::Hash::from(token_digest(token)).to_hex().to_string()
    }

    /// alice may write `src` of `main`, bob may read `main`, ci may write
    /// all of `main` and read `other`.
    fn file() -> Access {
        let text = format!(
            "# rules\n\
             token\talice\t{}\n\
             token\tbob\t{}\n\
             token\tci\t{}\n\
             \n\
             write\talice\tmain\tsrc\n\
             read\tbob\tmain\n\
             write\tci\tmain\n\
             read\tci\tother\n",
            hex("a"),
            hex("b"),
            hex("c")
        );
        Access::parse(&text).unwrap()
    }

    #[test]
    fn a_write_prefix_covers_its_path_and_below_by_parts() {
        let access = file();
        let alice = caller("alice");
        for ok in ["src", "src/a", "src/a/b"] {
            access.check_write(&alice, "main", [&path(ok)]).unwrap();
        }
        for denied in ["srcx", "sr", "s/src", "a"] {
            match access.check_write(&alice, "main", [&path("src/a"), &path(denied)]) {
                Err(Error::PermissionDenied(why)) => {
                    assert!(why.contains(&format!("{:?}", path(denied))), "{why}");
                }
                other => panic!("{denied}: expected PermissionDenied, got {other:?}"),
            }
        }
        // A rule with no prefix covers the whole repo.
        access.check_write(&caller("ci"), "main", [&path("a")]).unwrap();
    }

    #[test]
    fn a_repo_without_a_rule_is_hidden_and_read_does_not_write() {
        let access = file();
        let missing = db::no_repo("other").to_string();
        for (who, repo) in [("alice", "other"), ("bob", "other"), ("nobody", "main")] {
            let who = caller(who);
            let errors = [
                access.check_read(&who, repo).unwrap_err(),
                access.check_writer(&who, repo).unwrap_err(),
                access.check_write(&who, repo, [&path("src")]).unwrap_err(),
            ];
            for e in errors {
                assert!(matches!(e, Error::NotFound(_)), "{e:?}");
                assert_eq!(e.to_string(), missing.replace("other", repo));
            }
        }
        let bob = caller("bob");
        access.check_read(&bob, "main").unwrap();
        assert!(matches!(
            access.check_writer(&bob, "main"),
            Err(Error::PermissionDenied(_))
        ));
        assert!(matches!(
            access.check_write(&bob, "main", [&path("src")]),
            Err(Error::PermissionDenied(_))
        ));
        // A write rule lets its principal read.
        access.check_read(&caller("alice"), "main").unwrap();
        access.check_writer(&caller("alice"), "main").unwrap();
        // A read rule does not let it write.
        assert!(access.check_writer(&caller("ci"), "other").is_err());
    }

    #[test]
    fn the_tokens_of_the_file_name_their_principals() {
        let mut auth = file().auth();
        let call = |auth: &mut Auth, token: &str| {
            use tonic::service::Interceptor as _;
            let mut request = tonic::Request::new(());
            let value = format!("Bearer {token}").parse().unwrap();
            request.metadata_mut().insert("authorization", value);
            auth.call(request)
                .ok()
                .and_then(|r| r.extensions().get::<Caller>().cloned())
        };
        assert_eq!(call(&mut auth, "a"), Some(caller("alice")));
        assert_eq!(call(&mut auth, "c"), Some(caller("ci")));
        assert_eq!(call(&mut auth, "d"), None);
    }

    #[test]
    fn single_token_mode_may_do_everything() {
        let access = Access::single("t");
        let default = caller(DEFAULT_PRINCIPAL);
        access.check_write(&default, "any", [&path("a")]).unwrap();
        access.check_writer(&default, "any").unwrap();
        assert!(access.check_read(&caller("other"), "any").is_err());
    }

    #[test]
    fn a_bad_line_is_refused_with_its_number() {
        let token = format!("token\talice\t{}\n", hex("a"));
        let cases = [
            ("token alice abc\n".to_owned(), "line 1: expected"),
            ("token\talice\tabc\n".to_owned(), "64 hex digits"),
            (format!("{token}token\tbob\t{}\n", hex("a")), "line 2: another principal"),
            (format!("{token}token\talice\t{}\n", hex("b")), "has a token already"),
            (format!("{token}read\tbob\tmain\n"), "line 2: principal \"bob\" has no token"),
            (format!("{token}write\talice\tmain\t/abs\n"), "bad path"),
            (format!("{token}write\talice\tmain\ta\tb\n"), "expected"),
            (format!("{token}read\talice\t\n"), "repo"),
            (format!("token\tal ice\t{}\n", hex("a")), "bad principal"),
            (format!("{token}delete\talice\tmain\n"), "expected"),
        ];
        for (text, fragment) in cases {
            match Access::parse(&text) {
                Err(why) => assert!(why.contains(fragment), "{text:?}: {why}"),
                Ok(_) => panic!("{text:?}: accepted"),
            }
        }
    }
}
