//! Who may do what: tokens to principals, and each principal's read and
//! write rules per repo.
//!
//! The article says Scion has access control on repositories and can apply
//! permissions to individual directories. It does not say how. The rules
//! here are this project's choice:
//!
//! - A principal reads a whole repo, or the paths at or below its read
//!   prefixes. A read prefix names a path of the repo, by path parts: `src`
//!   covers `src` and `src/a`, not `srcx`. A read rule with no prefix covers
//!   the whole repo. A principal whose read rules are all prefixes is a
//!   *partial reader*: it reads what they cover through the view that the
//!   crate's `views` module derives for it, and a path outside them
//!   answers as a
//!   missing one. Its history reads answer `UNIMPLEMENTED`, uniformly, and
//!   so would its writes; a file that gives a partial reader a write rule
//!   is refused at load.
//! - A write rule names a path prefix of a repo. A principal may change a
//!   path at or below one of its prefixes, by path parts: `src` covers `src`
//!   and `src/a`, not `srcx`. A rule with no prefix covers the whole repo. A
//!   rule covers every ref of its repo. A write rule needs a whole-repo read
//!   rule: no blind writes, and no silent non-writer.
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
//! A `read` or `write` rule names an optional path prefix in a fourth
//! field: `read alice main src` lets alice read `src` and below, and
//! nothing else. The canonical read rule set of a principal (its read and
//! write prefixes, sorted, deduped, covered ones dropped) is hashed into a
//! rule-set digest, which keys the server's derived data. A repo holds at
//! most `MAX_RULE_SETS_PER_REPO` distinct rule sets, and a set at most
//! `MAX_PREFIXES_PER_SET` prefixes; an access file over either cap is
//! refused at load.
//!
//! The file holds digests, not tokens: `scion-server hash-token` prints
//! one. With `--token` instead, one principal, `default`, may read and
//! write every repo ([`Access::single`]).

use std::collections::{BTreeSet, HashMap};

use scion_core::RepoPath;
use scion_proto::runtime::auth::{Auth, Caller, DEFAULT_PRINCIPAL, token_digest};

use crate::db;
use crate::error::{Error, Result};

/// Longest principal name, in bytes.
pub(crate) const MAX_PRINCIPAL_BYTES: usize = 64;

/// The most distinct canonical read rule sets one repo may hold. A set is
/// shared by every principal whose read prefixes canonicalize to it, so the
/// cap is on roles, not principals. Over the cap, the access file is
/// refused at load.
pub(crate) const MAX_RULE_SETS_PER_REPO: usize = 32;

/// The most prefixes one canonical read rule set may hold. Over the cap,
/// the access file is refused at load.
pub(crate) const MAX_PREFIXES_PER_SET: usize = 256;

impl From<&str> for Access {
    /// [`Access::single`]: one token that may do everything.
    fn from(token: &str) -> Self {
        Self::single(token)
    }
}

/// What one principal may do in one repo. It may read what its read rules
/// cover, and change what its write rules cover.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Grant {
    /// It may read every path of the repo.
    read_all: bool,
    /// It may read a path at or below one of these.
    read: Vec<RepoPath>,
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

    /// The canonical read rule set: the whole repo (`None`), or the read
    /// prefixes, sorted, deduped, with prefixes covered by other prefixes
    /// dropped. The write prefixes are in it too: a write rule lets its
    /// principal read what it writes. Two files that name the same paths in
    /// different shapes share one rule set.
    fn canonical_read(&self) -> Option<Vec<RepoPath>> {
        if self.read_all {
            return None;
        }
        let mut set: BTreeSet<&RepoPath> = self.read.iter().collect();
        set.extend(self.write.iter());
        let mut prefixes: Vec<RepoPath> = set.into_iter().cloned().collect();
        prefixes.sort();
        prefixes.dedup();
        // A prefix that another prefix covers adds nothing: the sort puts a
        // covering prefix first, so one pass over the kept prefixes serves.
        let mut kept: Vec<RepoPath> = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            if !kept.iter().any(|cover| at_or_below(&prefix, cover)) {
                kept.push(prefix);
            }
        }
        Some(kept)
    }
}

/// Whether `path` is `prefix` or a path below it, by parts.
fn at_or_below(path: &RepoPath, prefix: &RepoPath) -> bool {
    let (path, prefix) = (path.as_bytes(), prefix.as_bytes());
    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest[0] == b'/')
}

/// The version of the rule-set digest, so a change of the canonical shape
/// re-derives everything that was keyed by an older digest.
const RULE_SET_DIGEST_VERSION: u8 = 1;

/// The digest of a canonical read rule set. The whole repo (`None`) and
/// every set of canonical prefixes hash to distinct digests, and two sets
/// with the same canonical prefixes hash to one digest.
pub(crate) fn rule_set_digest(canonical: Option<&[RepoPath]>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[RULE_SET_DIGEST_VERSION]);
    match canonical {
        None => {
            hasher.update(&[0]);
        }
        Some(prefixes) => {
            hasher.update(&[1]);
            hasher.update(&(prefixes.len() as u64).to_be_bytes());
            for prefix in prefixes {
                let bytes = prefix.as_bytes();
                hasher.update(&(bytes.len() as u64).to_be_bytes());
                hasher.update(bytes);
            }
        }
    }
    *hasher.finalize().as_bytes()
}

/// No blind writes, and no silent non-writer: a principal that holds any
/// write rule of a repo must read the whole repo. A partial reader cannot
/// write in v0.1 of the folder rules ([#183] holds the follow-up), so a
/// file that would make one is refused at load, with the line to fix.
/// `writes` holds each write rule with its line, for the error.
fn check_write_needs_read(
    access: &Access,
    writes: &[(usize, String, String)],
) -> Result<(), String> {
    for (n, principal, repo) in writes {
        let whole_read = access
            .grants
            .get(&(principal.clone(), repo.clone()))
            .is_some_and(|grant| grant.read_all);
        if !whole_read {
            return Err(format!(
                "line {n}: a write rule needs a read rule without a prefix, so principal \
                 {principal:?} may read {repo:?} whole: partial readers cannot write in \
                 v0.1 ([#183](https://github.com/bnsd55/openscion/issues/183))"
            ));
        }
    }
    Ok(())
}

/// The caps: one canonical set of at most [`MAX_PREFIXES_PER_SET`] prefixes,
/// and at most [`MAX_RULE_SETS_PER_REPO`] distinct canonical sets per repo.
/// Principals whose prefixes canonicalize to the same set share one.
fn check_caps(access: &Access) -> Result<(), String> {
    let mut per_repo: HashMap<&str, BTreeSet<[u8; 32]>> = HashMap::new();
    for ((principal, repo), grant) in &access.grants {
        // The whole repo needs no view, so it takes no set and no prefix
        // slot: only prefix rule sets count toward the caps.
        let Some(prefixes) = grant.canonical_read() else {
            continue;
        };
        if prefixes.len() > MAX_PREFIXES_PER_SET {
            return Err(format!(
                "principal {principal:?} holds more than {MAX_PREFIXES_PER_SET} read \
                 prefixes of repo {repo:?}"
            ));
        }
        let digest = rule_set_digest(Some(&prefixes));
        if per_repo.entry(repo.as_str()).or_default().insert(digest)
            && per_repo[repo.as_str()].len() > MAX_RULE_SETS_PER_REPO
        {
            return Err(format!(
                "repo {repo:?} holds more than {MAX_RULE_SETS_PER_REPO} distinct read rule sets"
            ));
        }
    }
    Ok(())
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
    /// How many tokens the access carries: one per line of an access
    /// file, or the one token of `--token`. The audit's start line
    /// names it, so an operator can match a config to a log.
    #[must_use]
    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

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
    /// two digests for one principal, a rule for a principal with no token,
    /// a whole-repo write rule without a whole-repo read rule, or a file
    /// over the rule-set caps.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut access = Self {
            tokens: Vec::new(),
            grants: HashMap::new(),
            everything: None,
        };
        // Each principal that a rule names, and the line, for the error.
        let mut named = Vec::new();
        // Each write rule, with its line, for the check that its principal
        // reads the whole repo: a partial reader cannot write in v0.1.
        let mut writes = Vec::new();
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
                    if digest == token_digest("") {
                        return Err(at("this is the digest of an empty token, and an empty \
                             token is no secret; hash the real token with `hash-token`"
                            .into()));
                    }
                    if access.tokens.iter().any(|(d, _)| *d == digest) {
                        return Err(at("another principal has this token".into()));
                    }
                    if access.tokens.iter().any(|(_, p)| p == principal) {
                        return Err(at(format!("principal {principal:?} has a token already")));
                    }
                    access.tokens.push((digest, (*principal).to_owned()));
                }
                ["read", principal, repo, rest @ ..] if rest.len() <= 1 => {
                    let grant = access.grant(principal, repo).map_err(at)?;
                    match rest {
                        [] => grant.read_all = true,
                        [prefix] => {
                            let prefix = RepoPath::new(*prefix)
                                .map_err(|e| at(format!("bad path {prefix:?}: {e}")))?;
                            grant.read.push(prefix);
                        }
                        _ => unreachable!("matched at most one prefix"),
                    }
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
                    // The check that this principal reads the whole repo
                    // waits for the end of the file: the `read` line may
                    // come after the `write` line.
                    writes.push((n, (*principal).to_owned(), (*repo).to_owned()));
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
        check_write_needs_read(&access, &writes)?;
        check_caps(&access)?;
        Ok(access)
    }

    /// Whether `caller` reads `repo` through folder rules, not whole. A
    /// caller that may not see the repo at all never reaches a content read,
    /// so it is not partial here.
    #[must_use]
    pub fn partial(&self, caller: &Caller, repo: &str) -> bool {
        match self.rights(caller, repo) {
            Some(Rights::Grant(grant)) => !grant.read_all,
            _ => false,
        }
    }

    /// The canonical read rule set of `caller` in `repo`: `None` for the
    /// whole repo, or the canonical prefixes. `None` also for a caller that
    /// may not see the repo: it never reaches a content read.
    #[must_use]
    pub fn read_rule_set(&self, caller: &Caller, repo: &str) -> Option<Vec<RepoPath>> {
        match self.rights(caller, repo) {
            Some(Rights::Grant(grant)) => grant.canonical_read(),
            _ => None,
        }
    }

    /// The digest of [`Access::read_rule_set`], which keys the derived view
    /// data. `None` for a whole-repo reader: it needs no view.
    #[must_use]
    pub fn read_digest(&self, caller: &Caller, repo: &str) -> Option<[u8; 32]> {
        match self.rights(caller, repo) {
            Some(Rights::Grant(grant)) => grant
                .canonical_read()
                .as_deref()
                .map(|prefixes| rule_set_digest(Some(prefixes))),
            _ => None,
        }
    }

    /// Every canonical read rule-set digest of `repo` that some principal
    /// holds: the live keys of the view rows. The sweep of a derivation
    /// removes view rows of digests outside this set, so a digest whose
    /// last principal went away dies at the next derivation.
    #[must_use]
    pub fn view_digests(&self, repo: &str) -> Vec<[u8; 32]> {
        let mut digests: std::collections::BTreeSet<[u8; 32]> = std::collections::BTreeSet::new();
        for ((principal, grant_repo), grant) in &self.grants {
            let _ = principal;
            if grant_repo != repo {
                continue;
            }
            if let Some(prefixes) = grant.canonical_read() {
                digests.insert(rule_set_digest(Some(&prefixes)));
            }
        }
        digests.into_iter().collect()
    }

    /// The grant of `principal` in `repo`, made if missing.
    fn grant(&mut self, principal: &str, repo: &str) -> Result<&mut Grant, String> {
        check_principal(principal)?;
        scion_proto::runtime::check::name("repo", repo)?;
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
        tracing::Span::current().record("repo", repo);
        if self.rights(caller, repo).is_some() {
            return Ok(());
        }
        // The reply says that the repo does not exist; the log says why.
        // One line for the refusal.
        tracing::warn!(principal = %caller.0, "may not read the repo");
        Err(db::no_repo(repo))
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
        tracing::Span::current().record("repo", repo);
        let grant = match self.rights(caller, repo) {
            None => {
                tracing::warn!(principal = %caller.0, "may not read the repo");
                return Err(db::no_repo(repo));
            }
            Some(Rights::Everything) => return Ok(()),
            Some(Rights::Grant(grant)) => grant,
        };
        match paths.into_iter().find(|path| !grant.may_write(path)) {
            None => Ok(()),
            Some(path) => {
                tracing::warn!(principal = %caller.0, path = ?path, "may not write the path");
                Err(Error::PermissionDenied(format!(
                    "principal {:?} may not write {path:?} in repo {repo:?}",
                    caller.0
                )))
            }
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
             read\talice\tmain\n\
             write\talice\tmain\tsrc\n\
             read\tbob\tmain\n\
             read\tci\tmain\n\
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
        access
            .check_write(&caller("ci"), "main", [&path("a")])
            .unwrap();
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
    fn a_read_prefix_covers_its_path_and_below_by_parts() {
        let text = format!(
            "token\talice\t{}\n\
             read\talice\tmain\tsrc\n",
            hex("a")
        );
        let access = Access::parse(&text).unwrap();
        let alice = caller("alice");
        assert!(access.partial(&alice, "main"));
        // The prefixes the server derives for the caller: `src` covers its
        // own path and everything below, by path parts.
        let prefixes = access.read_rule_set(&alice, "main").unwrap();
        let covered: Vec<&str> = prefixes
            .iter()
            .map(|p| std::str::from_utf8(p.as_bytes()).unwrap())
            .collect();
        assert_eq!(covered, ["src"]);
        assert!(access.partial(&alice, "main"));
        // A whole-repo reader is not partial.
        let text = format!(
            "token\tbob\t{}\n\
             read\tbob\tmain\n",
            hex("b")
        );
        let access = Access::parse(&text).unwrap();
        assert!(!access.partial(&caller("bob"), "main"));
        assert!(access.read_rule_set(&caller("bob"), "main").is_none());
    }

    #[test]
    fn a_write_needs_a_whole_read() {
        // Any write rule without a whole-repo read rule is refused at
        // load: a partial reader cannot write in v0.1.
        let text = format!(
            "token\talice\t{}\n\
             write\talice\tmain\tsrc/net\n",
            hex("a")
        );
        let err = Access::parse(&text).unwrap_err();
        assert!(err.contains("partial readers cannot write"), "{err}");
        assert!(err.contains("line 2"), "{err}");
        // A whole-repo write rule without a whole-repo read rule is refused,
        // also when the read rule has prefixes or comes first.
        let token = format!("token\tbob\t{}\n", hex("b"));
        for extra in [String::new(), "read\tbob\tmain\tsrc\n".to_owned()] {
            let text = format!("{token}{extra}write\tbob\tmain\n");
            match Access::parse(&text) {
                Err(why) => assert!(
                    why.contains("needs a read rule without a prefix")
                        && why.contains("partial readers cannot write"),
                    "{why}"
                ),
                Ok(_) => panic!("{text:?}: accepted"),
            }
        }
        // With the whole-repo read rule, it parses.
        let text = format!("{token}read\tbob\tmain\nwrite\tbob\tmain\n");
        Access::parse(&text).unwrap();
    }

    #[test]
    fn the_canonical_read_set_sorts_dedupes_and_drops_covered_prefixes() {
        let text = format!(
            "token\talice\t{}\n\
             read\talice\tmain\tsrc\n\
             read\talice\tmain\tdocs\n\
             read\talice\tmain\tsrc\n\
             read\talice\tmain\tsrc/net\n",
            hex("a")
        );
        let access = Access::parse(&text).unwrap();
        let set = access.read_rule_set(&caller("alice"), "main").unwrap();
        // `src/net` is covered by `src`;
        // the order is sorted; `src` twice is once.
        let shown: Vec<&str> = set
            .iter()
            .map(|p| std::str::from_utf8(p.as_bytes()).unwrap())
            .collect();
        assert_eq!(shown, ["docs", "src"]);
    }

    #[test]
    fn rule_sets_that_name_the_same_paths_share_one_digest() {
        let text = format!(
            "token\talice\t{}\n\
             token\tbob\t{}\n\
             read\talice\tmain\tsrc\n\
             read\talice\tmain\tdocs\n\
             read\tbob\tmain\tdocs\n\
             read\tbob\tmain\tsrc\n\
             read\tbob\tmain\tsrc/net\n",
            hex("a"),
            hex("b")
        );
        let access = Access::parse(&text).unwrap();
        let alice = access.read_rule_set(&caller("alice"), "main").unwrap();
        let bob = access.read_rule_set(&caller("bob"), "main").unwrap();
        assert_eq!(rule_set_digest(Some(&alice)), rule_set_digest(Some(&bob)));
        // A different set hashes differently, and the whole repo differs
        // from every prefix set.
        let text = format!(
            "token\tci\t{}\n\
             read\tci\tmain\tsrc\n",
            hex("c")
        );
        let access = Access::parse(&text).unwrap();
        let ci = access.read_rule_set(&caller("ci"), "main").unwrap();
        assert_ne!(
            rule_set_digest(Some(&ci)),
            rule_set_digest(Some(&[path("docs"), path("src")]))
        );
        assert_ne!(rule_set_digest(None), rule_set_digest(Some(&[path("src")])));
    }

    #[test]
    fn a_repo_over_the_rule_set_cap_is_refused_and_one_set_serves_many_principals() {
        use std::fmt::Write as _;
        // MAX_RULE_SETS_PER_REPO distinct sets parse, and a principal that
        // shares another's set adds no set: two principals on one set and
        // the rest distinct still parse.
        let mut text = String::new();
        for n in 0..MAX_RULE_SETS_PER_REPO {
            let _ = writeln!(text, "token\tp{n}\t{}", hex(&format!("t{n}")));
        }
        for n in 0..MAX_RULE_SETS_PER_REPO {
            let prefix = if n == 0 { "src" } else { &*format!("d{n}") };
            let _ = writeln!(text, "read\tp{n}\tmain\t{prefix}");
        }
        // p31 shares p0's set.
        let _ = writeln!(text, "token\tshare\t{}\nread\tshare\tmain\tsrc", hex("ts"));
        Access::parse(&text).unwrap();
        // One more distinct set is refused.
        let _ = writeln!(text, "token\textra\t{}\nread\textra\tmain\tdocs", hex("tx"));
        match Access::parse(&text) {
            Err(why) => assert!(
                why.contains(&MAX_RULE_SETS_PER_REPO.to_string()) && why.contains("main"),
                "{why}"
            ),
            Ok(_) => panic!("over the cap: accepted"),
        }
    }

    #[test]
    fn a_rule_set_over_the_prefix_cap_is_refused() {
        use std::fmt::Write as _;
        let mut text = format!("token\talice\t{}\n", hex("a"));
        for n in 0..=MAX_PREFIXES_PER_SET {
            let _ = writeln!(text, "read\talice\tmain\td{n}");
        }
        match Access::parse(&text) {
            Err(why) => assert!(why.contains(&MAX_PREFIXES_PER_SET.to_string()), "{why}"),
            Ok(_) => panic!("over the cap: accepted"),
        }
        // At the cap it parses.
        let mut text = format!("token\talice\t{}\n", hex("a"));
        for n in 0..MAX_PREFIXES_PER_SET {
            let _ = writeln!(text, "read\talice\tmain\td{n}");
        }
        Access::parse(&text).unwrap();
    }

    #[test]
    fn a_bad_line_is_refused_with_its_number() {
        let token = format!("token\talice\t{}\n", hex("a"));
        let cases = [
            ("token alice abc\n".to_owned(), "line 1: expected"),
            ("token\talice\tabc\n".to_owned(), "64 hex digits"),
            (
                format!("{token}token\tbob\t{}\n", hex("a")),
                "line 2: another principal",
            ),
            (
                format!("{token}token\talice\t{}\n", hex("b")),
                "has a token already",
            ),
            (
                format!("{token}read\tbob\tmain\n"),
                "line 2: principal \"bob\" has no token",
            ),
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

    // ---- The canonicalization properties: random rule shapes against a
    // ---- naive model of what canonical means.

    use proptest::prelude::*;

    /// Random read and write prefixes: short names from a small alphabet,
    /// so prefixes overlap, nest and repeat.
    fn prefixes() -> impl proptest::strategy::Strategy<Value = Vec<String>> {
        // "-" sorts before "/", so names that change the prefix boundary
        // ("a" covering "a-b") are tried.
        proptest::collection::vec(proptest::collection::vec("[ab-]{1,3}", 1..3), 0..10).prop_map(
            |parts| {
                parts
                    .into_iter()
                    .map(|names| names.join("/"))
                    .collect::<Vec<_>>()
            },
        )
    }

    /// The grant of a rules file with `read` and `write` prefix rules.
    fn grant(read: &[String], write: &[String]) -> Grant {
        let as_paths = |list: &[String]| {
            list.iter()
                .map(|p| RepoPath::new(p.clone()).unwrap())
                .collect()
        };
        Grant {
            read_all: false,
            read: as_paths(read),
            write_all: false,
            write: as_paths(write),
        }
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(128))]

        /// The canonical set equals a naive minimal covering set computed
        /// with plain string parts, canonicalizing twice gives the same
        /// set, and the write prefixes land in the canonical read set.
        #[test]
        fn canon_equals_the_naive_minimal_set(read in prefixes(), write in prefixes()) {
            let g = grant(&read, &write);
            let canon = g.canonical_read();
            let canon = canon
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|p| std::str::from_utf8(p.as_bytes()).unwrap().to_owned())
                .collect::<Vec<_>>();
            // The naive model: a prefix is kept iff no other prefix covers
            // it. The boundary is checked on the string, so a bug that lets
            // "a" cover "ab" fails here.
            let mut pool: Vec<String> = read.iter().chain(write.iter()).cloned().collect();
            pool.sort();
            pool.dedup();
            let naive: Vec<String> = pool
                .iter()
                .filter(|p| !pool.iter().any(|q| q != *p && p.starts_with(&format!("{q}/"))))
                .cloned()
                .collect();
            prop_assert_eq!(&canon, &naive, "the canonical set differs from the naive model");
            // The canonical set, fed back as read prefixes: canon of canon.
            let again = grant(&canon, &[]);
            prop_assert_eq!(
                again
                    .canonical_read()
                    .unwrap()
                    .iter()
                    .map(|p| std::str::from_utf8(p.as_bytes()).unwrap().to_owned())
                    .collect::<Vec<_>>(),
                canon.clone()
            );
            // A write rule lets its principal read what it writes: every
            // write prefix is covered by a kept read prefix.
            for w in &write {
                prop_assert!(
                    canon.iter().any(|k| w == k || w.starts_with(&format!("{k}/"))),
                    "the write prefix {w} is not covered by the canonical set"
                );
            }
        }

        /// Two grants that cover the same paths canonicalize to one set
        /// and one digest: reordering, duplicates and covered sub-prefixes
        /// change nothing, while dropping or adding coverage changes the
        /// digest.
        #[test]
        fn same_coverage_one_digest(set in prefixes()) {
            prop_assume!(!set.is_empty());
            let g = grant(&set, &[]);
            let canon = g.canonical_read().unwrap();
            prop_assume!(!canon.is_empty());
            let digest = rule_set_digest(Some(&canon));
            let canon_len = canon.len();
            let canon = canon
                .iter()
                .map(|p| std::str::from_utf8(p.as_bytes()).unwrap().to_owned())
                .collect::<Vec<_>>();

            // Reordered and duplicated: same coverage.
            let mut shuffled: Vec<String> = canon.clone();
            shuffled.reverse();
            shuffled.extend(shuffled.clone());
            prop_assert_eq!(
                grant(&shuffled, &[])
                    .canonical_read()
                    .map(|c| c.iter().map(|p| std::str::from_utf8(p.as_bytes()).unwrap().to_owned()).collect::<Vec<_>>()),
                Some(canon.clone())
            );
            prop_assert_eq!(rule_set_digest(Some(&grant(&shuffled, &[]).canonical_read().unwrap())), digest);

            // Covered sub-prefixes of every prefix: same coverage. Each
            // sub-prefix is below its parent, so the parent covers it.
            let mut padded: Vec<String> = shuffled.clone();
            for p in &shuffled {
                padded.push(format!("{p}/a"));
                padded.push(format!("{p}/a/b"));
            }
            prop_assert_eq!(
                grant(&padded, &[])
                    .canonical_read()
                    .map(|c| c.iter().map(|p| std::str::from_utf8(p.as_bytes()).unwrap().to_owned()).collect::<Vec<_>>()),
                Some(canon.clone())
            );

            // A prefix of a disjoint namespace adds coverage: different
            // digest. The alphabet of `prefixes` has no `c`.
            let mut more = padded.clone();
            more.push("c".into());
            let other = grant(&more, &[]).canonical_read().unwrap();
            prop_assert_ne!(rule_set_digest(Some(&other)), digest);

            // One prefix fewer: less coverage, different digest.
            if canon_len > 1 {
                let fewer: Vec<String> = canon[..canon_len - 1].to_vec();
                let less = grant(&fewer, &[]).canonical_read().unwrap();
                prop_assert_ne!(rule_set_digest(Some(&less)), digest);
            }
        }

    }
}
