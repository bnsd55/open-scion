//! The proof that lets a group-writable path pass: on Linux with glibc,
//! a group that holds no account but the path's owner. Debian, Ubuntu and
//! Fedora give every account a group of its own and umask 002, so what a
//! user makes is writable by a group that no one else is in.
//!
//! The sources of the account database are gated first: `/etc/nsswitch.conf`
//! must take `passwd`, `group` and `initgroups` from `files`, optionally
//! followed by `systemd`, and nothing else. Then one snapshot of the
//! database, read through glibc's NSS (the `ffi` module), must show that
//! the group exists, that every account whose primary group it is, every
//! name it lists, and every account `getgrouplist` puts in it is the
//! owner, and that the owner is in it. Any doubt refuses: a lookup error,
//! a record listed twice, a source this check cannot vouch for. On every
//! other target the proof refuses, so a group-writable path stays refused.
//!
//! The proof covers the account database as it is when it runs. It cannot
//! see a process that still holds a group its account has since left: a
//! login keeps the groups it started with until it ends.

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// One account, as the proof reads it: its name as bytes, its uid and its
/// primary gid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Account {
    pub(super) name: Vec<u8>,
    pub(super) uid: u32,
    pub(super) gid: u32,
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// One group: its name as bytes, its gid, and the names it lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Group {
    pub(super) name: Vec<u8>,
    pub(super) gid: u32,
    pub(super) members: Vec<Vec<u8>>,
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// Where the proof reads the account database: glibc's NSS on this
/// system, or a test's stand-in. Each answer is owned data, or why it
/// could not be read.
pub(super) trait Source {
    /// The text of `/etc/nsswitch.conf`.
    fn nsswitch(&self) -> Result<String, String>;
    /// Every account, as `getpwent_r` walks them.
    fn accounts(&self) -> Result<Vec<Account>, String>;
    /// Every group, as `getgrent_r` walks them.
    fn groups(&self) -> Result<Vec<Group>, String>;
    /// The groups `getgrouplist` gives `account`, its primary group among
    /// them.
    fn group_list(&self, account: &Account) -> Result<Vec<u32>, String>;
}

/// `Ok` when group `gid` holds no account but `owner`, by this system's
/// account database; else why not. Linux with glibc only: on every other
/// target every group is refused.
pub(super) fn owners_alone(gid: u32, owner: u32) -> Result<(), String> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        prove(&System, gid, owner)
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        let _ = (gid, owner);
        Err("the proof that a group is its owner's alone runs on Linux with glibc only".to_owned())
    }
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// The proof over `source`: the sources of the database first, then the
/// sole membership, judged on one snapshot read whole before any of it is.
pub(super) fn prove(source: &dyn Source, gid: u32, owner: u32) -> Result<(), String> {
    local_files_only(&source.nsswitch()?)?;
    let accounts = source.accounts()?;
    let groups = source.groups()?;
    let mut lists = Vec::with_capacity(accounts.len());
    for account in &accounts {
        lists.push(source.group_list(account)?);
    }
    sole_member(&accounts, &groups, &lists, gid, owner)
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// The names as text, for a refusal.
fn show(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// Whether the snapshot proves that `owner` is the one member of `gid`.
/// `lists[i]` is what `getgrouplist` gave `accounts[i]`.
fn sole_member(
    accounts: &[Account],
    groups: &[Group],
    lists: &[Vec<u32>],
    gid: u32,
    owner: u32,
) -> Result<(), String> {
    // A record listed twice makes the database ambiguous: which one does
    // a lookup answer? Names compare as bytes.
    for (i, account) in accounts.iter().enumerate() {
        if accounts[..i].iter().any(|other| other.name == account.name) {
            return Err(format!(
                "the account {} is listed twice",
                show(&account.name)
            ));
        }
    }
    for (i, group) in groups.iter().enumerate() {
        if groups[..i].iter().any(|other| other.gid == group.gid) {
            return Err(format!("group {} has more than one entry", group.gid));
        }
        if groups[..i].iter().any(|other| other.name == group.name) {
            return Err(format!("the group {} is listed twice", show(&group.name)));
        }
    }
    let Some(group) = groups.iter().find(|group| group.gid == gid) else {
        return Err(format!("group {gid} is not in the group database"));
    };
    if !accounts.iter().any(|account| account.uid == owner) {
        return Err(format!("its owner, uid {owner}, has no account"));
    }
    let mut owner_in = false;
    // Primary-gid members count even when the group lists no one.
    for account in accounts.iter().filter(|account| account.gid == gid) {
        if account.uid != owner {
            return Err(format!(
                "{} (uid {}) has it as its primary group",
                show(&account.name),
                account.uid
            ));
        }
        owner_in = true;
    }
    for name in &group.members {
        let Some(account) = accounts.iter().find(|account| &account.name == name) else {
            return Err(format!("it lists {}, which has no account", show(name)));
        };
        if account.uid != owner {
            return Err(format!("it lists {} (uid {})", show(name), account.uid));
        }
        owner_in = true;
    }
    for (account, list) in accounts.iter().zip(lists) {
        if list.contains(&gid) {
            if account.uid != owner {
                return Err(format!(
                    "{} (uid {}) is in it by getgrouplist",
                    show(&account.name),
                    account.uid
                ));
            }
            owner_in = true;
        }
    }
    if !owner_in {
        return Err(format!("its owner, uid {owner}, is not in it"));
    }
    Ok(())
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// The databases whose sources decide who is in a group.
const DATABASES: [&str; 3] = ["passwd", "group", "initgroups"];

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// `Ok` if `/etc/nsswitch.conf` takes every database of [`DATABASES`] from
/// `files`, optionally followed by `systemd`. `passwd` and `group` must be
/// named; `initgroups` takes the sources of `group` when it is not. A
/// database named twice refuses.
fn local_files_only(text: &str) -> Result<(), String> {
    let mut seen = [false; DATABASES.len()];
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or_default();
        let Some((database, sources)) = line.split_once(':') else {
            continue;
        };
        let database = database.trim();
        let Some(at) = DATABASES.iter().position(|known| *known == database) else {
            continue;
        };
        if seen[at] {
            return Err(format!("/etc/nsswitch.conf names {database} twice"));
        }
        seen[at] = true;
        files_then_systemd(database, sources)?;
    }
    for (at, database) in DATABASES.iter().enumerate().take(2) {
        if !seen[at] {
            return Err(format!(
                "/etc/nsswitch.conf names no sources for {database}"
            ));
        }
    }
    Ok(())
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// `Ok` if `sources` is `files`, optionally followed by `systemd`, each
/// source with any well-formed action clauses after it.
fn files_then_systemd(database: &str, sources: &str) -> Result<(), String> {
    let mut names = Vec::new();
    let mut rest = sources.trim();
    while !rest.is_empty() {
        if let Some(clause) = rest.strip_prefix('[') {
            let Some(end) = clause.find(']') else {
                return Err(format!(
                    "/etc/nsswitch.conf has an unclosed action for {database}"
                ));
            };
            let body = &clause[..end];
            if names.is_empty() || !well_formed_action(body) {
                return Err(format!(
                    "/etc/nsswitch.conf has a malformed action for {database}: [{body}]"
                ));
            }
            rest = clause[end + 1..].trim_start();
        } else {
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '[')
                .unwrap_or(rest.len());
            names.push(&rest[..end]);
            rest = rest[end..].trim_start();
        }
    }
    match names.as_slice() {
        ["files"] | ["files", "systemd"] => Ok(()),
        [] => Err(format!(
            "/etc/nsswitch.conf names no sources for {database}"
        )),
        _ => Err(
            match names
                .iter()
                .find(|name| !matches!(**name, "files" | "systemd"))
            {
                Some(source) => {
                    format!("/etc/nsswitch.conf takes {database} entries from {source}")
                }
                None => format!(
                    "/etc/nsswitch.conf takes {database} entries from {}, not files then systemd",
                    names.join(" ")
                ),
            },
        ),
    }
}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
/// A clause such as `NOTFOUND=return` or `!UNAVAIL=continue`, one or more
/// of them: a status glibc knows, then an action it knows.
fn well_formed_action(body: &str) -> bool {
    let mut any = false;
    for item in body.split_whitespace() {
        let Some((status, action)) = item.split_once('=') else {
            return false;
        };
        let status = status.strip_prefix('!').unwrap_or(status);
        let known_status = ["SUCCESS", "NOTFOUND", "UNAVAIL", "TRYAGAIN"]
            .iter()
            .any(|known| known.eq_ignore_ascii_case(status));
        let known_action = ["return", "continue", "merge"]
            .iter()
            .any(|known| known.eq_ignore_ascii_case(action));
        if !known_status || !known_action {
            return false;
        }
        any = true;
    }
    any
}

/// This system's account database, through glibc's NSS.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
struct System;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
impl Source for System {
    fn nsswitch(&self) -> Result<String, String> {
        std::fs::read_to_string("/etc/nsswitch.conf")
            .map_err(|why| format!("/etc/nsswitch.conf cannot be read ({why})"))
    }

    fn accounts(&self) -> Result<Vec<Account>, String> {
        Ok(super::ffi::passwd_entries()?
            .into_iter()
            .map(|entry| Account {
                name: entry.name,
                uid: entry.uid,
                gid: entry.gid,
            })
            .collect())
    }

    fn groups(&self) -> Result<Vec<Group>, String> {
        Ok(super::ffi::group_entries()?
            .into_iter()
            .map(|entry| Group {
                name: entry.name,
                gid: entry.gid,
                members: entry.members,
            })
            .collect())
    }

    fn group_list(&self, account: &Account) -> Result<Vec<u32>, String> {
        super::ffi::group_list(&account.name, account.gid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in account database: every answer fixed, and one lookup
    /// made to fail when `fail` names it.
    #[derive(Clone)]
    struct Stand {
        nsswitch: String,
        accounts: Vec<Account>,
        groups: Vec<Group>,
        /// What `getgrouplist` gives each account, by name.
        lists: Vec<(&'static str, Vec<u32>)>,
        fail: Option<&'static str>,
    }

    impl Source for Stand {
        fn nsswitch(&self) -> Result<String, String> {
            match self.fail {
                Some("nsswitch") => Err("/etc/nsswitch.conf cannot be read (stand-in)".to_owned()),
                _ => Ok(self.nsswitch.clone()),
            }
        }
        fn accounts(&self) -> Result<Vec<Account>, String> {
            match self.fail {
                Some("accounts") => Err("getpwent_r failed: stand-in".to_owned()),
                _ => Ok(self.accounts.clone()),
            }
        }
        fn groups(&self) -> Result<Vec<Group>, String> {
            match self.fail {
                Some("groups") => {
                    Err("getgrent_r needs a buffer past 1024 KiB for one entry".to_owned())
                }
                _ => Ok(self.groups.clone()),
            }
        }
        fn group_list(&self, account: &Account) -> Result<Vec<u32>, String> {
            if self.fail == Some("lists") {
                return Err(
                    "getgrouplist could not list the groups of bob (70000 groups)".to_owned(),
                );
            }
            Ok(self
                .lists
                .iter()
                .find(|(name, _)| name.as_bytes() == account.name)
                .map_or_else(|| vec![account.gid], |(_, list)| list.clone()))
        }
    }

    fn account(name: &str, uid: u32, gid: u32) -> Account {
        Account {
            name: name.as_bytes().to_vec(),
            uid,
            gid,
        }
    }

    fn group(name: &str, gid: u32, members: &[&str]) -> Group {
        Group {
            name: name.as_bytes().to_vec(),
            gid,
            members: members.iter().map(|m| m.as_bytes().to_vec()).collect(),
        }
    }

    const NSSWITCH: &str = "\
# /etc/nsswitch.conf
passwd:         files systemd
group:          files systemd
shadow:         files systemd
hosts:          files mdns4_minimal [NOTFOUND=return] dns
";

    /// A default Ubuntu machine: root, then ada (1000) and bob (1001),
    /// each in a group of their own; both in sudo (27); ada alone in tools
    /// (2000), as a supplementary group.
    fn ubuntu() -> Stand {
        Stand {
            nsswitch: NSSWITCH.to_owned(),
            accounts: vec![
                account("root", 0, 0),
                account("ada", 1000, 1000),
                account("bob", 1001, 1001),
            ],
            groups: vec![
                group("root", 0, &[]),
                group("sudo", 27, &["ada", "bob"]),
                group("ada", 1000, &[]),
                group("bob", 1001, &[]),
                group("tools", 2000, &["ada"]),
            ],
            lists: vec![
                ("root", vec![0]),
                ("ada", vec![1000, 27, 2000]),
                ("bob", vec![1001, 27]),
            ],
            fail: None,
        }
    }

    #[test]
    fn a_sole_primary_gid_owner_passes() {
        assert_eq!(prove(&ubuntu(), 1000, 1000), Ok(()));
        assert_eq!(prove(&ubuntu(), 1001, 1001), Ok(()));
    }

    #[test]
    fn a_sole_supplementary_owner_passes() {
        assert_eq!(prove(&ubuntu(), 2000, 1000), Ok(()));
    }

    #[test]
    fn another_account_with_it_as_its_primary_group_refuses() {
        let mut stand = ubuntu();
        stand.accounts.push(account("carol", 1002, 1000));
        assert_eq!(
            prove(&stand, 1000, 1000),
            Err("carol (uid 1002) has it as its primary group".to_owned())
        );
        // Another user's private group, judged for ada.
        assert_eq!(
            prove(&ubuntu(), 1001, 1000),
            Err("bob (uid 1001) has it as its primary group".to_owned())
        );
    }

    #[test]
    fn another_explicit_member_refuses() {
        assert_eq!(
            prove(&ubuntu(), 27, 1000),
            Err("it lists bob (uid 1001)".to_owned())
        );
        // A listed name with no account is another account to this proof.
        let mut stand = ubuntu();
        stand.groups[2].members.push(b"ghost".to_vec());
        assert_eq!(
            prove(&stand, 1000, 1000),
            Err("it lists ghost, which has no account".to_owned())
        );
    }

    #[test]
    fn a_member_seen_only_through_getgrouplist_refuses() {
        // tools lists only ada, but the initgroups sources put bob in it.
        let mut stand = ubuntu();
        stand.lists[2].1.push(2000);
        assert_eq!(
            prove(&stand, 2000, 1000),
            Err("bob (uid 1001) is in it by getgrouplist".to_owned())
        );
    }

    #[test]
    fn a_missing_group_or_owner_refuses() {
        assert_eq!(
            prove(&ubuntu(), 4242, 1000),
            Err("group 4242 is not in the group database".to_owned())
        );
        assert_eq!(
            prove(&ubuntu(), 1000, 4242),
            Err("its owner, uid 4242, has no account".to_owned())
        );
        // A group the owner is not in, which no one else is in either.
        let mut stand = ubuntu();
        stand.groups.push(group("games", 60, &[]));
        assert_eq!(
            prove(&stand, 60, 1000),
            Err("its owner, uid 1000, is not in it".to_owned())
        );
    }

    #[test]
    fn a_record_listed_twice_refuses() {
        let mut stand = ubuntu();
        stand.accounts.push(account("ada", 1000, 1000));
        assert_eq!(
            prove(&stand, 1000, 1000),
            Err("the account ada is listed twice".to_owned())
        );
        let mut stand = ubuntu();
        stand.groups.push(group("ada2", 1000, &[]));
        assert_eq!(
            prove(&stand, 1000, 1000),
            Err("group 1000 has more than one entry".to_owned())
        );
        let mut stand = ubuntu();
        stand.groups.push(group("ada", 3000, &[]));
        assert_eq!(
            prove(&stand, 1000, 1000),
            Err("the group ada is listed twice".to_owned())
        );
    }

    #[test]
    fn every_source_but_files_then_systemd_refuses() {
        let cases = [
            (
                "passwd: compat",
                "/etc/nsswitch.conf takes passwd entries from compat",
            ),
            (
                "passwd: files ldap",
                "/etc/nsswitch.conf takes passwd entries from ldap",
            ),
            (
                "passwd: files sss",
                "/etc/nsswitch.conf takes passwd entries from sss",
            ),
            (
                "passwd: nis files",
                "/etc/nsswitch.conf takes passwd entries from nis",
            ),
            (
                "passwd: files winbind",
                "/etc/nsswitch.conf takes passwd entries from winbind",
            ),
            (
                "passwd: files altfiles",
                "/etc/nsswitch.conf takes passwd entries from altfiles",
            ),
            (
                "passwd: systemd files",
                "/etc/nsswitch.conf takes passwd entries from systemd files, not files then systemd",
            ),
            (
                "passwd: files files",
                "/etc/nsswitch.conf takes passwd entries from files files, not files then systemd",
            ),
            (
                "passwd: files [NOTFOUND=return",
                "/etc/nsswitch.conf has an unclosed action for passwd",
            ),
            (
                "passwd: files [BOGUS=return]",
                "/etc/nsswitch.conf has a malformed action for passwd: [BOGUS=return]",
            ),
            (
                "passwd: files [NOTFOUND=jump]",
                "/etc/nsswitch.conf has a malformed action for passwd: [NOTFOUND=jump]",
            ),
            (
                "passwd: [NOTFOUND=return] files",
                "/etc/nsswitch.conf has a malformed action for passwd: [NOTFOUND=return]",
            ),
            (
                "passwd: files []",
                "/etc/nsswitch.conf has a malformed action for passwd: []",
            ),
            ("passwd:", "/etc/nsswitch.conf names no sources for passwd"),
        ];
        for (line, why) in cases {
            let mut stand = ubuntu();
            stand.nsswitch = format!("{line}\ngroup: files\n");
            assert_eq!(prove(&stand, 1000, 1000), Err(why.to_owned()), "{line}");
        }
        // The other databases are judged the same way.
        for (text, why) in [
            (
                "passwd: files\ngroup: files sss\n",
                "/etc/nsswitch.conf takes group entries from sss",
            ),
            (
                "passwd: files\ngroup: files\ninitgroups: files ldap\n",
                "/etc/nsswitch.conf takes initgroups entries from ldap",
            ),
            (
                "group: files\n",
                "/etc/nsswitch.conf names no sources for passwd",
            ),
            (
                "passwd: files\n",
                "/etc/nsswitch.conf names no sources for group",
            ),
            (
                "passwd: files\npasswd: files\ngroup: files\n",
                "/etc/nsswitch.conf names passwd twice",
            ),
        ] {
            let mut stand = ubuntu();
            stand.nsswitch = text.to_owned();
            assert_eq!(prove(&stand, 1000, 1000), Err(why.to_owned()), "{text}");
        }
    }

    #[test]
    fn files_then_systemd_with_well_formed_actions_passes() {
        for text in [
            "passwd: files\ngroup: files\n",
            "passwd: files systemd\ngroup: files systemd\ninitgroups: files\n",
            "passwd: files [NOTFOUND=return] systemd\ngroup: files [!UNAVAIL=continue]\n",
            "passwd: files # ldap\ngroup: files\nhosts: dns ldap\n",
        ] {
            let mut stand = ubuntu();
            stand.nsswitch = text.to_owned();
            assert_eq!(prove(&stand, 1000, 1000), Ok(()), "{text}");
        }
    }

    #[test]
    fn a_lookup_that_fails_refuses() {
        for (fail, why) in [
            ("nsswitch", "/etc/nsswitch.conf cannot be read (stand-in)"),
            ("accounts", "getpwent_r failed: stand-in"),
            (
                "groups",
                "getgrent_r needs a buffer past 1024 KiB for one entry",
            ),
            (
                "lists",
                "getgrouplist could not list the groups of bob (70000 groups)",
            ),
        ] {
            let mut stand = ubuntu();
            stand.fail = Some(fail);
            assert_eq!(prove(&stand, 1000, 1000), Err(why.to_owned()), "{fail}");
        }
    }

    /// Off Linux with glibc there is no proof: every group refuses.
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    #[test]
    fn every_group_refuses_off_glibc() {
        assert_eq!(
            owners_alone(0, 0),
            Err(
                "the proof that a group is its owner's alone runs on Linux with glibc only"
                    .to_owned()
            )
        );
    }

    /// The NSS walks read this system's database whole. Its contents are
    /// the system's, so the test asks only what every Linux system has:
    /// root, uid 0, whose primary group is 0.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn the_nss_walks_read_root() {
        let accounts = System.accounts().unwrap();
        let root = accounts
            .iter()
            .find(|account| account.uid == 0)
            .expect("an account of uid 0");
        assert_eq!(root.gid, 0);
        assert!(System.groups().unwrap().iter().any(|group| group.gid == 0));
        assert!(System.group_list(root).unwrap().contains(&0));
    }
}
