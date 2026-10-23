//! Pins the v1 wire bytes (`docs/protocol-v1.md`): the encoded form of
//! three fixed messages is `tests/protocol_wire_v1.golden`, assembled
//! by hand from the proto3 grammar, independent of the encoder, and
//! today's encoder must produce exactly those bytes. It also enforces
//! the doc's additive rule: a message that carries a field this
//! descriptor does not know decodes with the known values unchanged
//! ("A server, tier or client MUST ignore fields that it does not
//! know", docs/protocol-v1.md).
//!
//! The golden names the doc version it pins; the pairing test holds
//! that against the doc's own version line, so a re-pin fails CI until
//! the same PR changes docs/protocol-v1.md's version.
//!
//! Updating on purpose: the bytes ARE the grammar, so an update is a
//! new protocol version beside v1 with a changes/ fragment naming the
//! break - never a re-pin of this file.

use std::collections::BTreeMap;

use prost::Message as _;
use scion_proto::v1::{GetChangesetRequest, LogEntry, LogRequest, LogResponse};

const GOLDEN: &str = include_str!("protocol_wire_v1.golden");
const DOC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/protocol-v1.md");

const REPO: &str = "golden";
const ID: [u8; 32] = [7; 32];

/// Parses the golden into `message name -> bytes`, skipping comments.
fn blocks() -> BTreeMap<String, Vec<u8>> {
    let mut out: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut name = String::new();
    for line in GOLDEN.lines() {
        let line = line.trim();
        if let Some(head) = line.strip_prefix("## ") {
            name = head.split_whitespace().next().unwrap().into();
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let hex: String = line.chars().filter(char::is_ascii_hexdigit).collect();
        assert!(!hex.is_empty(), "a golden row must be hex: {line}");
        assert!(!name.is_empty(), "a golden row needs a message header");
        out.entry(name.clone()).or_default().extend(
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()),
        );
    }
    out
}

fn fixed_requests() -> (GetChangesetRequest, LogRequest) {
    (
        GetChangesetRequest {
            repo: REPO.to_owned(),
            changeset_id: ID.to_vec(),
        },
        LogRequest {
            repo: REPO.to_owned(),
            changeset_id: ID.to_vec(),
            limit: 3,
            path: b"a/b".to_vec(),
        },
    )
}

#[test]
fn the_encoder_writes_the_golden_bytes_and_reads_them_back() {
    let golden = blocks();
    let (get, log) = fixed_requests();
    let reply = LogResponse {
        entries: vec![LogEntry {
            changeset_id: ID.to_vec(),
            changeset: b"CS".to_vec(),
        }],
        next_changeset_id: b"c".to_vec(),
    };
    let pinned_get = golden
        .get("GetChangesetRequest")
        .unwrap_or_else(|| panic!("the golden must pin GetChangesetRequest"));
    assert_eq!(
        &get.encode_to_vec(),
        pinned_get,
        "the wire bytes changed: a wire change is a new protocol version \
         beside v1 (docs/protocol-v1.md) and a changes/ fragment naming the \
         break - never a re-pin of the golden"
    );
    let read = GetChangesetRequest::decode(pinned_get.as_slice()).unwrap();
    assert_eq!(read.repo, REPO);
    assert_eq!(read.changeset_id, ID.to_vec());
    let pinned_log = golden
        .get("LogRequest")
        .unwrap_or_else(|| panic!("the golden must pin LogRequest"));
    assert_eq!(&log.encode_to_vec(), pinned_log);
    let read = LogRequest::decode(pinned_log.as_slice()).unwrap();
    assert_eq!(read.repo, REPO);
    assert_eq!(read.changeset_id, ID.to_vec());
    assert_eq!(read.limit, 3);
    assert_eq!(read.path, b"a/b".to_vec());
    let pinned = golden
        .get("LogResponse")
        .expect("the golden pins LogResponse");
    assert_eq!(&reply.encode_to_vec(), pinned);
    let read = LogResponse::decode(pinned.as_slice()).unwrap();
    assert_eq!(read.entries.len(), 1);
    assert_eq!(read.entries[0].changeset_id, ID.as_slice());
    assert_eq!(read.entries[0].changeset, b"CS".to_vec());
    assert_eq!(read.next_changeset_id, b"c".to_vec());
}

/// Writes one proto3 tag as a varint.
fn tag(field: u32, wire: u8) -> Vec<u8> {
    let mut key = (u64::from(field) << 3) | u64::from(wire);
    let mut out = Vec::new();
    loop {
        let byte = (key & 0x7f) as u8;
        key >>= 7;
        if key == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out
}

/// The doc's additive rule, on the wire: one varint field and one
/// length-delimited field this descriptor does not know (999, 998),
/// appended to the golden's bytes, decode with every known value
/// unchanged. If the decoder refused or mangled them, an older reader
/// would break on a newer sender, which docs/protocol-v1.md forbids.
#[test]
fn a_message_with_unknown_fields_decodes_unchanged() {
    let golden = blocks();
    let unknown = [
        tag(999, 0),     // a varint field
        vec![1],         // its value
        tag(998, 2),     // a length-delimited field
        vec![3],         // its length
        b"xyz".to_vec(), // its bytes
    ]
    .concat();
    for name in ["GetChangesetRequest", "LogRequest", "LogResponse"] {
        let pinned = golden.get(name).expect("the golden pins every message");
        let with_unknown = [pinned.as_slice(), &unknown].concat();
        match name {
            "GetChangesetRequest" => {
                let read = GetChangesetRequest::decode(with_unknown.as_slice()).unwrap();
                assert_eq!(
                    (read.repo, read.changeset_id.as_slice()),
                    (REPO.to_owned(), ID.as_slice())
                );
            }
            "LogRequest" => {
                let read = LogRequest::decode(with_unknown.as_slice()).unwrap();
                assert_eq!(
                    (
                        read.repo,
                        read.changeset_id.as_slice(),
                        read.limit,
                        read.path
                    ),
                    (REPO.to_owned(), ID.as_slice(), 3, b"a/b".to_vec())
                );
            }
            "LogResponse" => {
                let read = LogResponse::decode(with_unknown.as_slice()).unwrap();
                assert_eq!(read.entries.len(), 1);
                assert_eq!(read.entries[0].changeset_id, ID.as_slice());
                assert_eq!(read.entries[0].changeset, b"CS".to_vec());
                assert_eq!(read.next_changeset_id, b"c".to_vec());
            }
            other => unreachable!("no such golden block: {other}"),
        }
    }
}

/// The pairing check: the golden names the doc version it pins, and
/// the doc's own version line must say the same. A re-pin of the
/// golden without the doc's version line fails here; the doc's version
/// moving without the golden fails too.
#[test]
fn the_golden_and_the_doc_agree_on_the_version() {
    let pinned = GOLDEN
        .lines()
        .find_map(|l| l.strip_prefix("# The doc version this golden pins:"))
        .expect("the golden names the doc version it pins");
    let pinned: u32 = pinned
        .trim()
        .trim_end_matches('.')
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .expect("the golden's doc version is a number");
    let doc = std::fs::read_to_string(DOC).expect("docs/protocol-v1.md is checked in");
    let doc_version = doc
        .lines()
        .find_map(|l| {
            let at = l.find("describes version ")?;
            let rest = &l[at + "describes version ".len()..];
            rest.chars().next()?.to_digit(10)
        })
        .expect("docs/protocol-v1.md states its version");
    assert_eq!(
        pinned, doc_version,
        "the wire golden and docs/protocol-v1.md disagree on the version: \
         a re-pin needs the same PR to move the doc's version line"
    );
}
