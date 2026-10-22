//! The protocol v1 surface, as the machine form of stability.md's 1.x
//! promise: the calls of `scion.v1.Scion` and `scion.v1.Objects` (with
//! their input, output and streaming), the fields of their messages
//! (number, label, type, oneof), and the values of their enums, are
//! additive-only in 1.x. The surface is read from the descriptor set
//! the build wrote — what the compiler saw, not a hand parse of the
//! text — and held against a golden. The golden is a floor, not a
//! ceiling: adding a call, a field or an enum value passes; removing
//! one, renumbering one, or changing an rpc's signature is serving a
//! v2, not changing v1 (docs/compatibility.md).
//! Updating on purpose: re-pin by replacing `protocol_surface_v1.golden`
//! with the diff the failure prints, and name the added surface
//! surface in a changes/ fragment - a removal or a renumber is never a
//! re-pin, it is serving v2.

use std::collections::BTreeSet;

use prost::Message as _;
use prost_types::{FileDescriptorSet, field_descriptor_proto};

const DESCRIPTOR: &str = concat!(env!("OUT_DIR"), "/scion_descriptor.bin");
const GOLDEN: &str = include_str!("protocol_surface_v1.golden");

#[test]
fn the_v1_surface_holds_everything_the_golden_names() {
    let bytes = std::fs::read(DESCRIPTOR)
        .unwrap_or_else(|e| panic!("the build's descriptor set opens: {e}"));
    let set = FileDescriptorSet::decode(bytes.as_slice()).expect("the descriptor set decodes");
    let actual: BTreeSet<String> = surface(&set).into_iter().collect();
    let golden: Vec<&str> = GOLDEN
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    assert!(
        !golden.is_empty(),
        "the golden must name the surface it freezes"
    );
    let missing: Vec<&str> = golden
        .iter()
        .copied()
        .filter(|line| !actual.contains(*line))
        .collect();
    assert!(
        missing.is_empty(),
        "the 1.x protocol surface shrank, renumbered, or changed an rpc's \
         signature; an independent v1 client would break. Restore these, or \
         serve a v2 beside v1 (docs/compatibility.md): {missing:?}"
    );
}

/// Every call, field and enum value the compiled proto names, as one
/// line each:
/// - a call: `Service.rpc:in=Request:out=Reply`, with `:client_stream`
///   and `:server_stream` when the rpc streams;
/// - a field: `Message.field:number:label:type` (and
///   `Message.field:number:oneof name:label type` when it sits in a
///   oneof);
/// - an enum value: `Enum.VALUE:number`.
fn surface(set: &FileDescriptorSet) -> Vec<String> {
    let mut out = Vec::new();
    for file in &set.file {
        for service in &file.service {
            let service_name = service.name.as_deref().unwrap_or("");
            for method in &service.method {
                let name = method.name.as_deref().unwrap_or("");
                let input = short_type(method.input_type.as_deref().unwrap_or(""));
                let output = short_type(method.output_type.as_deref().unwrap_or(""));
                let mut line = format!("{service_name}.{name}:in={input}:out={output}");
                if method.client_streaming == Some(true) {
                    line.push_str(":client_stream");
                }
                if method.server_streaming == Some(true) {
                    line.push_str(":server_stream");
                }
                out.push(line);
            }
        }
        for message in &file.message_type {
            message_lines(message, &mut out);
        }
        for descriptor in &file.enum_type {
            enum_lines(descriptor, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The type as the wire means it: a message or enum type keeps the
/// name it is called by (the package is fixed by the file), a scalar
/// keeps its proto kind, and a repeated field keeps its label, since
/// dropping either changes what a client decodes.
fn short_type(type_name: &str) -> String {
    type_name.rsplit('.').next().unwrap_or("").to_owned()
}

/// The fields of one message, its nested messages and its nested enums.
fn message_lines(descriptor: &prost_types::DescriptorProto, out: &mut Vec<String>) {
    let message = descriptor.name.as_deref().unwrap_or("");
    for field in &descriptor.field {
        let name = field.name.as_deref().unwrap_or("");
        let number = field.number.unwrap_or_default();
        let repeated = field.label == Some(field_descriptor_proto::Label::Repeated as i32);
        let kind = match field.r#type {
            Some(t) if t == field_descriptor_proto::Type::Message as i32 => field
                .type_name
                .as_deref()
                .map_or_else(|| "message".to_owned(), short_type),
            Some(t) if t == field_descriptor_proto::Type::Enum as i32 => field
                .type_name
                .as_deref()
                .map_or_else(|| "enum".to_owned(), short_type),
            Some(t) => match field_descriptor_proto::Type::try_from(t) {
                Ok(kind) => format!("{kind:?}").to_ascii_lowercase(),
                Err(_) => format!("unknown-{t}"),
            },
            None => "unset".into(),
        };
        let label = if repeated { "repeated " } else { "" };
        let line = match field.oneof_index {
            Some(index) => {
                let oneof = descriptor
                    .oneof_decl
                    .get(usize::try_from(index).unwrap_or(usize::MAX))
                    .and_then(|o| o.name.as_deref())
                    .unwrap_or("");
                format!("{message}.{name}:{number}:oneof {oneof}:{label}{kind}")
            }
            None => format!("{message}.{name}:{number}:{label}{kind}"),
        };
        out.push(line);
    }
    for nested in &descriptor.nested_type {
        message_lines(nested, out);
    }
    for descriptor in &descriptor.enum_type {
        enum_lines(descriptor, out);
    }
}

/// The values of one enum, with their numbers: renumbering one changes
/// what every client sends on the wire.
fn enum_lines(descriptor: &prost_types::EnumDescriptorProto, out: &mut Vec<String>) {
    let descriptor_enum = descriptor.name.as_deref().unwrap_or("");
    for value in &descriptor.value {
        out.push(format!(
            "{descriptor_enum}.{}:{}",
            value.name.as_deref().unwrap_or(""),
            value.number.unwrap_or_default()
        ));
    }
}
