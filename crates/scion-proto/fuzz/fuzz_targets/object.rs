//! The object decoder takes what a peer tier or a client sent: a header
//! that names a size and an encoding, and bytes that may be one Zstandard
//! frame. Any input must end in an error, or in decoded bytes of exactly
//! the header's size and never more than the object cap - a frame that
//! claims a small size but inflates past it is refused. What the decoder
//! accepts round-trips: packing the decoded bytes and unpacking them
//! again gives back the same bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_proto::object;
use scion_proto::v1::ObjectPart;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    // The part is what the wire carried. prost's own decoder space is
    // upstream's; what is ours starts at the decoded part.
    let part = match ObjectPart::decode(data) {
        Ok(part) => part,
        Err(_) => return,
    };
    // The frame parser runs on any bytes, also when the header refuses
    // the part: it must never panic, and what it accepts stays under the
    // cap.
    if let Ok(size) = object::frame_size(&part.data) {
        assert!(size <= scion_proto::MAX_OBJECT_BYTES);
    }
    let header = match object::Header::of(&part, true) {
        Ok(header) => header,
        Err(_) => return,
    };
    // The header's own limits, restated on what passed them.
    assert!(header.size <= scion_proto::MAX_OBJECT_BYTES);
    assert!(header.encoded_size <= object::MAX_ENCODED_BYTES);
    if let Ok(data) = object::decode(&header, part.data.to_vec()) {
        // The cap holds on the way out: the decoded bytes are exactly the
        // claimed size, never a byte more.
        assert_eq!(data.len(), header.size);
        assert!(data.len() <= scion_proto::MAX_OBJECT_BYTES);
        // What the decoder accepted round-trips through the encoder.
        let frame = object::pack(&data).expect("the packed frame");
        assert_eq!(object::frame_size(&frame).unwrap(), data.len());
        assert_eq!(object::unpack(&frame, data.len()).unwrap(), data);
    }
});
