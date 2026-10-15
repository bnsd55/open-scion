//! How one object travels in a `GetObjects` reply: in an encoding (RAW, or
//! one zstd frame), in parts of at most [`CHUNK_BYTES`] of encoded data.
//!
//! The first part carries a [`Header`]: the object's kind and ID, its
//! encoding, all its encoded bytes and its size. So a receiver knows how much
//! memory the object takes before the rest arrives. Every part but the last
//! holds exactly [`CHUNK_BYTES`]. A sender builds the parts with
//! [`Header::parts`]. A receiver checks the header with [`Header::of`],
//! collects the parts with [`Assembler`], unpacks with [`decode`] within the
//! object limit, and then checks the bytes against the ID itself.

use std::io::{self, Read};

use crate::v1::{Encoding, ObjectKind, ObjectPart};
use crate::{CHUNK_BYTES, MAX_OBJECT_BYTES};

/// The most bytes that a zstd frame of `size` bytes may take: the bound of
/// the zstd library (`ZSTD_compressBound`). A larger frame is refused.
#[must_use]
pub const fn zstd_bound(size: usize) -> usize {
    const SMALL: usize = 128 << 10;
    let margin = if size < SMALL {
        (SMALL - size) >> 11
    } else {
        0
    };
    size + (size >> 8) + margin
}

/// Largest encoded object: a zstd frame of an object of [`MAX_OBJECT_BYTES`].
pub const MAX_ENCODED_BYTES: usize = zstd_bound(MAX_OBJECT_BYTES);

/// What the first part of an object says about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    /// Tree or blob.
    pub kind: ObjectKind,
    /// The object's ID.
    pub id: [u8; 32],
    /// How its bytes are encoded.
    pub encoding: Encoding,
    /// All its encoded bytes, in all its parts.
    pub encoded_size: usize,
    /// The size of its bytes: a tree's `TreeV1` bytes, a blob's content.
    pub size: usize,
}

impl Header {
    /// Reads the header from the first part of an object, and checks it
    /// against the limits. `zstd` says whether the request accepted ZSTD.
    ///
    /// # Errors
    ///
    /// Why the part cannot start an object: no kind, an ID that is not 32
    /// bytes, an encoding that the request did not accept, a size above
    /// [`MAX_OBJECT_BYTES`], an encoded size that does not fit the size, or
    /// a part of the wrong length.
    pub fn of(first: &ObjectPart, zstd: bool) -> Result<Self, String> {
        let Ok(kind @ (ObjectKind::Tree | ObjectKind::Blob)) = ObjectKind::try_from(first.kind)
        else {
            return Err("the first part of an object has no kind".into());
        };
        let id = <[u8; 32]>::try_from(first.id.as_slice())
            .map_err(|_| "an object ID is not 32 bytes".to_owned())?;
        let encoding = match Encoding::try_from(first.encoding) {
            Ok(Encoding::Raw) => Encoding::Raw,
            Ok(Encoding::Zstd) if zstd => Encoding::Zstd,
            _ => return Err(format!("encoding {} was not asked for", first.encoding)),
        };
        let size = usize::try_from(first.size)
            .ok()
            .filter(|&size| size <= MAX_OBJECT_BYTES)
            .ok_or_else(|| format!("size {} is above the object limit", first.size))?;
        let largest = match encoding {
            Encoding::Raw => size,
            Encoding::Zstd => zstd_bound(size),
        };
        let encoded_size = usize::try_from(first.encoded_size)
            .ok()
            .filter(|&encoded| encoded <= largest)
            .ok_or_else(|| {
                format!(
                    "{} encoded bytes do not fit {size} bytes",
                    first.encoded_size
                )
            })?;
        if encoding == Encoding::Raw && encoded_size != size {
            return Err(format!("{encoded_size} raw bytes for {size} bytes"));
        }
        let header = Self {
            kind,
            id,
            encoding,
            encoded_size,
            size,
        };
        header.check_part(0, first.data.len())?;
        Ok(header)
    }

    /// How many encoded bytes the part that starts at `offset` holds.
    fn part_len(&self, offset: usize) -> usize {
        (self.encoded_size - offset).min(CHUNK_BYTES)
    }

    fn check_part(&self, offset: usize, len: usize) -> Result<(), String> {
        let due = self.part_len(offset);
        if len != due {
            return Err(format!("a part of {len} bytes where {due} were due"));
        }
        Ok(())
    }

    /// The parts of this object, from its encoded bytes in `chunks`: every
    /// chunk but the last of exactly [`CHUNK_BYTES`], as [`chunks`] and
    /// [`read_chunks`] make them.
    #[must_use]
    pub fn parts(&self, chunks: Vec<Vec<u8>>) -> Vec<ObjectPart> {
        debug_assert_eq!(
            chunks.iter().map(Vec::len).sum::<usize>(),
            self.encoded_size
        );
        let mut parts: Vec<ObjectPart> = chunks
            .into_iter()
            .map(|data| ObjectPart {
                data,
                ..ObjectPart::default()
            })
            .collect();
        if parts.is_empty() {
            parts.push(ObjectPart::default());
        }
        let first = &mut parts[0];
        first.kind = self.kind.into();
        first.id = self.id.to_vec();
        first.encoding = self.encoding.into();
        first.encoded_size = self.encoded_size as u64;
        first.size = self.size as u64;
        parts
    }
}

/// Cuts `data` into chunks of [`CHUNK_BYTES`], the last one shorter. Data
/// that fits one chunk is not copied.
#[must_use]
pub fn chunks(data: Vec<u8>) -> Vec<Vec<u8>> {
    if data.len() <= CHUNK_BYTES {
        return vec![data];
    }
    data.chunks(CHUNK_BYTES).map(<[u8]>::to_vec).collect()
}

/// Reads exactly `len` bytes from `reader` in chunks of [`CHUNK_BYTES`], the
/// last one shorter, so they are never all in one buffer.
///
/// # Errors
///
/// [`io::ErrorKind::UnexpectedEof`] if `reader` ends before `len` bytes, or
/// a read error.
pub fn read_chunks(mut reader: impl Read, len: usize) -> io::Result<Vec<Vec<u8>>> {
    let mut chunks = Vec::with_capacity(len.div_ceil(CHUNK_BYTES).max(1));
    let mut left = len;
    loop {
        let mut chunk = vec![0; left.min(CHUNK_BYTES)];
        reader.read_exact(&mut chunk)?;
        left -= chunk.len();
        chunks.push(chunk);
        if left == 0 {
            return Ok(chunks);
        }
    }
}

/// Collects the parts of one object.
#[derive(Debug)]
pub struct Assembler {
    header: Header,
    data: Vec<u8>,
}

impl Assembler {
    /// Starts an object with its first part, whose header [`Header::of`]
    /// read. Takes room for all its encoded bytes now, so the caller reserves
    /// that memory first.
    #[must_use]
    pub fn new(header: Header, first: ObjectPart) -> Self {
        let mut data = first.data;
        data.reserve_exact(header.encoded_size - data.len());
        Self { header, data }
    }

    /// The object's header.
    #[must_use]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Whether every part is in.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.data.len() == self.header.encoded_size
    }

    /// Adds the next part.
    ///
    /// # Errors
    ///
    /// Why it is not the next part: the object is done, the part sets a
    /// field other than `data`, or it has the wrong length.
    pub fn push(&mut self, part: &ObjectPart) -> Result<(), String> {
        if self.is_done() {
            return Err("a part after the last part of an object".into());
        }
        let ObjectPart {
            kind,
            id,
            data,
            encoding,
            encoded_size,
            size,
        } = part;
        if *kind != 0 || !id.is_empty() || *encoding != 0 || *encoded_size != 0 || *size != 0 {
            return Err("a part after the first sets more than data".into());
        }
        self.header.check_part(self.data.len(), data.len())?;
        self.data.extend_from_slice(data);
        Ok(())
    }

    /// The header and all the encoded bytes, once [`Assembler::is_done`].
    #[must_use]
    pub fn finish(self) -> (Header, Vec<u8>) {
        debug_assert!(self.is_done());
        (self.header, self.data)
    }
}

/// The object's bytes from all its encoded bytes `data`, as `header` gives
/// them. A zstd frame is unpacked as [`unpack`] does.
///
/// # Errors
///
/// Why `data` is not `header.size` bytes in its encoding.
pub fn decode(header: &Header, data: Vec<u8>) -> Result<Vec<u8>, String> {
    match header.encoding {
        Encoding::Raw if data.len() == header.size => Ok(data),
        Encoding::Raw => Err(format!("{} raw bytes for {}", data.len(), header.size)),
        Encoding::Zstd => unpack(&data, header.size),
    }
}

/// The size in the header of `frame`, which must be exactly one zstd frame
/// with its size in its header, at most [`MAX_OBJECT_BYTES`]. Reads only the
/// frame's headers, not its content.
///
/// # Errors
///
/// Why `frame` is not one such frame.
pub fn frame_size(frame: &[u8]) -> Result<usize, String> {
    let size = match zstd::zstd_safe::get_frame_content_size(frame) {
        Ok(Some(size)) => size,
        Ok(None) => return Err("the zstd frame header gives no size".into()),
        Err(_) => return Err("bad zstd frame header".into()),
    };
    let size = usize::try_from(size)
        .ok()
        .filter(|&size| size <= MAX_OBJECT_BYTES)
        .ok_or_else(|| "the zstd frame header gives a size above the object limit".to_owned())?;
    match zstd::zstd_safe::find_frame_compressed_size(frame) {
        Ok(len) if len == frame.len() => Ok(size),
        Ok(_) => Err("bytes follow the zstd frame".into()),
        Err(_) => Err("the zstd frame is cut short or bad".into()),
    }
}

/// Unpacks one zstd frame into exactly `size` bytes. The frame must pass
/// [`frame_size`] with that size. The output buffer is the only memory it
/// takes, so it never holds more than `size` bytes.
///
/// # Errors
///
/// Why `frame` does not unpack to `size` bytes.
pub fn unpack(frame: &[u8], size: usize) -> Result<Vec<u8>, String> {
    let header = frame_size(frame)?;
    if header != size {
        return Err(format!(
            "the zstd frame header gives {header} bytes, not {size}"
        ));
    }
    let data = zstd::bulk::decompress(frame, size).map_err(|e| format!("cannot unpack: {e}"))?;
    if data.len() != size {
        return Err(format!("unpacks to {} bytes, not {size}", data.len()));
    }
    Ok(data)
}

/// Packs `data` into one zstd frame with its size in the header, as the
/// server stores blobs.
///
/// # Errors
///
/// If the compressor fails.
pub fn pack(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::bulk::compress(data, zstd::DEFAULT_COMPRESSION_LEVEL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(encoding: Encoding, encoded_size: usize, size: usize) -> Header {
        Header {
            kind: ObjectKind::Blob,
            id: [7; 32],
            encoding,
            encoded_size,
            size,
        }
    }

    /// The parts of `data`, then the object they make again.
    fn round_trip(header: &Header, data: &[u8]) -> Result<(Header, Vec<u8>), String> {
        let mut parts = header.parts(chunks(data.to_vec())).into_iter();
        let first = parts.next().unwrap();
        let mut object = Assembler::new(Header::of(&first, true)?, first);
        for part in parts {
            object.push(&part)?;
        }
        assert!(object.is_done());
        Ok(object.finish())
    }

    #[test]
    fn the_zstd_bound_is_the_libraries() {
        for size in [
            0,
            1,
            1000,
            (128 << 10) - 1,
            128 << 10,
            3 << 20,
            MAX_OBJECT_BYTES,
        ] {
            assert_eq!(
                zstd_bound(size),
                zstd::zstd_safe::compress_bound(size),
                "{size}"
            );
        }
    }

    #[test]
    fn an_object_goes_in_parts_of_exactly_one_chunk_but_the_last() {
        for len in [0, 1, CHUNK_BYTES, CHUNK_BYTES + 1, 3 * CHUNK_BYTES + 5] {
            let data: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let header = header(Encoding::Raw, len, len);
            let parts = header.parts(chunks(data.clone()));
            assert_eq!(parts.len(), len.div_ceil(CHUNK_BYTES).max(1), "{len}");
            assert!(parts.iter().all(|p| p.data.len() <= CHUNK_BYTES));
            assert_eq!(round_trip(&header, &data).unwrap(), (header, data));
            let read = read_chunks(&parts_bytes(&parts)[..], len).unwrap();
            assert_eq!(read, chunks(parts_bytes(&parts)), "{len}");
        }
    }

    fn parts_bytes(parts: &[ObjectPart]) -> Vec<u8> {
        parts.iter().flat_map(|p| p.data.clone()).collect()
    }

    #[test]
    fn a_header_out_of_the_limits_is_refused() {
        let ok = header(Encoding::Raw, 3, 3)
            .parts(vec![b"abc".to_vec()])
            .remove(0);
        assert!(Header::of(&ok, false).is_ok());
        let bad = |change: fn(&mut ObjectPart)| {
            let mut part = ok.clone();
            change(&mut part);
            Header::of(&part, false).unwrap_err()
        };
        bad(|p| p.kind = 0);
        bad(|p| {
            p.id.pop();
        });
        bad(|p| p.encoding = Encoding::Zstd.into());
        bad(|p| p.encoding = 7);
        bad(|p| p.encoded_size = 4);
        bad(|p| p.data.push(b'd'));
        // ZSTD, if asked for: a size over the limit, or more encoded bytes
        // than a frame of its size may take, is refused.
        let mut zstd = ok.clone();
        zstd.encoding = Encoding::Zstd.into();
        assert!(Header::of(&zstd, true).is_ok());
        let mut huge = zstd.clone();
        huge.size = MAX_OBJECT_BYTES as u64 + 1;
        let why = Header::of(&huge, true).unwrap_err();
        assert!(why.contains("above the object limit"), "{why}");
        let mut long = zstd;
        long.data = vec![0; zstd_bound(0) + 1];
        long.encoded_size = long.data.len() as u64;
        long.size = 0;
        let why = Header::of(&long, true).unwrap_err();
        assert!(why.contains("do not fit"), "{why}");
    }

    #[test]
    fn a_wrong_part_sequence_is_refused() {
        let data = vec![1; CHUNK_BYTES + 10];
        let header = header(Encoding::Raw, data.len(), data.len());
        let mut parts = header.parts(chunks(data));
        let first = parts.remove(0);
        let start = || Assembler::new(Header::of(&first, false).unwrap(), first.clone());

        // A short or long last part, or one that sets a header field.
        for change in [
            (|p: &mut ObjectPart| p.data.push(0)) as fn(&mut ObjectPart),
            |p| {
                p.data.pop();
            },
            |p| p.size = 1,
            |p| p.kind = ObjectKind::Blob.into(),
        ] {
            let mut part = parts[0].clone();
            change(&mut part);
            assert!(start().push(&part).is_err());
        }
        // Not done before the last part, and nothing after it.
        let mut object = start();
        assert!(!object.is_done());
        object.push(&parts[0]).unwrap();
        assert!(object.is_done());
        assert!(object.push(&parts[0]).is_err());
    }

    #[test]
    fn a_frame_unpacks_only_to_the_size_its_header_and_the_object_give() {
        let data = b"some content, some content, some content".to_vec();
        let frame = pack(&data).unwrap();
        assert_eq!(frame_size(&frame), Ok(data.len()));
        assert_eq!(unpack(&frame, data.len()), Ok(data.clone()));
        let header = header(Encoding::Zstd, frame.len(), data.len());
        assert_eq!(decode(&header, frame.clone()), Ok(data.clone()));

        // The object says another size than the frame header.
        assert!(unpack(&frame, data.len() - 1).is_err());
        assert!(unpack(&frame, data.len() + 1).is_err());
        // Cut short, or with bytes after the frame.
        assert!(frame_size(&frame[..frame.len() - 1]).is_err());
        assert!(frame_size(&[frame.clone(), frame.clone()].concat()).is_err());
        assert!(unpack(&frame[..frame.len() - 1], data.len()).is_err());
        // No size in the header: a streamed frame.
        let mut streamed = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        io::Write::write_all(&mut streamed, &data).unwrap();
        let streamed = streamed.finish().unwrap();
        assert!(frame_size(&streamed).is_err());
        // A header that gives more than the object limit.
        let mut frame = Vec::new();
        let mut encoder = zstd::stream::Encoder::new(&mut frame, 1).unwrap();
        encoder
            .set_pledged_src_size(Some(MAX_OBJECT_BYTES as u64 + 1))
            .unwrap();
        io::copy(
            &mut io::repeat(0).take(MAX_OBJECT_BYTES as u64 + 1),
            &mut encoder,
        )
        .unwrap();
        encoder.finish().unwrap();
        assert!(
            frame_size(&frame)
                .unwrap_err()
                .contains("above the object limit")
        );
    }
}
