//! How one object travels in a `GetObjects` reply: in an encoding (RAW, or
//! one zstd frame), in parts of at most [`CHUNK_BYTES`] of encoded data.
//!
//! The first part carries a [`Header`]: the object's kind and ID, its
//! encoding, all its encoded bytes and its size. So a receiver knows how much
//! memory the object takes before the rest arrives. A part holds 1 to
//! [`CHUNK_BYTES`] bytes (an empty object is one empty part), and the parts
//! add up to the encoded size. A sender builds the parts with
//! [`Header::parts`]. A receiver checks the header with [`Header::of`],
//! collects the parts with [`Assembler`], unpacks with [`decode`] or
//! [`unpacker`] within the object limit, and then checks the bytes against
//! the ID itself.

use std::io::{self, Read};

use crate::Bytes;
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

/// The magic number of a Zstandard frame (RFC 8878), as it is on the wire.
/// Skippable frames and the legacy formats have other numbers.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Largest window of a zstd frame that a receiver accepts: 8 MiB, the window
/// that RFC 8878 asks every decoder to support. libzstd stays within it at
/// every level up to 19.
pub(crate) const MAX_WINDOW_BYTES: usize = 1 << MAX_WINDOW_LOG;
const MAX_WINDOW_LOG: u32 = 23;

/// Most memory that [`unpacker`] holds besides the bytes it returns: the
/// window, a block, and the decoder's tables.
pub const UNPACK_BYTES: usize = MAX_WINDOW_BYTES + (1 << 20);

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
    /// a part of a length that [`Assembler::push`] refuses.
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

    /// Checks the length of a part that starts at `offset`: 1 to
    /// [`CHUNK_BYTES`], and not past the encoded size. Only the one part of
    /// an empty object is empty.
    fn check_part(&self, offset: usize, len: usize) -> Result<(), String> {
        let left = self.encoded_size - offset;
        if len > CHUNK_BYTES || len > left || (len == 0 && left > 0) {
            return Err(format!(
                "a part of {len} bytes, with {left} of {} bytes left",
                self.encoded_size
            ));
        }
        Ok(())
    }

    /// The parts of this object, from its encoded bytes in `chunks` of 1 to
    /// [`CHUNK_BYTES`] bytes, as [`read_chunks`] makes them.
    #[must_use]
    pub fn parts(&self, chunks: Vec<Bytes>) -> Vec<ObjectPart> {
        debug_assert_eq!(
            chunks.iter().map(Bytes::len).sum::<usize>(),
            self.encoded_size
        );
        let mut chunks = chunks.into_iter();
        let first = self.first_part(chunks.next().unwrap_or_default());
        let later = chunks.map(|data| ObjectPart {
            data,
            ..ObjectPart::default()
        });
        std::iter::once(first).chain(later).collect()
    }

    /// The first part of this object, with `data`.
    #[must_use]
    pub fn first_part(&self, data: Bytes) -> ObjectPart {
        ObjectPart {
            kind: self.kind.into(),
            id: self.id.to_vec(),
            data,
            encoding: self.encoding.into(),
            encoded_size: self.encoded_size as u64,
            size: self.size as u64,
        }
    }
}

/// Reads exactly `len` bytes from `reader` in chunks of [`CHUNK_BYTES`], the
/// last one shorter, each in its own buffer.
///
/// # Errors
///
/// [`io::ErrorKind::UnexpectedEof`] if `reader` ends before `len` bytes, or
/// a read error.
pub fn read_chunks(mut reader: impl Read, len: usize) -> io::Result<Vec<Bytes>> {
    let mut chunks = Vec::with_capacity(len.div_ceil(CHUNK_BYTES).max(1));
    let mut left = len;
    loop {
        let mut chunk = vec![0; left.min(CHUNK_BYTES)];
        reader.read_exact(&mut chunk)?;
        left -= chunk.len();
        chunks.push(Bytes::from(chunk));
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
        let ObjectPart { data: first, .. } = first;
        let mut data = Vec::with_capacity(header.encoded_size);
        data.extend_from_slice(&first);
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
    /// field of the first part (`kind`, `id`, `encoding`, `encoded_size` or
    /// `size`), or its length is not 1 to [`CHUNK_BYTES`] within the encoded
    /// size.
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
            return Err("a part after the first sets a field of the first part".into());
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

/// The size in the header of `frame`, which must be exactly one Zstandard
/// frame (RFC 8878): the frame magic number, so not a skippable frame or a
/// legacy format; no dictionary ID; a window of at most
/// `MAX_WINDOW_BYTES`; its size in its header, at most
/// [`MAX_OBJECT_BYTES`]; and no bytes after it. Reads only the frame's
/// headers, not its content.
///
/// # Errors
///
/// Why `frame` is not one such frame.
pub fn frame_size(frame: &[u8]) -> Result<usize, String> {
    if !frame.starts_with(&ZSTD_MAGIC) {
        return Err("not a Zstandard frame".into());
    }
    let size = match zstd::zstd_safe::get_frame_content_size(frame) {
        Ok(Some(size)) => size,
        Ok(None) => return Err("the zstd frame header gives no size".into()),
        Err(_) => return Err("bad zstd frame header".into()),
    };
    let size = usize::try_from(size)
        .ok()
        .filter(|&size| size <= MAX_OBJECT_BYTES)
        .ok_or_else(|| "the zstd frame header gives a size above the object limit".to_owned())?;
    // The header is whole, so its descriptor and window byte are there.
    let descriptor = frame[4];
    if descriptor & 0b11 != 0 {
        return Err("the zstd frame names a dictionary".into());
    }
    // A single-segment frame has no window byte: its window is its size.
    let window = if descriptor & 0b10_0000 == 0 {
        let byte = frame[5];
        let base = 1_u64 << (10 + u32::from(byte >> 3));
        base + base / 8 * u64::from(byte & 0b111)
    } else {
        size as u64
    };
    if window > MAX_WINDOW_BYTES as u64 {
        return Err(format!("the zstd window of {window} bytes is above 8 MiB"));
    }
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

/// Unpacks a frame that passed [`frame_size`] as a stream, holding at most
/// [`UNPACK_BYTES`] besides what the caller reads. The caller reads at most
/// the frame's size.
///
/// # Errors
///
/// If the decoder cannot start.
pub fn unpacker(frame: &[u8]) -> io::Result<impl Read + '_> {
    let mut decoder = zstd::stream::read::Decoder::with_buffer(frame)?;
    decoder.window_log_max(MAX_WINDOW_LOG)?;
    Ok(decoder)
}

/// Packs `data` into one zstd frame with its size in the header, as the
/// server stores blobs. The frame's buffer is no larger than the frame: the
/// compressor makes room for the worst case, which a buffer kept as `Bytes`
/// would hold on to.
///
/// # Errors
///
/// If the compressor fails.
pub fn pack(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut frame = zstd::bulk::compress(data, zstd::DEFAULT_COMPRESSION_LEVEL)?;
    frame.shrink_to_fit();
    Ok(frame)
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

    /// `parts`, then the object they make again.
    fn assemble(parts: Vec<ObjectPart>) -> Result<(Header, Vec<u8>), String> {
        let mut parts = parts.into_iter();
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
    fn an_object_goes_in_parts_of_one_chunk_but_the_last() {
        for len in [0, 1, CHUNK_BYTES, CHUNK_BYTES + 1, 3 * CHUNK_BYTES + 5] {
            let data: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let header = header(Encoding::Raw, len, len);
            let parts = header.parts(read_chunks(&data[..], len).unwrap());
            assert_eq!(parts.len(), len.div_ceil(CHUNK_BYTES).max(1), "{len}");
            assert!(parts.iter().all(|p| p.data.len() <= CHUNK_BYTES));
            assert_eq!(assemble(parts).unwrap(), (header, data.clone()));
        }
    }

    #[test]
    fn parts_may_be_shorter_than_a_chunk() {
        // One byte at a time: any length from 1 to a chunk is a part.
        let data = b"abc".to_vec();
        let header = header(Encoding::Raw, 3, 3);
        let bytes: Vec<Bytes> = data.iter().map(|&b| Bytes::from(vec![b])).collect();
        assert_eq!(assemble(header.parts(bytes)).unwrap(), (header, data));
    }

    #[test]
    fn a_header_out_of_the_limits_is_refused() {
        let ok = header(Encoding::Raw, 3, 3)
            .parts(vec![Bytes::from_static(b"abc")])
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
        bad(|p| p.encoded_size = 2);
        bad(|p| p.data = Bytes::from_static(b"abcd"));
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
        long.data = Bytes::from(vec![0; zstd_bound(0) + 1]);
        long.encoded_size = long.data.len() as u64;
        long.size = 0;
        let why = Header::of(&long, true).unwrap_err();
        assert!(why.contains("do not fit"), "{why}");
    }

    #[test]
    fn a_wrong_part_sequence_is_refused() {
        let data = vec![1; CHUNK_BYTES + 10];
        let header = header(Encoding::Raw, data.len(), data.len());
        let mut parts = header.parts(read_chunks(&data[..], data.len()).unwrap());
        let first = parts.remove(0);
        let start = || Assembler::new(Header::of(&first, false).unwrap(), first.clone());

        // A part past the encoded size, an empty part, or one that sets a
        // field of the first part.
        for change in [
            (|p: &mut ObjectPart| p.data = Bytes::from(vec![0; 11])) as fn(&mut ObjectPart),
            |p| p.data = Bytes::new(),
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

    /// `data` packed at `level` with a window of 2^`window_log` bytes.
    fn pack_with(data: &[u8], level: i32, window_log: u32) -> Vec<u8> {
        let mut packer = zstd::bulk::Compressor::new(level).unwrap();
        packer
            .set_parameter(zstd::zstd_safe::CParameter::WindowLog(window_log))
            .unwrap();
        packer.compress(data).unwrap()
    }

    #[test]
    fn a_frame_unpacks_only_to_the_size_its_header_and_the_object_give() {
        let data = b"some content, some content, some content".to_vec();
        let frame = pack(&data).unwrap();
        assert_eq!(frame_size(&frame), Ok(data.len()));
        assert_eq!(unpack(&frame, data.len()), Ok(data.clone()));
        let header = header(Encoding::Zstd, frame.len(), data.len());
        assert_eq!(decode(&header, frame.clone()), Ok(data.clone()));
        let mut streamed = Vec::new();
        unpacker(&frame)
            .unwrap()
            .read_to_end(&mut streamed)
            .unwrap();
        assert_eq!(streamed, data);

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
        let why = frame_size(&frame).unwrap_err();
        assert!(why.contains("above the object limit"), "{why}");
    }

    #[test]
    fn only_a_plain_zstd_frame_is_accepted() {
        let data = b"some content, some content, some content".to_vec();
        let frame = pack(&data).unwrap();
        // A skippable frame, which unpacks to nothing: the empty blob.
        let skippable = [0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0];
        let why = frame_size(&skippable).unwrap_err();
        assert!(why.contains("not a Zstandard frame"), "{why}");
        // The magic number of the legacy v0.7 format.
        let mut legacy = frame.clone();
        legacy[0] = 0x27;
        let why = frame_size(&legacy).unwrap_err();
        assert!(why.contains("not a Zstandard frame"), "{why}");
        // A dictionary ID of one byte, after the descriptor of a small
        // single-segment frame.
        assert_eq!(frame[4] & 0b10_0011, 0b10_0000, "{:#x}", frame[4]);
        let mut named = frame.clone();
        named[4] |= 1;
        named.insert(5, 7);
        let named_size = zstd::zstd_safe::get_frame_content_size(&named)
            .ok()
            .flatten();
        assert_eq!(named_size, Some(data.len() as u64));
        let why = frame_size(&named).unwrap_err();
        assert!(why.contains("dictionary"), "{why}");
    }

    #[test]
    fn a_packed_buffer_is_no_larger_than_its_frame() {
        // Content that packs small, in a buffer made for the worst case.
        let frame = pack(&vec![0; 1 << 20]).unwrap();
        assert!(frame.len() < 1024, "{}", frame.len());
        assert!(frame.capacity() <= frame.len() + 64, "{}", frame.capacity());
    }

    #[test]
    fn the_unpacker_refuses_a_window_over_8_mib_by_itself() {
        // A frame with a window of 16 MiB, unpacked without `frame_size`
        // first: the decoder does not take the memory.
        let frame = pack_with(&vec![0; 17 << 20], 1, 24);
        assert!(frame_size(&frame).is_err());
        let mut out = Vec::new();
        let read = unpacker(&frame).unwrap().read_to_end(&mut out);
        assert!(read.is_err(), "{} bytes unpacked", out.len());
    }

    #[test]
    fn a_frame_with_a_window_over_8_mib_is_refused() {
        // A window of 8 MiB is the largest accepted, in a frame larger than
        // it (not single-segment), and in a single-segment frame of 8 MiB.
        let big = vec![0; (8 << 20) + 1];
        let frame = pack_with(&big, 1, 23);
        assert_eq!(frame_size(&frame), Ok(big.len()));
        let mut streamed = Vec::new();
        unpacker(&frame)
            .unwrap()
            .read_to_end(&mut streamed)
            .unwrap();
        assert_eq!(blake3::hash(&streamed), blake3::hash(&big));
        let fits = vec![0; 8 << 20];
        assert_eq!(frame_size(&pack_with(&fits, 1, 24)), Ok(fits.len()));
        // A single-segment frame of 8 MiB and a byte, and a window byte of
        // 16 MiB.
        let why = frame_size(&pack_with(&big, 1, 24)).unwrap_err();
        assert!(why.contains("window"), "{why}");
        let mut wide = pack_with(&vec![0; 9 << 20], 1, 23);
        assert_eq!(wide[4] & 0b10_0000, 0, "not single-segment");
        // The window byte of the frame, set to 16 MiB.
        wide[5] = (24 - 10) << 3;
        let why = frame_size(&wide).unwrap_err();
        assert!(why.contains("window"), "{why}");
    }
}
