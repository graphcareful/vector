use std::num::NonZeroU32;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use snafu::Snafu;

/// Identifies the start of every disk-v3 frame.
pub(crate) const FRAME_MAGIC: [u8; 4] = *b"VDB3";
/// The only frame format understood by this implementation.
pub(crate) const FRAME_VERSION: u8 = 1;
/// Size of the fixed version-1 frame header.
pub(crate) const FRAME_HEADER_LEN: usize = 34;

const FRAME_FLAGS: u8 = 0;
const ENCODABLE_PAYLOAD_CODEC: u16 = 1;

/// The validated, typed representation of a version-1 frame header.
///
/// Encoding and decoding are explicit so the on-disk format is independent of
/// Rust's in-memory struct layout and the host's byte order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
    magic: [u8; 4],
    version: u8,
    flags: u8,
    record_id: u64,
    event_count: NonZeroU32,
    payload_codec: u16,
    reserved: u16,
    codec_metadata: u32,
    checksum: u32,
    payload_len: u32,
}

impl FrameHeader {
    const fn new(
        record_id: u64,
        event_count: NonZeroU32,
        codec_metadata: u32,
        payload_len: u32,
    ) -> Self {
        Self {
            magic: FRAME_MAGIC,
            version: FRAME_VERSION,
            flags: FRAME_FLAGS,
            record_id,
            event_count,
            payload_codec: ENCODABLE_PAYLOAD_CODEC,
            reserved: 0,
            codec_metadata,
            checksum: 0,
            payload_len,
        }
    }

    fn encode(self) -> [u8; FRAME_HEADER_LEN] {
        let mut encoded = [0_u8; FRAME_HEADER_LEN];
        let mut output = encoded.as_mut_slice();
        output.put_slice(&self.magic);
        output.put_u8(self.version);
        output.put_u8(self.flags);
        output.put_u64(self.record_id);
        output.put_u32(self.event_count.get());
        output.put_u16(self.payload_codec);
        output.put_u16(self.reserved);
        output.put_u32(self.codec_metadata);
        output.put_u32(self.checksum);
        output.put_u32(self.payload_len);
        encoded
    }

    fn decode(input: &[u8], max_frame_len: usize) -> Result<Self, FrameDecodeError> {
        if input.len() < FRAME_HEADER_LEN {
            return Err(FrameDecodeError::IncompleteHeader {
                available: input.len(),
                required: FRAME_HEADER_LEN,
            });
        }

        let mut input = &input[..FRAME_HEADER_LEN];
        let mut magic = [0_u8; 4];
        input.copy_to_slice(&mut magic);
        if magic != FRAME_MAGIC {
            return Err(FrameDecodeError::InvalidMagic { actual: magic });
        }

        let version = input.get_u8();
        if version != FRAME_VERSION {
            return Err(FrameDecodeError::UnsupportedVersion { version });
        }

        let flags = input.get_u8();
        if flags != FRAME_FLAGS {
            return Err(FrameDecodeError::UnsupportedFlags { flags });
        }

        let record_id = input.get_u64();
        let event_count =
            NonZeroU32::new(input.get_u32()).ok_or(FrameDecodeError::ZeroEventCount)?;

        let payload_codec = input.get_u16();
        if payload_codec != ENCODABLE_PAYLOAD_CODEC {
            return Err(FrameDecodeError::UnsupportedPayloadCodec {
                codec: payload_codec,
            });
        }

        let reserved = input.get_u16();
        if reserved != 0 {
            return Err(FrameDecodeError::NonzeroReservedField { value: reserved });
        }

        let codec_metadata = input.get_u32();
        let checksum = input.get_u32();
        let payload_len = input.get_u32();
        let frame_len = FRAME_HEADER_LEN
            .checked_add(
                usize::try_from(payload_len).map_err(|_| FrameDecodeError::LengthOverflow)?,
            )
            .ok_or(FrameDecodeError::LengthOverflow)?;
        if frame_len > max_frame_len {
            return Err(FrameDecodeError::DeclaredFrameTooLarge {
                frame_len,
                limit: max_frame_len,
            });
        }

        Ok(Self {
            magic,
            version,
            flags,
            record_id,
            event_count,
            payload_codec,
            reserved,
            codec_metadata,
            checksum,
            payload_len,
        })
    }

    fn calculate_checksum(mut self, payload: &[u8]) -> u32 {
        self.checksum = 0;
        let mut hasher = Hasher::new();
        hasher.update(&self.encode());
        hasher.update(payload);
        hasher.finalize()
    }

    /// Validates the encoded application payload described by this header.
    /// Bytes after the declared payload are ignored.
    pub(super) fn validate_payload(self, input: &[u8]) -> Result<&[u8], FrameDecodeError> {
        let payload_len = usize::try_from(self.payload_len)
            .expect("Vector only supports platforms where u32 fits in usize");
        if input.len() < payload_len {
            return Err(FrameDecodeError::IncompleteFrame {
                available: FRAME_HEADER_LEN.saturating_add(input.len()),
                required: self.frame_len(),
            });
        }

        let payload = &input[..payload_len];
        let calculated_checksum = self.calculate_checksum(payload);
        if self.checksum != calculated_checksum {
            return Err(FrameDecodeError::ChecksumMismatch {
                calculated: calculated_checksum,
                actual: self.checksum,
            });
        }

        Ok(payload)
    }

    #[must_use]
    pub(crate) fn frame_len(self) -> usize {
        FRAME_HEADER_LEN
            + usize::try_from(self.payload_len)
                .expect("Vector only supports platforms where u32 fits in usize")
    }

    #[must_use]
    pub(crate) const fn record_id(self) -> u64 {
        self.record_id
    }

    #[must_use]
    pub(crate) const fn event_count(self) -> NonZeroU32 {
        self.event_count
    }

    #[must_use]
    pub(crate) const fn codec_metadata(self) -> u32 {
        self.codec_metadata
    }
}

/// A complete, self-delimiting frame ready to be appended to a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFrame {
    event_count: NonZeroU32,
    bytes: Bytes,
}

impl PreparedFrame {
    #[cfg(test)]
    pub(crate) const fn from_test_bytes(bytes: Bytes) -> Self {
        Self {
            event_count: NonZeroU32::MIN,
            bytes,
        }
    }

    #[must_use]
    pub(crate) const fn event_count(&self) -> NonZeroU32 {
        self.event_count
    }

    #[must_use]
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

/// Wraps an already encoded `Encodable` payload in a complete version-1 frame.
pub(crate) fn encode_frame(
    record_id: u64,
    event_count: NonZeroU32,
    codec_metadata: u32,
    payload: &[u8],
    max_frame_len: usize,
) -> Result<PreparedFrame, FrameEncodeError> {
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload.len())
        .ok_or(FrameEncodeError::FrameLengthOverflow)?;
    if frame_len > max_frame_len {
        return Err(FrameEncodeError::EncodedFrameTooLarge {
            frame_len,
            limit: max_frame_len,
        });
    }

    let payload_len =
        u32::try_from(payload.len()).map_err(|_| FrameEncodeError::FrameLengthOverflow)?;
    let mut header = FrameHeader::new(record_id, event_count, codec_metadata, payload_len);
    header.checksum = header.calculate_checksum(payload);

    let mut bytes = BytesMut::with_capacity(frame_len);
    bytes.put_slice(&header.encode());
    bytes.put_slice(payload);

    Ok(PreparedFrame {
        event_count,
        bytes: bytes.freeze(),
    })
}

/// Validates a complete version-1 header and returns the information needed to
/// read and decode the rest of its frame.
pub(crate) fn decode_frame_header(
    input: &[u8],
    max_frame_len: usize,
) -> Result<FrameHeader, FrameDecodeError> {
    FrameHeader::decode(input, max_frame_len)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Snafu)]
pub(crate) enum FrameEncodeError {
    #[snafu(display("frame length overflowed"))]
    FrameLengthOverflow,

    #[snafu(display(
        "encoded frame length {frame_len} exceeds the configured maximum of {limit} bytes"
    ))]
    EncodedFrameTooLarge { frame_len: usize, limit: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Snafu)]
pub(crate) enum FrameDecodeError {
    #[snafu(display("incomplete frame header: found {available} bytes but need {required}"))]
    IncompleteHeader { available: usize, required: usize },

    #[snafu(display("invalid frame magic {actual:?}"))]
    InvalidMagic { actual: [u8; 4] },

    #[snafu(display("unsupported frame version {version}"))]
    UnsupportedVersion { version: u8 },

    #[snafu(display("unsupported frame flags 0x{flags:02x}"))]
    UnsupportedFlags { flags: u8 },

    #[snafu(display("frame length {frame_len} exceeds the configured maximum of {limit} bytes"))]
    DeclaredFrameTooLarge { frame_len: usize, limit: usize },

    #[snafu(display("frame length cannot be represented on this platform"))]
    LengthOverflow,

    #[snafu(display("frame event count must be nonzero"))]
    ZeroEventCount,

    #[snafu(display("unsupported payload codec {codec}"))]
    UnsupportedPayloadCodec { codec: u16 },

    #[snafu(display("reserved frame field must be zero, got {value}"))]
    NonzeroReservedField { value: u16 },

    #[snafu(display("incomplete frame: found {available} bytes but need {required}"))]
    IncompleteFrame { available: usize, required: usize },

    #[snafu(display(
        "frame checksum mismatch: calculated 0x{calculated:08x}, stored 0x{actual:08x}"
    ))]
    ChecksumMismatch { calculated: u32, actual: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameDecodeErrorKind {
    RecoverableCorruption,
    IncompatibleFormat,
    IncompleteData,
}

impl FrameDecodeError {
    #[must_use]
    pub(crate) const fn kind(&self) -> FrameDecodeErrorKind {
        match self {
            Self::InvalidMagic { .. }
            | Self::DeclaredFrameTooLarge { .. }
            | Self::LengthOverflow
            | Self::ZeroEventCount
            | Self::NonzeroReservedField { .. }
            | Self::ChecksumMismatch { .. } => FrameDecodeErrorKind::RecoverableCorruption,
            Self::UnsupportedVersion { .. }
            | Self::UnsupportedFlags { .. }
            | Self::UnsupportedPayloadCodec { .. } => FrameDecodeErrorKind::IncompatibleFormat,
            Self::IncompleteHeader { .. } | Self::IncompleteFrame { .. } => {
                FrameDecodeErrorKind::IncompleteData
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_FRAME_LEN: usize = 1024;

    fn encode(
        record_id: u64,
        event_count: NonZeroU32,
        codec_metadata: u32,
        payload: &[u8],
    ) -> Result<PreparedFrame, FrameEncodeError> {
        encode_frame(
            record_id,
            event_count,
            codec_metadata,
            payload,
            MAX_FRAME_LEN,
        )
    }

    fn decode(input: &[u8]) -> Result<(FrameHeader, &[u8]), FrameDecodeError> {
        let header = decode_frame_header(input, MAX_FRAME_LEN)?;
        let payload = header.validate_payload(&input[FRAME_HEADER_LEN..])?;
        Ok((header, payload))
    }

    #[test]
    fn version_one_encoding_is_stable() {
        let frame = encode(
            0x0102_0304_0506_0708,
            NonZeroU32::new(3).unwrap(),
            7,
            b"abc",
        )
        .unwrap();

        let expected = [
            0x56, 0x44, 0x42, 0x33, 0x01, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x2a, 0x67,
            0x7a, 0xd9, 0x00, 0x00, 0x00, 0x03, 0x61, 0x62, 0x63,
        ];

        assert_eq!(frame.bytes(), &expected[..]);
    }

    #[test]
    fn encoded_frame_round_trips() {
        let frame = encode(42, NonZeroU32::new(5).unwrap(), 0xaabb_ccdd, b"payload").unwrap();
        let (header, payload) = decode(frame.bytes()).unwrap();

        assert_eq!(header.frame_len(), frame.bytes().len());
        assert_eq!(header.record_id(), 42);
        assert_eq!(header.event_count().get(), 5);
        assert_eq!(header.codec_metadata(), 0xaabb_ccdd);
        assert_eq!(payload, b"payload");
    }

    #[test]
    fn decoder_reads_concatenated_frames_one_at_a_time() {
        let first = encode(4, NonZeroU32::new(2).unwrap(), 1, b"one").unwrap();
        let second = encode(6, NonZeroU32::new(1).unwrap(), 2, b"two").unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        let (first_header, first_payload) = decode(&concatenated).unwrap();
        let (second_header, second_payload) =
            decode(&concatenated[first_header.frame_len()..]).unwrap();

        assert_eq!(first_payload, b"one");
        assert_eq!(second_header.record_id(), 6);
        assert_eq!(second_payload, b"two");
    }

    #[test]
    fn decoder_rejects_truncated_header_and_payload() {
        let frame = encode(0, NonZeroU32::MIN, 0, b"payload").unwrap();

        assert_eq!(
            decode(&frame.bytes()[..FRAME_HEADER_LEN - 1]),
            Err(FrameDecodeError::IncompleteHeader {
                available: FRAME_HEADER_LEN - 1,
                required: FRAME_HEADER_LEN,
            })
        );
        assert_eq!(
            decode(&frame.bytes()[..frame.bytes().len() - 1]),
            Err(FrameDecodeError::IncompleteFrame {
                available: frame.bytes().len() - 1,
                required: frame.bytes().len(),
            })
        );
    }

    #[test]
    fn encoder_and_decoder_reject_oversized_frames() {
        assert_eq!(
            encode_frame(0, NonZeroU32::MIN, 0, b"x", FRAME_HEADER_LEN),
            Err(FrameEncodeError::EncodedFrameTooLarge {
                frame_len: FRAME_HEADER_LEN + 1,
                limit: FRAME_HEADER_LEN,
            })
        );

        let frame = encode(0, NonZeroU32::MIN, 0, b"payload").unwrap();
        let mut oversized = frame.bytes().to_vec();
        let oversized_payload_len = MAX_FRAME_LEN + 1 - FRAME_HEADER_LEN;
        oversized[30..34]
            .copy_from_slice(&u32::try_from(oversized_payload_len).unwrap().to_be_bytes());
        assert_eq!(
            decode(&oversized),
            Err(FrameDecodeError::DeclaredFrameTooLarge {
                frame_len: MAX_FRAME_LEN + 1,
                limit: MAX_FRAME_LEN,
            })
        );
    }

    #[test]
    fn decoder_rejects_checksum_mismatch() {
        let frame = encode(0, NonZeroU32::MIN, 0, b"payload").unwrap();
        let mut corrupted = frame.bytes().to_vec();
        *corrupted.last_mut().unwrap() ^= 0xff;

        assert!(matches!(
            decode(&corrupted),
            Err(FrameDecodeError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn decoder_rejects_unsupported_version() {
        let frame = encode(0, NonZeroU32::MIN, 0, b"payload").unwrap();
        let mut unsupported = frame.bytes().to_vec();
        unsupported[4] = FRAME_VERSION + 1;

        assert_eq!(
            decode(&unsupported),
            Err(FrameDecodeError::UnsupportedVersion {
                version: FRAME_VERSION + 1,
            })
        );
    }

    #[test]
    fn decode_errors_are_classified() {
        use FrameDecodeErrorKind::{IncompatibleFormat, IncompleteData, RecoverableCorruption};

        let cases = [
            (
                FrameDecodeError::IncompleteHeader {
                    available: 1,
                    required: FRAME_HEADER_LEN,
                },
                IncompleteData,
            ),
            (
                FrameDecodeError::InvalidMagic { actual: [0; 4] },
                RecoverableCorruption,
            ),
            (
                FrameDecodeError::UnsupportedVersion { version: 2 },
                IncompatibleFormat,
            ),
            (
                FrameDecodeError::UnsupportedFlags { flags: 1 },
                IncompatibleFormat,
            ),
            (
                FrameDecodeError::DeclaredFrameTooLarge {
                    frame_len: 1025,
                    limit: 1024,
                },
                RecoverableCorruption,
            ),
            (FrameDecodeError::LengthOverflow, RecoverableCorruption),
            (FrameDecodeError::ZeroEventCount, RecoverableCorruption),
            (
                FrameDecodeError::UnsupportedPayloadCodec { codec: 2 },
                IncompatibleFormat,
            ),
            (
                FrameDecodeError::NonzeroReservedField { value: 1 },
                RecoverableCorruption,
            ),
            (
                FrameDecodeError::IncompleteFrame {
                    available: FRAME_HEADER_LEN,
                    required: FRAME_HEADER_LEN + 1,
                },
                IncompleteData,
            ),
            (
                FrameDecodeError::ChecksumMismatch {
                    calculated: 1,
                    actual: 2,
                },
                RecoverableCorruption,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.kind(), expected, "{error}");
        }
    }
}
