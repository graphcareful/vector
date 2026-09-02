use std::{io, io::SeekFrom, num::NonZeroU32};

use bytes::{Bytes, BytesMut};
use snafu::Snafu;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
};

use super::{
    frame::{FRAME_HEADER_LEN, FrameDecodeError, FrameDecodeErrorKind, decode_frame_header},
    position::Position,
};

/// A single segment file positioned at the next unread frame.
///
/// The position advances only after a complete frame has passed all format and
/// checksum validation. An incomplete tail is reported with the cursor restored
/// to the last frame boundary; the caller must repair or reject the segment.
#[derive(Debug)]
pub(crate) struct ReadableSegment<F> {
    file: F,
    position: Position,
    max_frame_len: usize,
    incomplete_tail: bool,
}

impl<F> ReadableSegment<F>
where
    F: AsyncRead + AsyncSeek + Unpin,
{
    /// Wraps a segment file and positions it at the next unread frame.
    pub(crate) async fn new(
        file: F,
        position: Position,
        max_frame_len: usize,
    ) -> Result<Self, ReadableSegmentError> {
        if max_frame_len < FRAME_HEADER_LEN {
            return Err(ReadableSegmentError::InvalidMaxFrameLength {
                max_frame_len,
                minimum: FRAME_HEADER_LEN,
            });
        }

        let mut segment = Self {
            file,
            position: Position::at_segment_start(position.segment_base_offset()),
            max_frame_len,
            incomplete_tail: false,
        };
        segment.seek_to(position).await?;
        Ok(segment)
    }

    /// Position immediately before the next frame that will be read.
    #[must_use]
    pub(crate) const fn position(&self) -> Position {
        self.position
    }

    /// Repositions the segment to a position that the caller has already
    /// established as a frame boundary in this segment.
    pub(crate) async fn seek_to(&mut self, position: Position) -> Result<(), ReadableSegmentError> {
        let segment_base_offset = self.position.segment_base_offset();
        if position.segment_base_offset() != segment_base_offset {
            return Err(ReadableSegmentError::MismatchedSegmentBaseOffset {
                expected: segment_base_offset,
                actual: position.segment_base_offset(),
            });
        }

        let segment_byte_offset = position.segment_byte_offset();
        let file_len = self
            .file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|source| self.io_error(source))?;
        if segment_byte_offset > file_len {
            return Err(ReadableSegmentError::OffsetBeyondEnd {
                segment_byte_offset,
                file_len,
            });
        }

        self.file
            .seek(SeekFrom::Start(segment_byte_offset))
            .await
            .map_err(|source| self.io_error(source))?;
        self.position = position;
        self.incomplete_tail = false;
        Ok(())
    }

    /// Returns the record ID in the next complete frame header without
    /// logically advancing the segment.
    ///
    /// `None` means that a complete next header is not currently available.
    /// [`Self::read_next`] distinguishes a clean end from an incomplete tail.
    pub(crate) async fn peek_next_record_id(
        &mut self,
        segment_byte_limit: u64,
    ) -> Result<Option<u64>, ReadableSegmentError> {
        if self.incomplete_tail {
            return Ok(None);
        }

        let segment_byte_offset = self.position.segment_byte_offset();
        if segment_byte_offset > segment_byte_limit {
            return Err(ReadableSegmentError::ReadLimitBeforePosition {
                segment_byte_offset,
                limit: segment_byte_limit,
            });
        }
        let header_end = segment_byte_offset
            .checked_add(
                u64::try_from(FRAME_HEADER_LEN)
                    .map_err(|_| ReadableSegmentError::OffsetOverflow)?,
            )
            .ok_or(ReadableSegmentError::OffsetOverflow)?;
        if header_end > segment_byte_limit {
            return Ok(None);
        }

        let position = self
            .file
            .stream_position()
            .await
            .map_err(|source| self.io_error(source))?;
        let header_read = read_header(&mut self.file).await;
        self.file
            .seek(SeekFrom::Start(position))
            .await
            .map_err(|source| self.io_error(source))?;

        let Some(header_bytes) = header_read
            .map_err(|source| self.io_error(source))?
            .complete()
        else {
            return Ok(None);
        };
        decode_frame_header(&header_bytes, self.max_frame_len)
            .map(|header| Some(header.record_id()))
            .map_err(|source| {
                let kind = source.kind();
                ReadableSegmentError::Frame {
                    segment_byte_offset: self.position.segment_byte_offset(),
                    kind,
                    source,
                }
            })
    }

    /// Reads and validates the next frame without decoding its payload into the
    /// application record type.
    pub(crate) async fn read_next(
        &mut self,
        segment_byte_limit: u64,
    ) -> Result<SegmentRead, ReadableSegmentError> {
        if self.incomplete_tail {
            return Ok(SegmentRead::IncompleteTail);
        }
        let segment_byte_offset = self.position.segment_byte_offset();
        if segment_byte_offset > segment_byte_limit {
            return Err(ReadableSegmentError::ReadLimitBeforePosition {
                segment_byte_offset,
                limit: segment_byte_limit,
            });
        }
        if segment_byte_offset == segment_byte_limit {
            return Ok(SegmentRead::EndOfAvailableData);
        }
        let header_end = segment_byte_offset
            .checked_add(
                u64::try_from(FRAME_HEADER_LEN)
                    .map_err(|_| ReadableSegmentError::OffsetOverflow)?,
            )
            .ok_or(ReadableSegmentError::OffsetOverflow)?;
        if header_end > segment_byte_limit {
            self.incomplete_tail = true;
            return Ok(SegmentRead::IncompleteTail);
        }
        let header_bytes = match read_header(&mut self.file)
            .await
            .map_err(|source| self.io_error(source))?
        {
            HeaderRead::Complete(header_bytes) => header_bytes,
            HeaderRead::EndOfAvailableData => return Ok(SegmentRead::EndOfAvailableData),
            HeaderRead::IncompleteTail => {
                return self.restore_after_incomplete_tail().await;
            }
        };

        let header = match decode_frame_header(&header_bytes, self.max_frame_len) {
            Ok(header) => header,
            Err(source) => return Err(self.restore_after_frame_error(source).await),
        };
        let frame_len = header.frame_len();
        let frame_len_u64 =
            u64::try_from(frame_len).map_err(|_| ReadableSegmentError::OffsetOverflow)?;
        let next_segment_byte_offset = segment_byte_offset
            .checked_add(frame_len_u64)
            .ok_or(ReadableSegmentError::OffsetOverflow)?;
        if next_segment_byte_offset > segment_byte_limit {
            return self.restore_after_incomplete_tail().await;
        }

        let mut frame_bytes = BytesMut::with_capacity(frame_len);
        frame_bytes.extend_from_slice(&header_bytes);
        frame_bytes.resize(frame_len, 0);
        match self
            .file
            .read_exact(&mut frame_bytes[FRAME_HEADER_LEN..])
            .await
        {
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
                return self.restore_after_incomplete_tail().await;
            }
            Err(source) => {
                return Err(ReadableSegmentError::Io {
                    segment_byte_offset: self.position.segment_byte_offset(),
                    source,
                });
            }
        }

        if let Err(source) = header.validate_payload(&frame_bytes[FRAME_HEADER_LEN..]) {
            return Err(self.restore_after_frame_error(source).await);
        }
        let record_id = header.record_id();
        let event_count = header.event_count();
        let next_record_id = record_id
            .checked_add(u64::from(event_count.get()))
            .ok_or(ReadableSegmentError::OffsetOverflow)?;
        let codec_metadata = header.codec_metadata();
        let frame_bytes = frame_bytes.freeze();
        let frame = OwnedDecodedFrame {
            segment_byte_offset: self.position.segment_byte_offset(),
            frame_len,
            record_id,
            event_count,
            codec_metadata,
            payload: frame_bytes.slice(FRAME_HEADER_LEN..),
        };

        self.position = Position::new(
            self.position.segment_base_offset(),
            next_segment_byte_offset,
            next_record_id,
        );

        Ok(SegmentRead::Frame(frame))
    }

    fn io_error(&self, source: io::Error) -> ReadableSegmentError {
        ReadableSegmentError::Io {
            segment_byte_offset: self.position.segment_byte_offset(),
            source,
        }
    }

    async fn restore_after_frame_error(
        &mut self,
        frame_error: FrameDecodeError,
    ) -> ReadableSegmentError {
        let segment_byte_offset = self.position.segment_byte_offset();
        let kind = frame_error.kind();
        match self.file.seek(SeekFrom::Start(segment_byte_offset)).await {
            Ok(_) => ReadableSegmentError::Frame {
                segment_byte_offset,
                kind,
                source: frame_error,
            },
            Err(source) => ReadableSegmentError::FramePositionRestore {
                segment_byte_offset,
                frame_error,
                source,
            },
        }
    }

    async fn restore_after_incomplete_tail(&mut self) -> Result<SegmentRead, ReadableSegmentError> {
        let segment_byte_offset = self.position.segment_byte_offset();
        self.file
            .seek(SeekFrom::Start(segment_byte_offset))
            .await
            .map_err(|source| self.io_error(source))?;
        self.incomplete_tail = true;
        Ok(SegmentRead::IncompleteTail)
    }
}

impl ReadableSegment<File> {
    /// Returns the current physical end of the segment without moving the file
    /// cursor.
    pub(crate) async fn end_offset(&self) -> Result<u64, ReadableSegmentError> {
        self.file
            .metadata()
            .await
            .map(|metadata| metadata.len())
            .map_err(|source| self.io_error(source))
    }
}

enum HeaderRead {
    Complete([u8; FRAME_HEADER_LEN]),
    EndOfAvailableData,
    IncompleteTail,
}

impl HeaderRead {
    const fn complete(self) -> Option<[u8; FRAME_HEADER_LEN]> {
        match self {
            Self::Complete(header) => Some(header),
            Self::EndOfAvailableData | Self::IncompleteTail => None,
        }
    }
}

async fn read_header<F>(file: &mut F) -> io::Result<HeaderRead>
where
    F: AsyncRead + Unpin,
{
    let mut header_bytes = [0_u8; FRAME_HEADER_LEN];
    match file.read_exact(&mut header_bytes[..1]).await {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(HeaderRead::EndOfAvailableData);
        }
        Err(source) => return Err(source),
    }
    match file.read_exact(&mut header_bytes[1..]).await {
        Ok(_) => Ok(HeaderRead::Complete(header_bytes)),
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            Ok(HeaderRead::IncompleteTail)
        }
        Err(source) => Err(source),
    }
}

/// Result of attempting to read one frame from a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SegmentRead {
    Frame(OwnedDecodedFrame),
    /// EOF occurred exactly at a complete-frame boundary. An active file may
    /// grow later, so this does not imply that the segment is permanently done.
    EndOfAvailableData,
    /// EOF occurred after part of the next frame. The partial bytes have been
    /// left unread and this segment cannot resume without being repositioned.
    IncompleteTail,
}

/// An owned, validated frame whose payload still uses its application codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedDecodedFrame {
    segment_byte_offset: u64,
    frame_len: usize,
    record_id: u64,
    event_count: NonZeroU32,
    codec_metadata: u32,
    payload: Bytes,
}

impl OwnedDecodedFrame {
    #[must_use]
    pub(crate) const fn segment_byte_offset(&self) -> u64 {
        self.segment_byte_offset
    }

    #[must_use]
    pub(crate) const fn frame_len(&self) -> usize {
        self.frame_len
    }

    #[must_use]
    pub(crate) const fn record_id(&self) -> u64 {
        self.record_id
    }

    #[must_use]
    pub(crate) const fn event_count(&self) -> NonZeroU32 {
        self.event_count
    }

    #[must_use]
    pub(crate) const fn codec_metadata(&self) -> u32 {
        self.codec_metadata
    }

    #[must_use]
    pub(crate) fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum ReadableSegmentError {
    #[snafu(display(
        "maximum frame length {max_frame_len} is smaller than the {minimum}-byte frame header"
    ))]
    InvalidMaxFrameLength {
        max_frame_len: usize,
        minimum: usize,
    },

    #[snafu(display("read failed in segment at byte {segment_byte_offset}: {source}"))]
    Io {
        segment_byte_offset: u64,
        source: io::Error,
    },

    #[snafu(display("invalid frame in segment at byte {segment_byte_offset}: {source}"))]
    Frame {
        segment_byte_offset: u64,
        kind: FrameDecodeErrorKind,
        source: FrameDecodeError,
    },

    #[snafu(display(
        "invalid frame in segment at byte {segment_byte_offset}: {frame_error}; failed to restore the read position: {source}"
    ))]
    FramePositionRestore {
        segment_byte_offset: u64,
        frame_error: FrameDecodeError,
        source: io::Error,
    },

    #[snafu(display("segment read offset overflowed"))]
    OffsetOverflow,

    #[snafu(display("segment read position {segment_byte_offset} is beyond byte limit {limit}"))]
    ReadLimitBeforePosition {
        segment_byte_offset: u64,
        limit: u64,
    },

    #[snafu(display(
        "cannot seek to segment byte {segment_byte_offset}: file contains only {file_len} bytes"
    ))]
    OffsetBeyondEnd {
        segment_byte_offset: u64,
        file_len: u64,
    },

    #[snafu(display(
        "cannot seek segment with record base offset {expected} to a position for segment {actual}"
    ))]
    MismatchedSegmentBaseOffset { expected: u64, actual: u64 },
}

#[cfg(test)]
mod tests {
    use std::{
        cmp,
        mem::size_of,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

    use super::*;
    use crate::variants::disk_v3::frame::encode_frame;

    const MAX_FRAME_LEN: usize = 1024;

    #[derive(Clone)]
    struct GrowingReader {
        state: Arc<Mutex<GrowingReaderState>>,
        max_read: usize,
    }

    struct GrowingReaderState {
        bytes: Vec<u8>,
        position: usize,
    }

    impl GrowingReader {
        fn new(bytes: &[u8], max_read: usize) -> Self {
            assert!(max_read > 0);
            Self {
                state: Arc::new(Mutex::new(GrowingReaderState {
                    bytes: bytes.to_vec(),
                    position: 0,
                })),
                max_read,
            }
        }

        fn position(&self) -> usize {
            self.state.lock().unwrap().position
        }
    }

    impl AsyncRead for GrowingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let mut state = self.state.lock().unwrap();
            let available = state.bytes.len() - state.position;
            let read = cmp::min(available, cmp::min(self.max_read, buf.remaining()));
            if read > 0 {
                let end = state.position + read;
                buf.put_slice(&state.bytes[state.position..end]);
                state.position = end;
            }
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncSeek for GrowingReader {
        fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            let current = i128::try_from(state.position).unwrap();
            let len = i128::try_from(state.bytes.len()).unwrap();
            let position = match position {
                SeekFrom::Start(position) => i128::from(position),
                SeekFrom::End(offset) => len + i128::from(offset),
                SeekFrom::Current(offset) => current + i128::from(offset),
            };
            state.position = usize::try_from(position).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid seek position")
            })?;
            Ok(())
        }

        fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            let state = self.state.lock().unwrap();
            Poll::Ready(
                u64::try_from(state.position)
                    .map_err(|_| io::Error::other("seek position does not fit in u64")),
            )
        }
    }

    fn encoded_frame(record_id: u64, event_count: u32, payload: &[u8]) -> Bytes {
        encode_frame(
            record_id,
            NonZeroU32::new(event_count).unwrap(),
            7,
            payload,
            MAX_FRAME_LEN,
        )
        .unwrap()
        .bytes()
        .clone()
    }

    #[tokio::test]
    async fn reads_concatenated_frames_and_advances_only_at_boundaries() {
        let first = encoded_frame(10, 2, b"first");
        let second = encoded_frame(12, 1, b"second");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&first);
        bytes.extend_from_slice(&second);
        let segment_end = u64::try_from(bytes.len()).unwrap();
        let reader = GrowingReader::new(&bytes, 3);
        let reader_position = reader.clone();
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(10), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            Some(10)
        );
        assert_eq!(reader_position.position(), 0);
        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            Some(10)
        );
        assert_eq!(reader_position.position(), 0);
        let SegmentRead::Frame(first_read) = segment.read_next(segment_end).await.unwrap() else {
            panic!("expected first frame");
        };
        assert_eq!(first_read.segment_byte_offset(), 0);
        assert_eq!(first_read.record_id(), 10);
        assert_eq!(first_read.event_count().get(), 2);
        assert_eq!(first_read.codec_metadata(), 7);
        assert_eq!(first_read.payload().as_ref(), b"first");
        assert_eq!(
            segment.position(),
            Position::new(10, u64::try_from(first.len()).unwrap(), 12)
        );

        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            Some(12)
        );
        assert_eq!(reader_position.position(), first.len());
        let SegmentRead::Frame(second_read) = segment.read_next(segment_end).await.unwrap() else {
            panic!("expected second frame");
        };
        assert_eq!(
            second_read.segment_byte_offset(),
            u64::try_from(first.len()).unwrap()
        );
        assert_eq!(second_read.frame_len(), second.len());
        assert_eq!(second_read.payload().as_ref(), b"second");
        assert_eq!(
            segment.position(),
            Position::new(10, u64::try_from(first.len() + second.len()).unwrap(), 13,)
        );
        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            None
        );
        assert_eq!(
            segment.read_next(segment_end).await.unwrap(),
            SegmentRead::EndOfAvailableData
        );
    }

    #[tokio::test]
    async fn incomplete_header_does_not_advance() {
        let frame = encoded_frame(0, 1, b"payload");
        let split = 10;
        let segment_end = u64::try_from(split).unwrap();
        let reader = GrowingReader::new(&frame[..split], usize::MAX);
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(0), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            None
        );
        assert_eq!(
            segment.read_next(segment_end).await.unwrap(),
            SegmentRead::IncompleteTail
        );
        assert_eq!(segment.position(), Position::at_segment_start(0));
        assert_eq!(
            segment.read_next(segment_end).await.unwrap(),
            SegmentRead::IncompleteTail
        );
    }

    #[tokio::test]
    async fn incomplete_payload_does_not_advance() {
        let frame = encoded_frame(0, 1, b"payload");
        let split = FRAME_HEADER_LEN + 2;
        let segment_end = u64::try_from(split).unwrap();
        let reader = GrowingReader::new(&frame[..split], usize::MAX);
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(0), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert_eq!(
            segment.peek_next_record_id(segment_end).await.unwrap(),
            Some(0)
        );
        assert_eq!(
            segment.read_next(segment_end).await.unwrap(),
            SegmentRead::IncompleteTail
        );
        assert_eq!(segment.position(), Position::at_segment_start(0));
        assert_eq!(
            segment.read_next(segment_end).await.unwrap(),
            SegmentRead::IncompleteTail
        );
    }

    #[tokio::test]
    async fn declared_frame_length_cannot_cross_the_read_limit() {
        let mut committed = encoded_frame(0, 1, b"committed").to_vec();
        let committed_len = committed.len();
        let declared_payload_len = u32::try_from(committed_len).unwrap() + 1;
        committed[FRAME_HEADER_LEN - size_of::<u32>()..FRAME_HEADER_LEN]
            .copy_from_slice(&declared_payload_len.to_be_bytes());
        let unpublished = encoded_frame(1, 1, b"unpublished");
        let mut bytes = committed;
        bytes.extend_from_slice(&unpublished);
        let reader = GrowingReader::new(&bytes, usize::MAX);
        let reader_position = reader.clone();
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(0), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert_eq!(
            segment
                .read_next(u64::try_from(committed_len).unwrap())
                .await
                .unwrap(),
            SegmentRead::IncompleteTail
        );
        assert_eq!(segment.position(), Position::at_segment_start(0));
        assert_eq!(reader_position.position(), 0);
    }

    #[tokio::test]
    async fn invalid_header_restores_the_read_position() {
        let first = encoded_frame(0, 1, b"first");
        let mut corrupt = encoded_frame(1, 1, b"corrupt").to_vec();
        corrupt[0] ^= 0xff;
        let mut bytes = first.to_vec();
        bytes.extend_from_slice(&corrupt);
        let segment_end = u64::try_from(bytes.len()).unwrap();
        let reader = GrowingReader::new(&bytes, usize::MAX);
        let reader_position = reader.clone();
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(0), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert!(matches!(
            segment.read_next(segment_end).await,
            Ok(SegmentRead::Frame(frame)) if frame.record_id() == 0
        ));
        let expected_position = Position::new(0, u64::try_from(first.len()).unwrap(), 1);

        for _ in 0..2 {
            assert!(matches!(
                segment.read_next(segment_end).await,
                Err(ReadableSegmentError::Frame {
                    segment_byte_offset,
                    kind: FrameDecodeErrorKind::RecoverableCorruption,
                    source: FrameDecodeError::InvalidMagic { .. },
                }) if segment_byte_offset == u64::try_from(first.len()).unwrap()
            ));
            assert_eq!(segment.position(), expected_position);
            assert_eq!(reader_position.position(), first.len());
        }
    }

    #[tokio::test]
    async fn checksum_mismatch_restores_the_read_position() {
        let first = encoded_frame(0, 1, b"first");
        let mut corrupt = encoded_frame(1, 1, b"corrupt").to_vec();
        *corrupt.last_mut().unwrap() ^= 0xff;
        let mut bytes = first.to_vec();
        bytes.extend_from_slice(&corrupt);
        let segment_end = u64::try_from(bytes.len()).unwrap();
        let reader = GrowingReader::new(&bytes, usize::MAX);
        let reader_position = reader.clone();
        let mut segment =
            ReadableSegment::new(reader, Position::at_segment_start(0), MAX_FRAME_LEN)
                .await
                .unwrap();

        assert!(matches!(
            segment.read_next(segment_end).await,
            Ok(SegmentRead::Frame(frame)) if frame.record_id() == 0
        ));
        let expected_position = Position::new(0, u64::try_from(first.len()).unwrap(), 1);

        for _ in 0..2 {
            assert!(matches!(
                segment.read_next(segment_end).await,
                Err(ReadableSegmentError::Frame {
                    segment_byte_offset,
                    kind: FrameDecodeErrorKind::RecoverableCorruption,
                    source: FrameDecodeError::ChecksumMismatch { .. },
                }) if segment_byte_offset == u64::try_from(first.len()).unwrap()
            ));
            assert_eq!(segment.position(), expected_position);
            assert_eq!(reader_position.position(), first.len());
        }
    }
}
