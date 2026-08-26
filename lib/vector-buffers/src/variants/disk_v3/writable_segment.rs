use std::io;

use snafu::Snafu;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// End boundary supplied when an active segment is sealed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentEnd {
    pub(crate) segment_byte_offset: u64,
    pub(crate) next_record_id: u64,
}

impl SegmentEnd {
    #[must_use]
    pub(crate) const fn new(segment_byte_offset: u64, next_record_id: u64) -> Self {
        Self {
            segment_byte_offset,
            next_record_id,
        }
    }
}

/// The single segment file currently open for append.
///
/// Frames start at file offset zero. Every successful underlying write advances
/// `write_offset` immediately. The owner awaits each operation to completion;
/// this type does not expose poll-based I/O.
#[derive(Debug)]
pub(crate) struct WritableSegment<F> {
    base_offset: u64,
    file: F,
    write_offset: u64,
    target_size: u64,
}

impl<F> WritableSegment<F>
where
    F: AsyncWrite + Unpin,
{
    /// Wraps a newly created empty segment file.
    ///
    /// The caller must name the file with [`segment_file_name`], create it with
    /// `create_new`, and provide it positioned at offset zero.
    /// Directory synchronization and exclusive directory ownership belong to
    /// the storage layer and writer actor.
    pub(crate) fn new(
        file: F,
        base_offset: u64,
        target_size: u64,
    ) -> Result<Self, WritableSegmentError> {
        Self::resume(file, base_offset, 0, target_size)
    }

    /// Wraps an existing segment file positioned for append.
    pub(crate) fn resume(
        file: F,
        base_offset: u64,
        write_offset: u64,
        target_size: u64,
    ) -> Result<Self, WritableSegmentError> {
        if target_size == 0 {
            return Err(WritableSegmentError::InvalidTargetSize { target_size });
        }
        if write_offset > target_size {
            return Err(WritableSegmentError::ExistingDataExceedsTarget {
                write_offset,
                target_size,
            });
        }

        Ok(Self {
            base_offset,
            file,
            write_offset,
            target_size,
        })
    }

    #[must_use]
    pub(crate) const fn base_offset(&self) -> u64 {
        self.base_offset
    }

    #[must_use]
    pub(crate) const fn write_offset(&self) -> u64 {
        self.write_offset
    }

    #[must_use]
    pub(crate) const fn target_size(&self) -> u64 {
        self.target_size
    }

    #[must_use]
    pub(crate) const fn data_len(&self) -> u64 {
        self.write_offset
    }

    #[must_use]
    pub(crate) const fn remaining_capacity(&self) -> u64 {
        self.target_size - self.write_offset
    }

    #[must_use]
    pub(crate) fn can_fit(&self, frame_len: usize) -> bool {
        u64::try_from(frame_len).is_ok_and(|frame_len| frame_len <= self.remaining_capacity())
    }

    /// Writes one complete batch, retaining progress from partial writes.
    pub(crate) async fn write_all(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let requested = u64::try_from(bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "write length cannot be represented as a u64",
            )
        })?;

        if requested > self.remaining_capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "write of {requested} bytes exceeds remaining segment capacity of {} bytes",
                    self.remaining_capacity()
                ),
            ));
        }

        while !bytes.is_empty() {
            let written = self.file.write(bytes).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write the complete segment batch",
                ));
            }
            if written > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "writer reported more bytes than requested",
                ));
            }

            let written_u64 = u64::try_from(written).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "written byte count cannot be represented as a u64",
                )
            })?;
            self.write_offset = self.write_offset.checked_add(written_u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment write offset overflowed",
                )
            })?;
            bytes = &bytes[written..];
        }

        Ok(())
    }

    /// Makes all bytes written to the file visible to the operating system.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }

    /// Converts an externally synchronized segment into sealed metadata.
    ///
    /// The owner must first flush the writable handle and synchronize the file.
    /// Consuming `self` makes the transition one-time at the type level.
    pub(crate) fn into_sealed(
        self,
        end: SegmentEnd,
    ) -> Result<SealedSegment, WritableSegmentError> {
        if end.segment_byte_offset != self.data_len() {
            return Err(WritableSegmentError::InvalidByteEnd {
                expected: self.data_len(),
                actual: end.segment_byte_offset,
            });
        }

        if end.next_record_id < self.base_offset {
            return Err(WritableSegmentError::RecordIdRegression {
                start: self.base_offset,
                end: end.next_record_id,
            });
        }

        Ok(SealedSegment {
            base_offset: self.base_offset,
            end,
            file_len: self.write_offset,
        })
    }
}

/// Metadata for an immutable segment after its file has been synchronized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SealedSegment {
    base_offset: u64,
    end: SegmentEnd,
    file_len: u64,
}

impl SealedSegment {
    #[must_use]
    pub(crate) const fn base_offset(&self) -> u64 {
        self.base_offset
    }

    #[must_use]
    pub(crate) const fn end(&self) -> SegmentEnd {
        self.end
    }

    #[must_use]
    pub(crate) const fn file_len(&self) -> u64 {
        self.file_len
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum WritableSegmentError {
    #[snafu(display("segment target size must be greater than zero, got {target_size}"))]
    InvalidTargetSize { target_size: u64 },

    #[snafu(display(
        "existing segment contains {write_offset} bytes, exceeding its target size of {target_size} bytes"
    ))]
    ExistingDataExceedsTarget { write_offset: u64, target_size: u64 },

    #[snafu(display("invalid segment byte end: expected {expected}, got {actual}"))]
    InvalidByteEnd { expected: u64, actual: u64 },

    #[snafu(display("segment record ID regressed from {start} to {end}"))]
    RecordIdRegression { start: u64, end: u64 },
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::io::AsyncWrite;

    use super::{SegmentEnd, WritableSegment, WritableSegmentError};
    use crate::variants::disk_v3::segment_files::{
        SegmentFileNameError, parse_segment_file_name, segment_file_name,
    };

    #[derive(Default)]
    struct TestFileState {
        bytes: Mutex<Vec<u8>>,
        flushes: AtomicUsize,
    }

    struct TestFile {
        state: Arc<TestFileState>,
        max_write: usize,
        fail_after_writes: Option<usize>,
        writes: usize,
    }

    impl TestFile {
        fn new(max_write: usize) -> (Self, Arc<TestFileState>) {
            assert!(max_write > 0);
            let state = Arc::new(TestFileState::default());
            (
                Self {
                    state: Arc::clone(&state),
                    max_write,
                    fail_after_writes: None,
                    writes: 0,
                },
                state,
            )
        }

        fn failing_after(max_write: usize, successful_writes: usize) -> (Self, Arc<TestFileState>) {
            let (mut file, state) = Self::new(max_write);
            file.fail_after_writes = Some(successful_writes);
            (file, state)
        }
    }

    impl AsyncWrite for TestFile {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.fail_after_writes == Some(this.writes) {
                return Poll::Ready(Err(std::io::Error::other("injected write failure")));
            }

            let written = bytes.len().min(this.max_write);
            this.state
                .bytes
                .lock()
                .unwrap()
                .extend_from_slice(&bytes[..written]);
            this.writes += 1;
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.state.flushes.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn new_segment_starts_empty() {
        let (file, state) = TestFile::new(7);
        let segment = WritableSegment::new(file, 7, 1024).unwrap();

        assert_eq!(segment.base_offset(), 7);
        assert_eq!(segment.write_offset(), 0);
        assert_eq!(segment.data_len(), 0);
        assert_eq!(segment.target_size(), 1024);
        assert_eq!(segment.remaining_capacity(), 1024);
        assert!(segment.can_fit(1024));
        assert!(!segment.can_fit(1025));
        assert_eq!(state.flushes.load(Ordering::Relaxed), 0);
        assert!(state.bytes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn write_all_handles_partial_underlying_writes() {
        let (file, state) = TestFile::new(3);
        let mut segment = WritableSegment::new(file, 7, 1024).unwrap();
        let payload = b"abcdef";

        segment.write_all(payload).await.unwrap();
        assert_eq!(segment.data_len(), 6);
        segment.flush().await.unwrap();

        let sealed = segment.into_sealed(SegmentEnd::new(6, 9)).unwrap();
        assert_eq!(sealed.base_offset(), 7);
        assert_eq!(sealed.end(), SegmentEnd::new(6, 9));
        assert_eq!(sealed.file_len(), 6);

        let bytes = state.bytes.lock().unwrap();
        assert_eq!(bytes.as_slice(), payload);
    }

    #[tokio::test]
    async fn write_error_preserves_completed_partial_write_offset() {
        let (file, state) = TestFile::failing_after(3, 1);
        let mut segment = WritableSegment::new(file, 7, 1024).unwrap();

        let error = segment.write_all(b"abcdef").await.unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(segment.write_offset(), 3);
        assert_eq!(state.bytes.lock().unwrap().as_slice(), b"abc");
    }

    #[tokio::test]
    async fn write_larger_than_remaining_capacity_is_rejected() {
        let (file, state) = TestFile::new(usize::MAX);
        let mut segment = WritableSegment::new(file, 7, 4).unwrap();

        let error = segment.write_all(b"abcde").await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(segment.write_offset(), 0);
        assert!(state.bytes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn into_sealed_validates_the_segment_byte_end() {
        let (file, _) = TestFile::new(usize::MAX);
        let mut segment = WritableSegment::new(file, 7, 1024).unwrap();
        segment.write_all(b"abc").await.unwrap();
        segment.flush().await.unwrap();

        assert!(matches!(
            segment.into_sealed(SegmentEnd::new(2, 8)),
            Err(WritableSegmentError::InvalidByteEnd {
                expected: 3,
                actual: 2,
            })
        ));
    }

    #[test]
    fn target_size_must_be_nonzero() {
        let (file, _) = TestFile::new(usize::MAX);

        assert!(matches!(
            WritableSegment::new(file, 7, 0),
            Err(WritableSegmentError::InvalidTargetSize { target_size: 0 })
        ));
    }

    #[test]
    fn segment_file_names_are_canonical_record_base_offsets() {
        assert_eq!(segment_file_name(0), "0.log");
        assert_eq!(segment_file_name(42), "42.log");
        assert_eq!(
            parse_segment_file_name(OsStr::new("18446744073709551615.log")).unwrap(),
            u64::MAX
        );

        for invalid in [
            "",
            ".log",
            "00.log",
            "01.log",
            "-1.log",
            "1.tmp",
            "18446744073709551616.log",
        ] {
            assert!(matches!(
                parse_segment_file_name(OsStr::new(invalid)),
                Err(SegmentFileNameError::InvalidExtension | SegmentFileNameError::InvalidOffset)
            ));
        }
    }
}
