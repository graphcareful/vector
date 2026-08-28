use std::{
    cmp::Ordering,
    io,
    ops::Bound::{Excluded, Unbounded},
    path::{Path, PathBuf},
    sync::Arc,
};

use snafu::Snafu;
use tokio::fs::File;
use tracing::warn;

use super::{
    position::Position,
    readable_segment::{OwnedDecodedFrame, ReadableSegment, ReadableSegmentError, SegmentRead},
    segment_files::{SegmentFileError, list_segment_base_offsets, segment_file_name},
};

/// Reads complete frames across record-base-offset-named segment files.
///
/// This type only tracks the read cursor. Each returned frame includes the
/// position after that frame; acknowledgement and checkpoint persistence of
/// that position belong to the caller.
pub(crate) struct SegmentedLogReader {
    directory: Arc<PathBuf>,
    active_segment: ReadableSegment<File>,
    sealed_segment_end_offset: Option<u64>,
    max_frame_len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredLogTail {
    position: Position,
}

impl RecoveredLogTail {
    #[must_use]
    pub(crate) const fn position(self) -> Position {
        self.position
    }
}

impl SegmentedLogReader {
    /// Returns the beginning of the earliest retained segment.
    pub(crate) async fn earliest_position(
        directory: &Path,
    ) -> Result<Option<Position>, SegmentedLogReaderError> {
        Ok(list_segment_base_offsets(directory)
            .await
            .map_err(|source| SegmentedLogReaderError::SegmentFiles { source })?
            .first()
            .copied()
            .map(Position::at_segment_start))
    }

    /// Validates every available frame and returns the active segment's tail.
    pub(crate) async fn recover_tail(
        directory: impl Into<PathBuf>,
        max_frame_len: usize,
    ) -> Result<Option<RecoveredLogTail>, SegmentedLogReaderError> {
        let directory = directory.into();
        let segment_base_offsets = list_segment_base_offsets(&directory)
            .await
            .map_err(|source| SegmentedLogReaderError::SegmentFiles { source })?;
        let Some(first_segment_base_offset) = segment_base_offsets.first().copied() else {
            return Ok(None);
        };
        let mut reader = Self::open(
            &directory,
            Position::at_segment_start(first_segment_base_offset),
            max_frame_len,
        )
        .await?;

        loop {
            let segment_base_offset = reader.active_segment.position().segment_base_offset();
            let segment_byte_limit =
                reader.active_segment.end_offset().await.map_err(|source| {
                    SegmentedLogReaderError::Segment {
                        segment_base_offset,
                        source,
                    }
                })?;

            loop {
                let expected_position = reader.active_segment.position();
                match reader
                    .active_segment
                    .read_next(segment_byte_limit)
                    .await
                    .map_err(|source| SegmentedLogReaderError::Segment {
                        segment_base_offset,
                        source,
                    })? {
                    SegmentRead::Frame(frame) => {
                        reader.finish_frame(frame, expected_position, None).await?;
                    }
                    SegmentRead::EndOfAvailableData => break,
                    SegmentRead::IncompleteTail => {
                        let position = reader.read_position();
                        return Err(SegmentedLogReaderError::IncompleteTail {
                            segment_base_offset: position.segment_base_offset(),
                            segment_byte_offset: position.segment_byte_offset(),
                        });
                    }
                }
            }

            if !reader.advance_segment().await? {
                return Ok(Some(RecoveredLogTail {
                    position: reader.read_position(),
                }));
            }
        }
    }

    /// Opens the position's segment and validates its next record ID when a
    /// complete next header is available.
    pub(crate) async fn open(
        directory: impl Into<PathBuf>,
        position: Position,
        max_frame_len: usize,
    ) -> Result<Self, SegmentedLogReaderError> {
        let directory = Arc::new(directory.into());
        let active_segment = Self::try_open_at(&directory, position, max_frame_len)
            .await?
            .ok_or(SegmentedLogReaderError::MissingSegment {
                segment_base_offset: position.segment_base_offset(),
            })?;

        Ok(Self {
            directory,
            active_segment,
            sealed_segment_end_offset: None,
            max_frame_len,
        })
    }

    /// Position immediately before the next frame that will be read.
    #[must_use]
    pub(crate) const fn read_position(&self) -> Position {
        self.active_segment.position()
    }

    /// Reads the next frame without advancing beyond `committed`.
    ///
    /// The reader moves to the next higher segment when necessary and reports
    /// [`SegmentedRead::CaughtUp`] without performing I/O when its logical
    /// record position has reached the committed position.
    pub(crate) async fn read_next(
        &mut self,
        committed: Position,
    ) -> Result<SegmentedRead, SegmentedLogReaderError> {
        loop {
            let expected_position = self.active_segment.position();
            match expected_position
                .next_record_id()
                .cmp(&committed.next_record_id())
            {
                Ordering::Equal => return Ok(SegmentedRead::CaughtUp),
                Ordering::Greater => {
                    return Err(SegmentedLogReaderError::ReaderBeyondCommitted {
                        read_position: expected_position,
                        committed,
                    });
                }
                Ordering::Less => {}
            }
            if expected_position.segment_base_offset() > committed.segment_base_offset() {
                return Err(SegmentedLogReaderError::ReaderBeyondCommitted {
                    read_position: expected_position,
                    committed,
                });
            }

            let segment_base_offset = expected_position.segment_base_offset();
            let segment_byte_limit =
                if committed.segment_base_offset() == expected_position.segment_base_offset() {
                    committed.segment_byte_offset()
                } else {
                    self.end_offset_of_sealed_segment().await?
                };
            match self
                .active_segment
                .read_next(segment_byte_limit)
                .await
                .map_err(|source| SegmentedLogReaderError::Segment {
                    segment_base_offset,
                    source,
                })? {
                SegmentRead::Frame(frame) => {
                    return self
                        .finish_frame(frame, expected_position, Some(committed))
                        .await;
                }
                SegmentRead::EndOfAvailableData => {
                    if segment_base_offset == committed.segment_base_offset() {
                        return Err(SegmentedLogReaderError::CommittedDataUnavailable {
                            read_position: expected_position,
                            committed,
                        });
                    }
                    if !self.advance_segment().await? {
                        return Err(SegmentedLogReaderError::CommittedDataUnavailable {
                            read_position: expected_position,
                            committed,
                        });
                    }
                }
                SegmentRead::IncompleteTail => {
                    return Err(SegmentedLogReaderError::CommittedDataUnavailable {
                        read_position: expected_position,
                        committed,
                    });
                }
            }
        }
    }

    async fn finish_frame(
        &mut self,
        frame: OwnedDecodedFrame,
        expected_position: Position,
        committed: Option<Position>,
    ) -> Result<SegmentedRead, SegmentedLogReaderError> {
        let actual = frame.record_id();
        let expected = expected_position.next_record_id();
        if actual < expected {
            let error = SegmentedLogReaderError::RecordIdRegression {
                segment_base_offset: expected_position.segment_base_offset(),
                segment_byte_offset: frame.segment_byte_offset(),
                expected,
                actual,
            };
            return Err(self
                .restore_after_position_error(expected_position, error)
                .await);
        }

        let position = self.active_segment.position();
        if let Some(committed) =
            committed.filter(|committed| position.next_record_id() > committed.next_record_id())
        {
            let error = SegmentedLogReaderError::ReaderBeyondCommitted {
                read_position: position,
                committed,
            };
            return Err(self
                .restore_after_position_error(expected_position, error)
                .await);
        }
        if actual > expected {
            warn!(
                segment_base_offset = expected_position.segment_base_offset(),
                segment_byte_offset = frame.segment_byte_offset(),
                expected_record_id = expected,
                actual_record_id = actual,
                missing_records = actual - expected,
                "Detected a gap in disk buffer record IDs."
            );
        }

        Ok(SegmentedRead::Frame { frame, position })
    }

    async fn restore_after_position_error(
        &mut self,
        position: Position,
        error: SegmentedLogReaderError,
    ) -> SegmentedLogReaderError {
        match self.active_segment.seek_to(position).await {
            Ok(()) => error,
            Err(source) => SegmentedLogReaderError::PositionRestore {
                segment_base_offset: position.segment_base_offset(),
                segment_byte_offset: position.segment_byte_offset(),
                read_error: error.to_string(),
                source,
            },
        }
    }

    async fn advance_segment(&mut self) -> Result<bool, SegmentedLogReaderError> {
        let current_position = self.active_segment.position();
        let current_base_offset = current_position.segment_base_offset();
        let expected_next_base_offset = current_position.next_record_id();
        let Some(next_base_offset) = self.next_segment_base_offset(current_base_offset).await?
        else {
            return Ok(false);
        };
        if next_base_offset < expected_next_base_offset {
            return Err(SegmentedLogReaderError::OverlappingSegment {
                expected_next_record_id: expected_next_base_offset,
                next_base_offset,
            });
        }
        if next_base_offset > expected_next_base_offset {
            warn!(
                expected_segment_base_offset = expected_next_base_offset,
                actual_segment_base_offset = next_base_offset,
                missing_records = next_base_offset - expected_next_base_offset,
                "Detected a record ID gap between disk buffer segments."
            );
        }

        let position = Position::at_segment_start(next_base_offset);
        let next_segment = Self::try_open_at(&self.directory, position, self.max_frame_len)
            .await?
            .ok_or(SegmentedLogReaderError::MissingSegment {
                segment_base_offset: next_base_offset,
            })?;

        self.active_segment = next_segment;
        self.sealed_segment_end_offset = None;
        Ok(true)
    }

    /// Returns the stable physical end offset of the active sealed segment.
    async fn end_offset_of_sealed_segment(&mut self) -> Result<u64, SegmentedLogReaderError> {
        if let Some(end_offset) = self.sealed_segment_end_offset {
            return Ok(end_offset);
        }

        let segment_base_offset = self.active_segment.position().segment_base_offset();
        let end_offset = self.active_segment.end_offset().await.map_err(|source| {
            SegmentedLogReaderError::Segment {
                segment_base_offset,
                source,
            }
        })?;
        self.sealed_segment_end_offset = Some(end_offset);
        Ok(end_offset)
    }

    /// Opens the requested segment, or the segment with the smallest greater
    /// record base offset when the requested file is missing.
    async fn try_open_at(
        directory: &Path,
        position: Position,
        max_frame_len: usize,
    ) -> Result<Option<ReadableSegment<File>>, SegmentedLogReaderError> {
        let requested_base_offset = position.segment_base_offset();
        let requested_path = directory.join(segment_file_name(requested_base_offset));
        let (file, position) = match File::open(requested_path).await {
            Ok(file) => (file, position),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                let Some(next_base_offset) =
                    Self::next_segment_base_offset_in(directory, requested_base_offset).await?
                else {
                    return Ok(None);
                };
                let expected_next_record_id = position.next_record_id();
                if next_base_offset < expected_next_record_id {
                    return Err(SegmentedLogReaderError::OverlappingSegment {
                        expected_next_record_id,
                        next_base_offset,
                    });
                }
                if next_base_offset > expected_next_record_id {
                    warn!(
                        expected_segment_base_offset = expected_next_record_id,
                        actual_segment_base_offset = next_base_offset,
                        missing_records = next_base_offset - expected_next_record_id,
                        "Detected a record ID gap between disk buffer segments."
                    );
                }
                let path = directory.join(segment_file_name(next_base_offset));
                let file =
                    File::open(path)
                        .await
                        .map_err(|source| SegmentedLogReaderError::Open {
                            segment_base_offset: next_base_offset,
                            source,
                        })?;
                (file, Position::at_segment_start(next_base_offset))
            }
            Err(source) => {
                return Err(SegmentedLogReaderError::Open {
                    segment_base_offset: requested_base_offset,
                    source,
                });
            }
        };
        let segment_base_offset = position.segment_base_offset();
        let mut segment = ReadableSegment::new(file, position, max_frame_len)
            .await
            .map_err(|source| SegmentedLogReaderError::Segment {
                segment_base_offset,
                source,
            })?;
        let segment_byte_limit =
            segment
                .end_offset()
                .await
                .map_err(|source| SegmentedLogReaderError::Segment {
                    segment_base_offset,
                    source,
                })?;

        if let Some(actual) = segment
            .peek_next_record_id(segment_byte_limit)
            .await
            .map_err(|source| SegmentedLogReaderError::Segment {
                segment_base_offset,
                source,
            })?
        {
            if position.segment_byte_offset() == 0 && actual != segment_base_offset {
                return Err(SegmentedLogReaderError::SegmentBaseOffsetMismatch {
                    segment_base_offset,
                    actual_first_record_id: actual,
                });
            }
            let expected = position.next_record_id();
            if actual < expected {
                return Err(SegmentedLogReaderError::RecordIdRegression {
                    segment_base_offset,
                    segment_byte_offset: position.segment_byte_offset(),
                    expected,
                    actual,
                });
            }
        }

        Ok(Some(segment))
    }

    async fn next_segment_base_offset(
        &self,
        after: u64,
    ) -> Result<Option<u64>, SegmentedLogReaderError> {
        Self::next_segment_base_offset_in(&self.directory, after).await
    }

    async fn next_segment_base_offset_in(
        directory: &Path,
        after: u64,
    ) -> Result<Option<u64>, SegmentedLogReaderError> {
        let base_offsets = list_segment_base_offsets(directory)
            .await
            .map_err(|source| SegmentedLogReaderError::SegmentFiles { source })?;
        Ok(base_offsets
            .range((Excluded(after), Unbounded))
            .next()
            .copied())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SegmentedRead {
    Frame {
        frame: OwnedDecodedFrame,
        position: Position,
    },
    CaughtUp,
}

#[derive(Debug, Snafu)]
pub(crate) enum SegmentedLogReaderError {
    #[snafu(display("failed to open segment {segment_base_offset}.log: {source}"))]
    Open {
        segment_base_offset: u64,
        source: io::Error,
    },

    #[snafu(display("segment {segment_base_offset}.log does not exist"))]
    MissingSegment { segment_base_offset: u64 },

    #[snafu(display("failed to inspect segment files: {source}"))]
    SegmentFiles { source: SegmentFileError },

    #[snafu(display("segment {segment_base_offset}.log could not be read: {source}"))]
    Segment {
        segment_base_offset: u64,
        source: ReadableSegmentError,
    },

    #[snafu(display(
        "record ID regressed in {segment_base_offset}.log at byte {segment_byte_offset}: expected at least {expected}, got {actual}"
    ))]
    RecordIdRegression {
        segment_base_offset: u64,
        segment_byte_offset: u64,
        expected: u64,
        actual: u64,
    },

    #[snafu(display(
        "segment {next_base_offset}.log overlaps a preceding segment whose next record ID is {expected_next_record_id}"
    ))]
    OverlappingSegment {
        expected_next_record_id: u64,
        next_base_offset: u64,
    },

    #[snafu(display(
        "segment {segment_base_offset}.log begins with record ID {actual_first_record_id}"
    ))]
    SegmentBaseOffsetMismatch {
        segment_base_offset: u64,
        actual_first_record_id: u64,
    },

    #[snafu(display(
        "segment {segment_base_offset}.log has an incomplete frame at byte {segment_byte_offset}"
    ))]
    IncompleteTail {
        segment_base_offset: u64,
        segment_byte_offset: u64,
    },

    #[snafu(display(
        "reader at {read_position:?} could not read data committed through {committed:?}"
    ))]
    CommittedDataUnavailable {
        read_position: Position,
        committed: Position,
    },

    #[snafu(display(
        "reader position {read_position:?} advanced beyond committed position {committed:?}"
    ))]
    ReaderBeyondCommitted {
        read_position: Position,
        committed: Position,
    },

    #[snafu(display(
        "failed to restore segment {segment_base_offset}.log to byte {segment_byte_offset} after {read_error}: {source}"
    ))]
    PositionRestore {
        segment_base_offset: u64,
        segment_byte_offset: u64,
        read_error: String,
        source: ReadableSegmentError,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, num::NonZeroU32};

    use bytes::Bytes;
    use tokio::fs;

    use super::*;
    use crate::variants::disk_v3::frame::encode_frame;

    const MAX_FRAME_LEN: usize = 1024;

    fn frame(record_id: u64, event_count: u32, payload: &'static [u8]) -> Bytes {
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

    async fn write_segment(directory: &std::path::Path, start: u64, bytes: &Bytes) {
        fs::write(directory.join(segment_file_name(start)), bytes)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn lists_unique_segment_base_offsets_and_selects_the_closest_greater_offset() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-segment-list").unwrap();
        for base_offset in [90, 10, 50] {
            write_segment(directory.path(), base_offset, &Bytes::new()).await;
        }
        fs::write(directory.path().join("checkpoint.db"), b"ignored")
            .await
            .unwrap();

        assert_eq!(
            list_segment_base_offsets(directory.path()).await.unwrap(),
            BTreeSet::from([10, 50, 90])
        );
        assert_eq!(
            SegmentedLogReader::next_segment_base_offset_in(directory.path(), 10)
                .await
                .unwrap(),
            Some(50)
        );
        assert_eq!(
            SegmentedLogReader::next_segment_base_offset_in(directory.path(), 89)
                .await
                .unwrap(),
            Some(90)
        );
        assert_eq!(
            SegmentedLogReader::next_segment_base_offset_in(directory.path(), 90)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn reads_across_segments_and_returns_next_frame_positions() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-reader").unwrap();
        let first = frame(10, 2, b"first");
        let second = frame(12, 1, b"second");
        write_segment(directory.path(), 10, &first).await;
        write_segment(directory.path(), 12, &second).await;

        let initial = Position::at_segment_start(10);
        let committed = Position::new(12, u64::try_from(second.len()).unwrap(), 13);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();
        assert_eq!(reader.sealed_segment_end_offset, None);

        let SegmentedRead::Frame {
            frame,
            position: first_position,
        } = reader.read_next(committed).await.unwrap()
        else {
            panic!("expected first frame");
        };
        assert_eq!(frame.record_id(), 10);
        assert_eq!(
            first_position,
            Position::new(10, u64::try_from(first.len()).unwrap(), 12)
        );
        assert_eq!(reader.read_position(), first_position);
        assert_eq!(
            reader.sealed_segment_end_offset,
            Some(u64::try_from(first.len()).unwrap())
        );

        let SegmentedRead::Frame {
            frame,
            position: second_position,
        } = reader.read_next(committed).await.unwrap()
        else {
            panic!("expected second frame");
        };
        assert_eq!(frame.record_id(), 12);
        assert_eq!(
            second_position,
            Position::new(12, u64::try_from(second.len()).unwrap(), 13)
        );
        assert_eq!(reader.read_position(), second_position);
        assert_eq!(reader.sealed_segment_end_offset, None);
        assert_eq!(
            reader.read_next(committed).await.unwrap(),
            SegmentedRead::CaughtUp
        );
    }

    #[tokio::test]
    async fn committed_position_controls_read_availability() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-committed").unwrap();
        write_segment(directory.path(), 10, &Bytes::new()).await;
        let initial = Position::at_segment_start(10);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();

        assert_eq!(
            reader.read_next(initial).await.unwrap(),
            SegmentedRead::CaughtUp
        );

        let committed = Position::new(10, 1, 11);
        assert!(matches!(
            reader.read_next(committed).await,
            Err(SegmentedLogReaderError::CommittedDataUnavailable {
                read_position,
                committed: actual,
            }) if read_position == initial && actual == committed
        ));
        assert_eq!(reader.read_position(), initial);
    }

    #[tokio::test]
    async fn missing_committed_bytes_are_not_skipped_when_a_later_segment_exists() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-missing-committed").unwrap();
        write_segment(directory.path(), 10, &Bytes::new()).await;
        write_segment(directory.path(), 12, &Bytes::new()).await;
        let initial = Position::at_segment_start(10);
        let committed = Position::new(10, 100, 12);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();

        assert!(matches!(
            reader.read_next(committed).await,
            Err(SegmentedLogReaderError::CommittedDataUnavailable {
                read_position,
                committed: actual,
            }) if read_position == initial && actual == committed
        ));
        assert_eq!(reader.read_position(), initial);
    }

    #[tokio::test]
    async fn rejects_a_reader_logically_beyond_committed() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-beyond-committed").unwrap();
        write_segment(directory.path(), 10, &Bytes::new()).await;
        let initial = Position::at_segment_start(10);
        let committed = Position::at_segment_start(9);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();

        assert!(matches!(
            reader.read_next(committed).await,
            Err(SegmentedLogReaderError::ReaderBeyondCommitted {
                read_position,
                committed: actual,
            }) if read_position == initial && actual == committed
        ));
        assert_eq!(reader.read_position(), initial);
    }

    #[tokio::test]
    async fn frame_beyond_committed_restores_the_read_position() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-frame-beyond").unwrap();
        let frame = frame(10, 2, b"crosses-watermark");
        write_segment(directory.path(), 10, &frame).await;
        let initial = Position::at_segment_start(10);
        let committed = Position::new(10, u64::try_from(frame.len()).unwrap(), 11);
        let frame_end = Position::new(10, u64::try_from(frame.len()).unwrap(), 12);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();

        for _ in 0..2 {
            assert!(matches!(
                reader.read_next(committed).await,
                Err(SegmentedLogReaderError::ReaderBeyondCommitted {
                    read_position,
                    committed: actual,
                }) if read_position == frame_end && actual == committed
            ));
            assert_eq!(reader.read_position(), initial);
        }
    }

    #[tokio::test]
    async fn restarts_from_a_position_at_the_end_of_a_segment() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-restart").unwrap();
        let first = frame(10, 2, b"first");
        let second = frame(12, 1, b"second");
        let first_len = u64::try_from(first.len()).unwrap();
        write_segment(directory.path(), 10, &first).await;
        write_segment(directory.path(), 12, &second).await;

        let position = Position::new(10, first_len, 12);
        let committed = Position::new(12, u64::try_from(second.len()).unwrap(), 13);
        let mut reader = SegmentedLogReader::open(directory.path(), position, MAX_FRAME_LEN)
            .await
            .unwrap();
        let SegmentedRead::Frame { frame, .. } = reader.read_next(committed).await.unwrap() else {
            panic!("expected frame from the next segment");
        };
        assert_eq!(frame.record_id(), 12);
    }

    #[tokio::test]
    async fn rejects_a_position_that_would_move_record_ids_backwards() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-position").unwrap();
        let first = frame(10, 1, b"first");
        write_segment(directory.path(), 10, &first).await;

        assert!(matches!(
            SegmentedLogReader::open(directory.path(), Position::new(10, 0, 11), MAX_FRAME_LEN,)
                .await,
            Err(SegmentedLogReaderError::RecordIdRegression {
                segment_base_offset: 10,
                segment_byte_offset: 0,
                expected: 11,
                actual: 10,
            })
        ));
    }

    #[tokio::test]
    async fn rejects_a_segment_whose_first_record_does_not_match_its_file_name() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-segment-base").unwrap();
        let first = frame(11, 1, b"wrong-base");
        write_segment(directory.path(), 10, &first).await;

        assert!(matches!(
            SegmentedLogReader::open(
                directory.path(),
                Position::at_segment_start(10),
                MAX_FRAME_LEN,
            )
            .await,
            Err(SegmentedLogReaderError::SegmentBaseOffsetMismatch {
                segment_base_offset: 10,
                actual_first_record_id: 11,
            })
        ));
    }

    #[tokio::test]
    async fn allows_record_id_gaps() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-record-gap").unwrap();
        let first = frame(10, 1, b"first");
        let after_gap = frame(12, 1, b"first-after-corruption");
        let mut bytes = Vec::from(first.as_ref());
        bytes.extend_from_slice(&after_gap);
        let bytes = Bytes::from(bytes);
        write_segment(directory.path(), 10, &bytes).await;

        let initial = Position::at_segment_start(10);
        let committed = Position::new(10, u64::try_from(bytes.len()).unwrap(), 13);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();
        assert!(matches!(
            reader.read_next(committed).await.unwrap(),
            SegmentedRead::Frame { frame, .. } if frame.record_id() == 10
        ));
        let SegmentedRead::Frame { frame, position } = reader.read_next(committed).await.unwrap()
        else {
            panic!("expected frame after record ID gap");
        };
        assert_eq!(frame.record_id(), 12);
        assert_eq!(position.next_record_id(), 13);
        assert_eq!(reader.read_position(), position);
    }

    #[tokio::test]
    async fn skips_a_missing_segment_and_reads_the_next_higher_offset() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-segment-gap").unwrap();
        let first = frame(10, 1, b"first");
        let after_gap = frame(15, 1, b"after-gap");
        write_segment(directory.path(), 10, &first).await;
        write_segment(directory.path(), 15, &after_gap).await;

        let initial = Position::at_segment_start(10);
        let committed = Position::new(15, u64::try_from(after_gap.len()).unwrap(), 16);
        let mut reader = SegmentedLogReader::open(directory.path(), initial, MAX_FRAME_LEN)
            .await
            .unwrap();
        assert!(matches!(
            reader.read_next(committed).await.unwrap(),
            SegmentedRead::Frame { frame, .. } if frame.record_id() == 10
        ));

        let SegmentedRead::Frame { frame, position } = reader.read_next(committed).await.unwrap()
        else {
            panic!("expected frame after missing segment");
        };
        assert_eq!(frame.record_id(), 15);
        assert_eq!(position.segment_base_offset(), 15);
        assert_eq!(position.next_record_id(), 16);
    }
}
