use std::path::PathBuf;

use snafu::Snafu;

use super::{
    acknowledgement::{AcknowledgementError, AcknowledgementToken, AcknowledgementTracker},
    frame::PreparedFrame,
    logical_capacity::{
        LogicalCapacity, LogicalCapacityError, LogicalCapacityReservation, calculate_logical_bytes,
    },
    position::Position,
    readable_segment::OwnedDecodedFrame,
    segmented_log_reader::{SegmentedLogReader, SegmentedLogReaderError, SegmentedRead},
    segmented_log_writer::{
        FilesystemSegmentStorage, SegmentedLogWriter, SegmentedLogWriterConfig,
        SegmentedLogWriterError,
    },
};

/// High-level owner of one persistent queue's reader, writer, and accounting.
pub(crate) struct DiskBuffer {
    writer: SegmentedLogWriter<FilesystemSegmentStorage>,
    reader: SegmentedLogReader,
    logical_capacity: LogicalCapacity,
    acknowledgements: AcknowledgementTracker,
}

/// Result of reading from the high-level buffer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DiskBufferRead {
    Frame {
        frame: OwnedDecodedFrame,
        acknowledgement: AcknowledgementToken,
    },
    EndOfAvailableData,
    IncompleteTail,
}

impl DiskBuffer {
    /// Opens or creates a persistent queue.
    ///
    /// `reclaimable` is the durable consumer position from which reading should
    /// resume. Initialization counts only complete bytes between that position
    /// and the recovered writer position.
    pub(crate) async fn open(
        directory: impl Into<PathBuf>,
        reclaimable: Position,
        max_frame_len: usize,
        max_logical_bytes: u64,
        writer_config: SegmentedLogWriterConfig,
    ) -> Result<Self, DiskBufferError> {
        let directory = directory.into();
        let writer = SegmentedLogWriter::open_or_create(
            FilesystemSegmentStorage::new(&directory),
            reclaimable.next_record_id(),
            max_frame_len,
            writer_config,
        )
        .await
        .map_err(|source| DiskBufferError::Writer { source })?;
        let reader = SegmentedLogReader::open(&directory, reclaimable, max_frame_len)
            .await
            .map_err(|source| DiskBufferError::Reader { source })?;
        let logical_bytes =
            calculate_logical_bytes(&directory, reader.read_position(), writer.committed())
                .await
                .map_err(|source| DiskBufferError::Capacity { source })?;
        let logical_capacity = LogicalCapacity::new(max_logical_bytes, logical_bytes)
            .map_err(|source| DiskBufferError::Capacity { source })?;
        let initial_reader_position = reader.read_position();

        Ok(Self {
            writer,
            reader,
            logical_capacity,
            acknowledgements: AcknowledgementTracker::new(initial_reader_position),
        })
    }

    /// Appends one complete encoded frame with its previously acquired capacity.
    pub(crate) async fn append(
        &mut self,
        frame: PreparedFrame,
        reservation: LogicalCapacityReservation,
    ) -> Result<(), DiskBufferError> {
        reservation
            .validate(&self.logical_capacity, frame.bytes().len())
            .map_err(|source| DiskBufferError::Capacity { source })?;

        self.writer
            .append(frame)
            .await
            .map(|_| ())
            .map_err(|source| DiskBufferError::Writer { source })?;
        reservation.commit();
        Ok(())
    }

    /// Publishes all frames currently buffered by the writer.
    pub(crate) async fn flush(&mut self) -> Result<Position, DiskBufferError> {
        self.writer
            .flush()
            .await
            .map_err(|source| DiskBufferError::Writer { source })
    }

    /// Publishes and synchronizes all frames currently buffered by the writer.
    pub(crate) async fn sync_all(&mut self) -> Result<Position, DiskBufferError> {
        self.writer
            .sync_all()
            .await
            .map_err(|source| DiskBufferError::Writer { source })
    }

    /// Returns the next available frame and its single-use acknowledgement token.
    pub(crate) async fn next(&mut self) -> Result<DiskBufferRead, DiskBufferError> {
        let read = self
            .reader
            .read_next()
            .await
            .map_err(|source| DiskBufferError::Reader { source })?;

        match read {
            SegmentedRead::Frame { frame, position } => {
                let acknowledgement = self
                    .acknowledgements
                    .track_read(position, frame.frame_len())
                    .map_err(|source| DiskBufferError::Acknowledgement { source })?;

                Ok(DiskBufferRead::Frame {
                    frame,
                    acknowledgement,
                })
            }
            SegmentedRead::EndOfAvailableData => Ok(DiskBufferRead::EndOfAvailableData),
            SegmentedRead::IncompleteTail => Ok(DiskBufferRead::IncompleteTail),
        }
    }

    /// Records one downstream-finalized frame in original read order.
    ///
    /// This advances only the observed acknowledgement. A later checkpoint
    /// operation must make this position durable before moving the reclaimable
    /// position and subtracting the uncheckpointed bytes from logical size.
    pub(crate) fn acknowledge(
        &mut self,
        acknowledgement: AcknowledgementToken,
    ) -> Result<Position, DiskBufferError> {
        self.acknowledgements
            .acknowledge(acknowledgement)
            .map_err(|source| DiskBufferError::Acknowledgement { source })
    }

    /// Returns admitted logical occupancy, including outstanding reservations.
    #[must_use]
    pub(crate) fn logical_bytes(&self) -> u64 {
        self.logical_capacity.occupied_bytes()
    }

    /// Returns a cloneable handle used to reserve logical bytes before append.
    #[must_use]
    pub(crate) fn capacity(&self) -> LogicalCapacity {
        self.logical_capacity.clone()
    }

    #[must_use]
    pub(crate) const fn committed_position(&self) -> Position {
        self.writer.committed()
    }

    #[must_use]
    pub(crate) const fn read_position(&self) -> Position {
        self.reader.read_position()
    }

    /// Largest contiguous position whose downstream finalizers have completed.
    #[must_use]
    pub(crate) const fn observed_acknowledged_position(&self) -> Position {
        self.acknowledgements.observed_position()
    }

    /// Durable acknowledged position through which capacity may be reused.
    #[must_use]
    pub(crate) const fn reclaimable_position(&self) -> Position {
        self.acknowledgements.reclaimable_position()
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum DiskBufferError {
    #[snafu(display("persistent queue writer failed: {source}"))]
    Writer { source: SegmentedLogWriterError },

    #[snafu(display("persistent queue reader failed: {source}"))]
    Reader { source: SegmentedLogReaderError },

    #[snafu(display("persistent queue logical-capacity operation failed: {source}"))]
    Capacity { source: LogicalCapacityError },

    #[snafu(display("persistent queue acknowledgement failed: {source}"))]
    Acknowledgement { source: AcknowledgementError },
}
