use std::path::PathBuf;

use snafu::Snafu;

use super::{
    frame::PreparedFrame,
    logical_capacity::{
        LogicalCapacity, LogicalCapacityError, LogicalCapacityReservation, calculate_logical_bytes,
    },
    position::Position,
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

        Ok(Self {
            writer,
            reader,
            logical_capacity,
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

    /// Returns the next available frame without acknowledging it.
    pub(crate) async fn next(&mut self) -> Result<SegmentedRead, DiskBufferError> {
        self.reader
            .read_next()
            .await
            .map_err(|source| DiskBufferError::Reader { source })
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
}

#[derive(Debug, Snafu)]
pub(crate) enum DiskBufferError {
    #[snafu(display("persistent queue writer failed: {source}"))]
    Writer { source: SegmentedLogWriterError },

    #[snafu(display("persistent queue reader failed: {source}"))]
    Reader { source: SegmentedLogReaderError },

    #[snafu(display("persistent queue logical-capacity operation failed: {source}"))]
    Capacity { source: LogicalCapacityError },
}
