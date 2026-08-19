use std::{num::NonZeroU32, path::PathBuf, sync::Arc};

use bytes::Bytes;
use snafu::Snafu;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;
use vector_common::{
    finalization::{BatchNotifier, EventFinalizerGroups, EventStatus},
    finalizer::OrderedFinalizer,
};

pub(crate) use super::writer_actor::{PublishedWriterState, WriterStatus};

use super::{
    acknowledgement::{AcknowledgementError, AcknowledgementToken},
    checkpoint::{CheckpointDecodeError, CheckpointError, CheckpointLoad, ReaderCheckpoint},
    frame::{FRAME_HEADER_LEN, FrameEncodeError},
    logical_capacity::{LogicalCapacity, LogicalCapacityError, calculate_logical_bytes},
    position::Position,
    readable_segment::OwnedDecodedFrame,
    segmented_log_reader::{SegmentedLogReader, SegmentedLogReaderError, SegmentedRead},
    segmented_log_writer::{
        FilesystemSegmentStorage, SegmentedLogWriter, SegmentedLogWriterConfig,
        SegmentedLogWriterError,
    },
    writer_actor::{WriterActor, WriterActorConfig, WriterCommand},
};

pub(crate) const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;

/// Complete configuration needed to recover and start one disk buffer.
#[derive(Clone, Debug)]
pub(crate) struct DiskBufferConfig {
    pub(crate) directory: PathBuf,
    pub(crate) initial: Position,
    pub(crate) max_frame_len: usize,
    pub(crate) max_logical_bytes: u64,
    pub(crate) command_queue_capacity: usize,
    pub(crate) writer: SegmentedLogWriterConfig,
}

/// Payload prepared by a producer but not yet assigned a record ID.
///
/// Producers can determine its exact framed length and charge logical capacity
/// before enqueueing it. The writer actor assigns the record ID and
/// creates the final encoded frame in command receive order.
#[derive(Debug)]
pub(crate) struct PreparedRecord {
    payload: Bytes,
    event_count: NonZeroU32,
    codec_metadata: u32,
    ingress_finalizers: EventFinalizerGroups,
}

impl PreparedRecord {
    pub(crate) fn new(
        payload: impl Into<Bytes>,
        event_count: NonZeroU32,
        codec_metadata: u32,
        ingress_finalizers: EventFinalizerGroups,
    ) -> Self {
        Self {
            payload: payload.into(),
            event_count,
            codec_metadata,
            ingress_finalizers,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        payload: impl Into<Bytes>,
        event_count: NonZeroU32,
        codec_metadata: u32,
    ) -> Self {
        Self::new(
            payload,
            event_count,
            codec_metadata,
            EventFinalizerGroups::default(),
        )
    }

    fn framed_len(&self) -> Result<usize, DiskBufferError> {
        FRAME_HEADER_LEN
            .checked_add(self.payload.len())
            .ok_or(DiskBufferError::FrameLengthOverflow)
    }

    pub(super) const fn event_count(&self) -> NonZeroU32 {
        self.event_count
    }

    pub(super) const fn codec_metadata(&self) -> u32 {
        self.codec_metadata
    }

    pub(super) fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub(super) fn take_ingress_finalizers(&mut self) -> EventFinalizerGroups {
        std::mem::take(&mut self.ingress_finalizers)
    }

    pub(super) fn mark_errored(&self) {
        self.ingress_finalizers.update_status(EventStatus::Errored);
    }
}

/// Result of a blocking send attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SendOutcome {
    Accepted,
    /// The record can never fit this buffer and was intentionally dropped.
    DroppedUnwritable,
}

/// Result of a send attempt that does not wait for logical capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TrySendOutcome {
    Accepted,
    /// Logical capacity was unavailable and the prepared record was intentionally dropped.
    DroppedNewest,
    /// The record can never fit this buffer and was intentionally dropped.
    DroppedUnwritable,
}

/// High-level constructor for one actor-backed disk buffer.
pub(crate) struct DiskBuffer;

impl DiskBuffer {
    /// Recovers the log, starts its single writer actor, and returns its MPSC
    /// producer and exclusive consumer endpoints.
    pub(crate) async fn open(
        config: DiskBufferConfig,
    ) -> Result<(DiskBufferSender, DiskBufferReceiver), DiskBufferError> {
        if config.command_queue_capacity == 0 {
            return Err(DiskBufferError::ZeroCommandQueueCapacity);
        }
        if config.writer.sync_interval.is_zero() {
            return Err(DiskBufferError::ZeroSyncInterval);
        }

        tokio::fs::create_dir_all(&config.directory)
            .await
            .map_err(|source| DiskBufferError::CreateDirectory { source })?;
        let checkpoint = ReaderCheckpoint::open(&config.directory)
            .await
            .map_err(|source| DiskBufferError::Checkpoint { source })?;
        let checkpoint_position = load_checkpoint(&checkpoint)?;

        let writer = SegmentedLogWriter::open_or_create(
            FilesystemSegmentStorage::new(&config.directory),
            config.initial.next_record_id(),
            config.max_frame_len,
            config.writer,
        )
        .await
        .map_err(|source| DiskBufferError::Writer { source })?;
        let earliest_position = SegmentedLogReader::earliest_position(&config.directory)
            .await
            .map_err(|source| DiskBufferError::Reader { source })?
            .unwrap_or(config.initial);
        let reclaimable = checkpoint_position.unwrap_or(earliest_position);
        let reader = SegmentedLogReader::open(&config.directory, reclaimable, config.max_frame_len)
            .await
            .map_err(|source| DiskBufferError::Reader { source })?;
        let logical_bytes = calculate_logical_bytes(
            &config.directory,
            reader.read_position(),
            writer.committed(),
        )
        .await
        .map_err(|source| DiskBufferError::Capacity { source })?;
        let logical_capacity = LogicalCapacity::new(config.max_logical_bytes, logical_bytes)
            .map_err(|source| DiskBufferError::Capacity { source })?;

        let initial_state = PublishedWriterState {
            committed: writer.committed(),
            data_synced: writer.data_synced(),
            consumer_acknowledged: reader.read_position(),
            reclaimable: reader.read_position(),
            status: WriterStatus::Running,
        };
        let (state_tx, state_rx) = watch::channel(initial_state);
        let (command_tx, command_rx) = mpsc::channel(config.command_queue_capacity);
        let (finalizer, finalizations) = OrderedFinalizer::new(None);
        let actor = WriterActor::new(
            writer,
            checkpoint,
            logical_capacity.clone(),
            finalizations,
            command_rx,
            state_tx,
            WriterActorConfig {
                sync_interval: config.writer.sync_interval,
                max_frame_len: config.max_frame_len,
            },
        );
        crate::spawn_named(actor.run(), "disk-v3-writer");

        let sender = DiskBufferSender {
            commands: command_tx.clone(),
            state: state_rx.clone(),
            capacity: logical_capacity,
            max_frame_len: config.max_frame_len,
            max_segment_len: config.writer.segment_size,
        };
        let receiver = DiskBufferReceiver {
            reader,
            state: state_rx,
            finalizer,
        };

        Ok((sender, receiver))
    }
}

fn load_checkpoint(checkpoint: &ReaderCheckpoint) -> Result<Option<Position>, DiskBufferError> {
    match checkpoint.load() {
        Ok(CheckpointLoad::Uninitialized) => Ok(None),
        Ok(CheckpointLoad::Position(position)) => Ok(Some(position)),
        Err(
            source @ (CheckpointDecodeError::InvalidMagic { .. }
            | CheckpointDecodeError::ChecksumMismatch { .. }),
        ) => {
            warn!(
                error = %source,
                "Ignoring an invalid disk buffer reader checkpoint and replaying from the earliest retained segment."
            );
            Ok(None)
        }
        Err(source @ CheckpointDecodeError::UnsupportedVersion { .. }) => {
            Err(DiskBufferError::DecodeCheckpoint { source })
        }
    }
}

/// Cloneable MPSC producer endpoint for one disk buffer.
#[derive(Clone, Debug)]
pub(crate) struct DiskBufferSender {
    commands: mpsc::Sender<WriterCommand>,
    state: watch::Receiver<PublishedWriterState>,
    capacity: LogicalCapacity,
    max_frame_len: usize,
    max_segment_len: u64,
}

impl DiskBufferSender {
    /// Waits for logical and command-queue capacity, then transfers ownership
    /// of the record to the writer actor.
    pub(crate) async fn send(
        &self,
        record: PreparedRecord,
    ) -> Result<SendOutcome, DiskBufferError> {
        let Some(frame_len) = self.admissible_frame_len(&record)? else {
            return Ok(SendOutcome::DroppedUnwritable);
        };
        let outcome = self
            .enqueue_append_while_running(record, frame_len, WhenLogicalFull::Block)
            .await?;
        debug_assert_eq!(outcome, EnqueueOutcome::Accepted);
        Ok(SendOutcome::Accepted)
    }

    /// Waits for command-queue space, then attempts a send without waiting for
    /// logical capacity. A full logical buffer intentionally drops the
    /// prepared record.
    pub(crate) async fn try_send(
        &self,
        record: PreparedRecord,
    ) -> Result<TrySendOutcome, DiskBufferError> {
        let Some(frame_len) = self.admissible_frame_len(&record)? else {
            return Ok(TrySendOutcome::DroppedUnwritable);
        };
        match self
            .enqueue_append_while_running(record, frame_len, WhenLogicalFull::DropNewest)
            .await?
        {
            EnqueueOutcome::Accepted => Ok(TrySendOutcome::Accepted),
            EnqueueOutcome::Full => Ok(TrySendOutcome::DroppedNewest),
        }
    }

    /// Makes every record sent before this call available to the receiver.
    pub(crate) async fn flush(&self) -> Result<Position, DiskBufferError> {
        self.request_position(WriterRequest::Flush).await
    }

    /// Makes every record sent before this call durable and available to the receiver.
    pub(crate) async fn sync(&self) -> Result<Position, DiskBufferError> {
        self.request_position(WriterRequest::Sync).await
    }

    /// Gracefully publishes and synchronizes queued data, then stops the actor.
    ///
    /// All producer clones must be quiesced and all emitted frames acknowledged
    /// before shutdown. Commands submitted behind shutdown are failed.
    pub(crate) async fn shutdown(&self) -> Result<Position, DiskBufferError> {
        self.request_position(WriterRequest::Shutdown).await
    }

    #[must_use]
    pub(crate) fn logical_bytes(&self) -> u64 {
        self.capacity.occupied_bytes()
    }

    #[must_use]
    pub(crate) fn state(&self) -> PublishedWriterState {
        self.state.borrow().clone()
    }

    async fn request_position(&self, request: WriterRequest) -> Result<Position, DiskBufferError> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        let command = match request {
            WriterRequest::Flush => WriterCommand::Flush { reply },
            WriterRequest::Sync => WriterCommand::Sync { reply },
            WriterRequest::Shutdown => WriterCommand::Shutdown { reply },
        };
        self.commands
            .send(command)
            .await
            .map_err(|_| self.actor_unavailable())?;
        response.await.map_err(|_| self.actor_unavailable())?
    }

    async fn enqueue_append_while_running(
        &self,
        record: PreparedRecord,
        frame_len: usize,
        when_full: WhenLogicalFull,
    ) -> Result<EnqueueOutcome, DiskBufferError> {
        let mut state = self.state.clone();
        loop {
            Self::ensure_status_running(&state.borrow().status)?;
            if !self.capacity.is_below_high_water() {
                if when_full == WhenLogicalFull::DropNewest {
                    return Ok(EnqueueOutcome::Full);
                }
                // Every capacity release is followed by a writer-state
                // publication, so this retained watch update is also the capacity
                // wakeup and cannot be lost between the check and await.
                state
                    .changed()
                    .await
                    .map_err(|_| self.actor_unavailable())?;
                continue;
            }

            let permit = tokio::select! {
                biased;
                permit = self.commands.reserve() => {
                    permit.map_err(|_| self.actor_unavailable())?
                }
                changed = state.changed() => {
                    changed.map_err(|_| self.actor_unavailable())?;
                    continue;
                }
            };

            Self::ensure_status_running(&state.borrow().status)?;
            match self.capacity.try_acquire(frame_len) {
                Ok(()) => {
                    // Filling a reserved channel slot is synchronous, so there
                    // is no cancellation point after capacity is charged.
                    permit.send(WriterCommand::Append { record });
                    return Ok(EnqueueOutcome::Accepted);
                }
                // Another producer crossed the high-water mark first. Dropping
                // the permit returns the unused command-channel slot.
                Err(LogicalCapacityError::Full) => {
                    if when_full == WhenLogicalFull::DropNewest {
                        return Ok(EnqueueOutcome::Full);
                    }
                }
                Err(source) => return Err(DiskBufferError::Capacity { source }),
            }
        }
    }

    fn admissible_frame_len(
        &self,
        record: &PreparedRecord,
    ) -> Result<Option<usize>, DiskBufferError> {
        let frame_len = record.framed_len()?;
        let frame_len_u64 =
            u64::try_from(frame_len).map_err(|_| DiskBufferError::FrameLengthOverflow)?;
        if frame_len > self.max_frame_len || frame_len_u64 > self.max_segment_len {
            return Ok(None);
        }
        Ok(Some(frame_len))
    }

    fn ensure_running(&self) -> Result<(), DiskBufferError> {
        Self::ensure_status_running(&self.state.borrow().status)
    }

    fn ensure_status_running(status: &WriterStatus) -> Result<(), DiskBufferError> {
        match status {
            WriterStatus::Running => Ok(()),
            WriterStatus::Closed => Err(DiskBufferError::ActorClosed),
            WriterStatus::Failed { reason } => Err(DiskBufferError::ActorFailed {
                reason: Arc::clone(reason),
            }),
        }
    }

    fn actor_unavailable(&self) -> DiskBufferError {
        match &self.state.borrow().status {
            WriterStatus::Running | WriterStatus::Closed => DiskBufferError::ActorClosed,
            WriterStatus::Failed { reason } => DiskBufferError::ActorFailed {
                reason: Arc::clone(reason),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WhenLogicalFull {
    Block,
    DropNewest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnqueueOutcome {
    Accepted,
    Full,
}

enum WriterRequest {
    Flush,
    Sync,
    Shutdown,
}

/// Exclusive consumer endpoint for one disk buffer.
pub(crate) struct DiskBufferReceiver {
    reader: SegmentedLogReader,
    state: watch::Receiver<PublishedWriterState>,
    finalizer: OrderedFinalizer<AcknowledgementToken>,
}

impl DiskBufferReceiver {
    /// Waits for the next committed frame.
    ///
    /// The returned batch notifier must be attached to every event decoded
    /// from the frame. Downstream completion then advances the reader
    /// checkpoint automatically in frame order. `None` is returned only after
    /// the actor has closed and every committed frame has been consumed.
    pub(crate) async fn next(&mut self) -> Result<Option<DiskBufferRead>, DiskBufferError> {
        loop {
            let writer_state = self.state.borrow().clone();
            let read_position = self.reader.read_position();
            if read_position.next_record_id() < writer_state.committed.next_record_id() {
                let read = self
                    .reader
                    .read_next()
                    .await
                    .map_err(|source| DiskBufferError::Reader { source })?;
                let SegmentedRead::Frame { frame, position } = read else {
                    return Err(DiskBufferError::CommittedDataUnavailable {
                        read_position,
                        committed: writer_state.committed,
                    });
                };
                let acknowledgement = AcknowledgementToken::new(position, frame.frame_len())
                    .map_err(|source| DiskBufferError::Acknowledgement { source })?;
                let (batch_notifier, finalization) = BatchNotifier::new_with_receiver();
                self.finalizer.add(acknowledgement, finalization);
                return Ok(Some(DiskBufferRead {
                    frame,
                    batch_notifier,
                }));
            }
            if read_position.next_record_id() > writer_state.committed.next_record_id() {
                return Err(DiskBufferError::ReaderBeyondCommitted {
                    read_position,
                    committed: writer_state.committed,
                });
            }

            match writer_state.status {
                WriterStatus::Running => self
                    .state
                    .changed()
                    .await
                    .map_err(|_| DiskBufferError::ActorClosed)?,
                WriterStatus::Closed => return Ok(None),
                WriterStatus::Failed { reason } => {
                    return Err(DiskBufferError::ActorFailed { reason });
                }
            }
        }
    }

    #[must_use]
    pub(crate) const fn read_position(&self) -> Position {
        self.reader.read_position()
    }

    #[must_use]
    pub(crate) fn state(&self) -> PublishedWriterState {
        self.state.borrow().clone()
    }

    fn actor_unavailable(&self) -> DiskBufferError {
        match &self.state.borrow().status {
            WriterStatus::Running | WriterStatus::Closed => DiskBufferError::ActorClosed,
            WriterStatus::Failed { reason } => DiskBufferError::ActorFailed {
                reason: Arc::clone(reason),
            },
        }
    }
}

/// One decoded frame and the notifier that tracks its downstream finalization.
#[derive(Debug)]
pub(crate) struct DiskBufferRead {
    pub(crate) frame: OwnedDecodedFrame,
    /// Attach this notifier to every event decoded from `frame` before
    /// transferring those events downstream.
    pub(crate) batch_notifier: BatchNotifier,
}

#[derive(Debug, Snafu)]
pub(crate) enum DiskBufferError {
    #[snafu(display("failed to create the persistent queue directory: {source}"))]
    CreateDirectory { source: std::io::Error },

    #[snafu(display("persistent queue writer failed: {source}"))]
    Writer { source: SegmentedLogWriterError },

    #[snafu(display("persistent queue reader failed: {source}"))]
    Reader { source: SegmentedLogReaderError },

    #[snafu(display("persistent queue frame encoding failed: {source}"))]
    Frame { source: FrameEncodeError },

    #[snafu(display("persistent queue logical-capacity operation failed: {source}"))]
    Capacity { source: LogicalCapacityError },

    #[snafu(display("persistent queue acknowledgement failed: {source}"))]
    Acknowledgement { source: AcknowledgementError },

    #[snafu(display("persistent queue checkpoint failed: {source}"))]
    Checkpoint { source: CheckpointError },

    #[snafu(display("persistent queue reader checkpoint is incompatible: {source}"))]
    DecodeCheckpoint { source: CheckpointDecodeError },

    #[snafu(display("disk buffer command queue capacity must be greater than zero"))]
    ZeroCommandQueueCapacity,

    #[snafu(display("disk buffer synchronization interval must be greater than zero"))]
    ZeroSyncInterval,

    #[snafu(display("disk buffer frame length overflowed usize"))]
    FrameLengthOverflow,

    #[snafu(display("disk buffer writer actor has closed"))]
    ActorClosed,

    #[snafu(display("disk buffer writer actor failed: {reason}"))]
    ActorFailed { reason: Arc<str> },

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
}
