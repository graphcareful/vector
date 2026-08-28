use std::{any::type_name, marker::PhantomData, num::NonZeroU32, path::PathBuf, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use snafu::Snafu;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;
use vector_common::{
    finalization::{BatchNotifier, EventFinalizerGroups, EventStatus},
    finalizer::OrderedFinalizer,
};

use crate::{Bufferable, encoding::AsMetadata, finalization::FinalizerGuard};

pub(crate) use super::writer_actor::{PublishedWriterState, WriterStatus};

use super::{
    checkpoint::{CheckpointDecodeError, CheckpointError, CheckpointLoad, ReaderCheckpoint},
    frame::{FRAME_HEADER_LEN, FrameEncodeError},
    logical_capacity::{LogicalCapacity, LogicalCapacityError, calculate_logical_bytes},
    position::Position,
    read_progress::{ReadProgressError, ReadProgressToken},
    readable_segment::OwnedDecodedFrame,
    reclaimer::SegmentReclaimer,
    segmented_log_reader::{SegmentedLogReader, SegmentedLogReaderError, SegmentedRead},
    segmented_log_writer::{
        FilesystemSegmentStorage, SegmentedLogWriter, SegmentedLogWriterConfig,
        SegmentedLogWriterError,
    },
    writer_actor::{WriterActor, WriterActorChannels, WriterActorConfig, WriterCommand},
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

    fn restore_ingress_finalizers(&mut self, finalizers: EventFinalizerGroups) {
        debug_assert!(self.ingress_finalizers.is_empty());
        self.ingress_finalizers = finalizers;
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
    pub(crate) async fn open<T>(
        config: DiskBufferConfig,
    ) -> Result<(DiskBufferSender<T>, DiskBufferReceiver<T>), DiskBufferError>
    where
        T: Bufferable,
    {
        Self::open_inner(config).await
    }

    #[cfg(test)]
    pub(super) async fn open_raw(
        config: DiskBufferConfig,
    ) -> Result<
        (
            DiskBufferSender<PreparedRecord>,
            DiskBufferReceiver<PreparedRecord>,
        ),
        DiskBufferError,
    > {
        Self::open_inner(config).await
    }

    async fn open_inner<T>(
        config: DiskBufferConfig,
    ) -> Result<(DiskBufferSender<T>, DiskBufferReceiver<T>), DiskBufferError> {
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
        let (reclaimable_segment_tx, reclaimable_segment_rx) =
            watch::channel(reclaimable.segment_base_offset());
        let (command_tx, command_rx) = mpsc::channel(config.command_queue_capacity);
        let (finalizer, finalizations) = OrderedFinalizer::new(None);
        let actor = WriterActor::new(
            writer,
            checkpoint,
            logical_capacity.clone(),
            finalizations,
            WriterActorChannels {
                reclaimable_segment: reclaimable_segment_tx,
                commands: command_rx,
                state: state_tx,
            },
            WriterActorConfig {
                sync_interval: config.writer.sync_interval,
                max_frame_len: config.max_frame_len,
            },
        );
        let reclaimer =
            SegmentReclaimer::new(Arc::new(config.directory.clone()), reclaimable_segment_rx);
        crate::spawn_named(actor.run(), "disk-v3-writer");
        crate::spawn_named(reclaimer.run(), "disk-v3-reclaimer");

        let sender = DiskBufferSender {
            commands: command_tx.clone(),
            state: state_rx.clone(),
            capacity: logical_capacity,
            max_frame_len: config.max_frame_len,
            max_segment_len: config.writer.segment_size,
            _record: PhantomData,
        };
        let receiver = DiskBufferReceiver {
            reader,
            state: state_rx,
            finalizer,
            failed: false,
            _record: PhantomData,
        };

        Ok((sender, receiver))
    }
}

fn prepare_record<T>(
    mut record: T,
    max_frame_len: usize,
) -> Result<Option<PreparedRecord>, DiskBufferError>
where
    T: Bufferable,
{
    let event_count = record.event_count();
    let ingress_finalizers = FinalizerGuard::new(record.take_finalizer_groups());
    let Some(event_count_u32) = u32::try_from(event_count).ok().and_then(NonZeroU32::new) else {
        return Err(DiskBufferError::InvalidEventCount {
            record_type: type_name::<T>(),
            event_count,
        });
    };
    let codec_metadata = T::get_metadata().into_u32();
    let max_payload_len = max_frame_len.saturating_sub(FRAME_HEADER_LEN);
    let encoded_size = record.encoded_size();
    if encoded_size.is_some_and(|encoded_size| encoded_size > max_payload_len) {
        warn!(
            record_type = type_name::<T>(),
            ?encoded_size,
            max_payload_len,
            "Record cannot fit in a disk-v3 frame; dropping it."
        );
        ingress_finalizers.disarm();
        return Ok(None);
    }
    let initial_capacity = encoded_size.unwrap_or_default();
    let mut payload = BytesMut::with_capacity(initial_capacity);
    let encode_result = {
        let mut limited_payload = (&mut payload).limit(max_payload_len);
        record.encode(&mut limited_payload)
    };
    if let Err(source) = encode_result {
        warn!(
            record_type = type_name::<T>(),
            error = %source,
            "Record could not be encoded for disk-v3; dropping it."
        );
        ingress_finalizers.disarm();
        return Ok(None);
    }

    Ok(Some(PreparedRecord::new(
        payload.freeze(),
        event_count_u32,
        codec_metadata,
        ingress_finalizers.into_inner(),
    )))
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

/// Cloneable, typed MPSC producer endpoint for one disk buffer.
pub(crate) struct DiskBufferSender<T> {
    commands: mpsc::Sender<WriterCommand>,
    state: watch::Receiver<PublishedWriterState>,
    capacity: LogicalCapacity,
    max_frame_len: usize,
    max_segment_len: u64,
    _record: PhantomData<fn(T)>,
}

impl<T> Clone for DiskBufferSender<T> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            state: self.state.clone(),
            capacity: self.capacity.clone(),
            max_frame_len: self.max_frame_len,
            max_segment_len: self.max_segment_len,
            _record: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for DiskBufferSender<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiskBufferSender")
            .field("state", &self.state)
            .field("capacity", &self.capacity)
            .field("max_frame_len", &self.max_frame_len)
            .field("max_segment_len", &self.max_segment_len)
            .field("record_type", &type_name::<T>())
            .finish_non_exhaustive()
    }
}

impl<T> DiskBufferSender<T>
where
    T: Bufferable,
{
    /// Waits for logical capacity and transfers the encoded record to the
    /// single writer actor.
    pub(crate) async fn send(&self, record: T) -> Result<SendOutcome, DiskBufferError> {
        let Some(record) = prepare_record(record, self.max_frame_len)? else {
            return Ok(SendOutcome::DroppedUnwritable);
        };
        self.send_prepared_record(record).await
    }

    /// Attempts admission without waiting for logical capacity. A full buffer
    /// intentionally drops the record; disk-v3 does not support overflow stages.
    pub(crate) async fn try_send(&self, record: T) -> Result<TrySendOutcome, DiskBufferError> {
        let Some(record) = prepare_record(record, self.max_frame_len)? else {
            return Ok(TrySendOutcome::DroppedUnwritable);
        };
        self.try_send_prepared_record(record).await
    }
}

impl<T> DiskBufferSender<T> {
    #[cfg(test)]
    pub(crate) async fn send_prepared(
        &self,
        record: PreparedRecord,
    ) -> Result<SendOutcome, DiskBufferError> {
        self.send_prepared_record(record).await
    }

    #[cfg(test)]
    pub(crate) async fn try_send_prepared(
        &self,
        record: PreparedRecord,
    ) -> Result<TrySendOutcome, DiskBufferError> {
        self.try_send_prepared_record(record).await
    }

    /// Waits for logical and command-queue capacity, then transfers ownership
    /// of the record to the writer actor.
    async fn send_prepared_record(
        &self,
        mut record: PreparedRecord,
    ) -> Result<SendOutcome, DiskBufferError> {
        let finalizers = FinalizerGuard::new(record.take_ingress_finalizers());
        let Some(frame_len) = self.admissible_frame_len(&record)? else {
            finalizers.disarm();
            return Ok(SendOutcome::DroppedUnwritable);
        };
        let outcome = self
            .enqueue_append_while_running(record, finalizers, frame_len, WhenLogicalFull::Block)
            .await?;
        debug_assert_eq!(outcome, EnqueueOutcome::Accepted);
        Ok(SendOutcome::Accepted)
    }

    /// Waits for command-queue space, then attempts a send without waiting for
    /// logical capacity. A full logical buffer intentionally drops the
    /// prepared record.
    async fn try_send_prepared_record(
        &self,
        mut record: PreparedRecord,
    ) -> Result<TrySendOutcome, DiskBufferError> {
        let finalizers = FinalizerGuard::new(record.take_ingress_finalizers());
        let Some(frame_len) = self.admissible_frame_len(&record)? else {
            finalizers.disarm();
            return Ok(TrySendOutcome::DroppedUnwritable);
        };
        match self
            .enqueue_append_while_running(
                record,
                finalizers,
                frame_len,
                WhenLogicalFull::DropNewest,
            )
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
        mut record: PreparedRecord,
        finalizers: FinalizerGuard,
        frame_len: usize,
        when_full: WhenLogicalFull,
    ) -> Result<EnqueueOutcome, DiskBufferError> {
        let mut state = self.state.clone();
        loop {
            Self::ensure_status_running(&state.borrow().status)?;
            if !self.capacity.is_below_high_water() {
                if when_full == WhenLogicalFull::DropNewest {
                    finalizers.disarm();
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
                    record.restore_ingress_finalizers(finalizers.into_inner());
                    permit.send(WriterCommand::Append { record });
                    return Ok(EnqueueOutcome::Accepted);
                }
                // Another producer crossed the high-water mark first. Dropping
                // the permit returns the unused command-channel slot.
                Err(LogicalCapacityError::Full) => {
                    if when_full == WhenLogicalFull::DropNewest {
                        finalizers.disarm();
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

/// Exclusive, typed consumer endpoint for one disk buffer.
pub(crate) struct DiskBufferReceiver<T> {
    reader: SegmentedLogReader,
    state: watch::Receiver<PublishedWriterState>,
    finalizer: OrderedFinalizer<ReadProgressToken>,
    failed: bool,
    _record: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for DiskBufferReceiver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiskBufferReceiver")
            .field("record_type", &type_name::<T>())
            .field("read_position", &self.read_position())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<T> DiskBufferReceiver<T>
where
    T: Bufferable,
{
    /// Waits for and decodes the next committed record.
    ///
    /// Decode failure is terminal for this receiver. The failed frame is not
    /// registered as acknowledged, so its durable checkpoint cannot advance.
    pub(crate) async fn next(&mut self) -> Result<Option<T>, DiskBufferError> {
        if self.failed {
            return Err(DiskBufferError::TypedReceiverFailed {
                record_type: type_name::<T>(),
            });
        }

        let read = match self.read_next_committed().await {
            Ok(read) => read,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let Some(read) = read else {
            return Ok(None);
        };

        match decode_record::<T>(&read.frame) {
            Ok(mut record) => {
                let batch_notifier = self.register_acknowledgement(&read)?;
                record.add_batch_notifier(batch_notifier);
                Ok(Some(record))
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

impl<T> DiskBufferReceiver<T> {
    /// Waits for the next committed frame.
    ///
    /// The returned batch notifier must be attached to every event decoded
    /// from the frame. Downstream completion then advances the reader
    /// checkpoint automatically in frame order. `None` is returned only after
    /// the actor has closed and every committed frame has been consumed.
    #[cfg(test)]
    pub(crate) async fn next_frame(&mut self) -> Result<Option<DiskBufferRead>, DiskBufferError> {
        let Some(read) = self.read_next_committed().await? else {
            return Ok(None);
        };
        let batch_notifier = self.register_acknowledgement(&read)?;
        Ok(Some(DiskBufferRead {
            frame: read.frame,
            batch_notifier,
        }))
    }

    async fn read_next_committed(
        &mut self,
    ) -> Result<Option<PendingAcknowledgement>, DiskBufferError> {
        loop {
            let writer_state = self.state.borrow().clone();
            match self
                .reader
                .read_next(writer_state.committed)
                .await
                .map_err(|source| DiskBufferError::Reader { source })?
            {
                SegmentedRead::Frame { frame, position } => {
                    return Ok(Some(PendingAcknowledgement { frame, position }));
                }
                SegmentedRead::CaughtUp => {}
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

    fn register_acknowledgement(
        &self,
        read: &PendingAcknowledgement,
    ) -> Result<BatchNotifier, DiskBufferError> {
        let progress = ReadProgressToken::for_frame(read.position, read.frame.frame_len())
            .map_err(|source| DiskBufferError::ReadProgress { source })?;
        let (batch_notifier, finalization) = BatchNotifier::new_with_receiver();
        self.finalizer.add(progress, finalization);
        Ok(batch_notifier)
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

struct PendingAcknowledgement {
    frame: OwnedDecodedFrame,
    position: Position,
}

fn decode_record<T>(frame: &OwnedDecodedFrame) -> Result<T, DiskBufferError>
where
    T: Bufferable,
{
    let codec_metadata = frame.codec_metadata();
    let metadata =
        T::Metadata::from_u32(codec_metadata).ok_or(DiskBufferError::InvalidCodecMetadata {
            record_type: type_name::<T>(),
            codec_metadata,
        })?;
    if !T::can_decode(metadata) {
        return Err(DiskBufferError::UnsupportedCodecMetadata {
            record_type: type_name::<T>(),
            codec_metadata,
        });
    }

    let record = T::decode(metadata, frame.payload().clone()).map_err(|source| {
        DiskBufferError::DecodeRecord {
            record_type: type_name::<T>(),
            reason: source.to_string(),
        }
    })?;
    let actual_event_count = record.event_count();
    let expected_event_count = frame.event_count().get();
    if actual_event_count != expected_event_count as usize {
        return Err(DiskBufferError::DecodedEventCountMismatch {
            record_type: type_name::<T>(),
            expected: expected_event_count,
            actual: actual_event_count,
        });
    }

    Ok(record)
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

    #[snafu(display("persistent queue reader progress failed: {source}"))]
    ReadProgress { source: ReadProgressError },

    #[snafu(display("persistent queue checkpoint failed: {source}"))]
    Checkpoint { source: CheckpointError },

    #[snafu(display("persistent queue reader checkpoint is incompatible: {source}"))]
    DecodeCheckpoint { source: CheckpointDecodeError },

    #[snafu(display("{record_type} reported unsupported disk buffer event count {event_count}"))]
    InvalidEventCount {
        record_type: &'static str,
        event_count: usize,
    },

    #[snafu(display(
        "disk buffer codec metadata {codec_metadata:#034b} is invalid for {record_type}"
    ))]
    InvalidCodecMetadata {
        record_type: &'static str,
        codec_metadata: u32,
    },

    #[snafu(display(
        "disk buffer codec metadata {codec_metadata:#034b} is unsupported by {record_type}"
    ))]
    UnsupportedCodecMetadata {
        record_type: &'static str,
        codec_metadata: u32,
    },

    #[snafu(display("failed to decode disk buffer record {record_type}: {reason}"))]
    DecodeRecord {
        record_type: &'static str,
        reason: String,
    },

    #[snafu(display(
        "decoded disk buffer record {record_type} contains {actual} events but its frame declares {expected}"
    ))]
    DecodedEventCountMismatch {
        record_type: &'static str,
        expected: u32,
        actual: usize,
    },

    #[snafu(display("typed disk buffer receiver for {record_type} has already failed"))]
    TypedReceiverFailed { record_type: &'static str },

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
}
