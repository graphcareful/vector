use std::{collections::VecDeque, sync::Arc, time::Duration};

use futures::{StreamExt, stream::BoxStream};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval_at},
};
use vector_common::finalization::{BatchStatus, EventFinalizerGroups, EventStatus};

use super::{
    acknowledgement::AcknowledgementToken,
    checkpoint::ReaderCheckpoint,
    disk_buffer::{DiskBufferError, PreparedRecord},
    frame::encode_frame,
    logical_capacity::LogicalCapacity,
    position::Position,
    segmented_log_writer::{DurabilityObserver, FilesystemSegmentStorage, SegmentedLogWriter},
};

/// Retained progress that allows producers and the consumer to observe the
/// actor without depending on edge-triggered notifications.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublishedWriterState {
    /// Everything before this position has been written to the segment file and
    /// can be read. It might still be lost in a crash because it may not have
    /// been synchronized to disk yet.
    pub(crate) committed: Position,
    /// Everything before this position has been synchronized to disk. Ingress
    /// finalizers for that data have been marked as delivered.
    pub(crate) data_synced: Position,
    /// Everything before this position has been acknowledged by downstream.
    /// This progress is still in memory and may not be saved in the checkpoint.
    pub(crate) consumer_acknowledged: Position,
    /// Everything before this position has been acknowledged and saved in the
    /// checkpoint. Its logical capacity has been released, and older sealed
    /// segments can be deleted.
    pub(crate) reclaimable: Position,
    /// Whether the writer is running, shut down normally, or failed.
    pub(crate) status: WriterStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WriterStatus {
    Running,
    Closed,
    Failed { reason: Arc<str> },
}

pub(super) enum WriterCommand {
    Append {
        record: PreparedRecord,
    },
    Flush {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
    Sync {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
}

#[derive(Clone, Copy, Debug)]
enum WriterFlushMode {
    Publish,
    Sync,
}

pub(super) struct WriterActor {
    writer: SegmentedLogWriter<FilesystemSegmentStorage, EventFinalizerGroups>,
    checkpoint: ReaderCheckpoint,
    capacity: LogicalCapacity,
    finalizations: BoxStream<'static, (BatchStatus, AcknowledgementToken)>,
    consumer_acknowledged: Position,
    reclaimable: Position,
    commands: mpsc::Receiver<WriterCommand>,
    state: watch::Sender<PublishedWriterState>,
    config: WriterActorConfig,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WriterActorConfig {
    pub(super) sync_interval: Duration,
    pub(super) max_frame_len: usize,
}

impl WriterActor {
    pub(super) fn new(
        writer: SegmentedLogWriter<FilesystemSegmentStorage>,
        checkpoint: ReaderCheckpoint,
        capacity: LogicalCapacity,
        finalizations: BoxStream<'static, (BatchStatus, AcknowledgementToken)>,
        commands: mpsc::Receiver<WriterCommand>,
        state: watch::Sender<PublishedWriterState>,
        config: WriterActorConfig,
    ) -> Self {
        let reclaimable = state.borrow().reclaimable;
        let finalizers = IngressFinalizerTracker::new(writer.data_synced());
        let writer = writer.with_durability_observer(finalizers);
        Self {
            writer,
            checkpoint,
            capacity,
            finalizations,
            consumer_acknowledged: reclaimable,
            reclaimable,
            commands,
            state,
            config,
        }
    }

    pub(super) async fn run(mut self) {
        let first_sync = Instant::now() + self.config.sync_interval;
        let mut sync = interval_at(first_sync, self.config.sync_interval);
        sync.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut finalizations_open = true;

        loop {
            tokio::select! {
                biased;
                _ = sync.tick() => {
                    if let Err(error) = self.writer.sync_committed().await {
                        self.fail(&DiskBufferError::Writer { source: error });
                        return;
                    }
                    self.publish(WriterStatus::Running);
                }
                finalized = self.finalizations.next(), if finalizations_open => {
                    let Some((_status, acknowledgement)) = finalized else {
                        finalizations_open = false;
                        continue;
                    };
                    // Any terminal downstream status consumes the frame, as in
                    // disk v2. Sink retry policy decides when an error is final.
                    if let Err(error) = self.checkpoint_acknowledgement(acknowledgement).await {
                        self.fail(&error);
                        return;
                    }
                    self.publish(WriterStatus::Running);
                }
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        match self.close().await {
                            Ok(_) => self.publish(WriterStatus::Closed),
                            Err(error) => self.fail(&error),
                        }
                        return;
                    };
                    if self.handle(command).await {
                        return;
                    }
                }
            }
        }
    }

    /// Returns `true` when the actor reached a terminal state.
    async fn handle(&mut self, command: WriterCommand) -> bool {
        match command {
            WriterCommand::Append { mut record } => {
                if let Err(error) = self.append(&record).await {
                    record.mark_errored();
                    self.fail(&error);
                    return true;
                }
                let finalizers = record.take_ingress_finalizers();
                self.writer.register_durability(finalizers);
                self.publish(WriterStatus::Running);
            }
            WriterCommand::Flush { reply } => {
                return self
                    .handle_flush_request(WriterFlushMode::Publish, reply)
                    .await;
            }
            WriterCommand::Sync { reply } => {
                return self
                    .handle_flush_request(WriterFlushMode::Sync, reply)
                    .await;
            }
            WriterCommand::Shutdown { reply } => match self.close().await {
                Ok(position) => {
                    self.publish(WriterStatus::Closed);
                    _ = reply.send(Ok(position));
                    self.fail_pending("disk buffer writer shut down");
                    return true;
                }
                Err(error) => {
                    let reason: Arc<str> = error.to_string().into();
                    _ = reply.send(Err(error));
                    self.fail_reason(&reason);
                    return true;
                }
            },
        }
        false
    }

    /// Returns `true` when the request failed and made the actor terminal.
    async fn handle_flush_request(
        &mut self,
        mode: WriterFlushMode,
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    ) -> bool {
        let result = match mode {
            WriterFlushMode::Publish => self.writer.flush().await,
            WriterFlushMode::Sync => self.writer.sync_all().await,
        };
        match result {
            Ok(position) => {
                self.publish(WriterStatus::Running);
                _ = reply.send(Ok(position));
                false
            }
            Err(source) => {
                let error = DiskBufferError::Writer { source };
                let reason: Arc<str> = error.to_string().into();
                _ = reply.send(Err(error));
                self.fail_reason(&reason);
                true
            }
        }
    }

    async fn append(&mut self, record: &PreparedRecord) -> Result<(), DiskBufferError> {
        let frame = encode_frame(
            self.writer.next_record_id(),
            record.event_count(),
            record.codec_metadata(),
            record.payload(),
            self.config.max_frame_len,
        )
        .map_err(|source| DiskBufferError::Frame { source })?;
        self.writer
            .append(frame)
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        Ok(())
    }

    async fn checkpoint_acknowledgement(
        &mut self,
        acknowledgement: AcknowledgementToken,
    ) -> Result<Position, DiskBufferError> {
        let (position, frame_bytes) = acknowledgement.into_parts();
        self.consumer_acknowledged = position;

        self.writer
            .sync_committed()
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        self.checkpoint
            .persist(position)
            .map_err(|source| DiskBufferError::Checkpoint { source })?;
        self.capacity
            .release(frame_bytes)
            .map_err(|source| DiskBufferError::Capacity { source })?;
        self.reclaimable = position;
        Ok(position)
    }

    async fn close(&mut self) -> Result<Position, DiskBufferError> {
        let position = self
            .writer
            .sync_all()
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        Ok(position)
    }

    fn publish(&self, status: WriterStatus) {
        let next = PublishedWriterState {
            committed: self.writer.committed(),
            data_synced: self.writer.data_synced(),
            consumer_acknowledged: self.consumer_acknowledged,
            reclaimable: self.reclaimable,
            status,
        };
        self.state.send_if_modified(|current| {
            if *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
    }

    fn fail(&mut self, error: &DiskBufferError) {
        let reason = Arc::from(error.to_string());
        self.fail_reason(&reason);
    }

    fn fail_reason(&mut self, reason: &Arc<str>) {
        self.writer.notify_durability_failure();
        self.publish(WriterStatus::Failed {
            reason: Arc::clone(reason),
        });
        self.fail_pending(reason.as_ref());
    }

    fn fail_pending(&mut self, reason: &str) {
        // Close first so a producer cannot enqueue between the final empty
        // `try_recv` and channel closure. Everything already accepted is then
        // failed explicitly instead of resolving its finalizers as delivered.
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            fail_command_with_reason(command, reason);
        }
    }
}

struct PendingIngressFinalizer {
    /// The first record ID after the record covered by these finalizers.
    next_record_id: u64,
    finalizers: EventFinalizerGroups,
}

struct IngressFinalizerTracker {
    data_synced_next_record_id: u64,
    entries: VecDeque<PendingIngressFinalizer>,
}

impl IngressFinalizerTracker {
    fn new(data_synced: Position) -> Self {
        Self {
            data_synced_next_record_id: data_synced.next_record_id(),
            entries: VecDeque::new(),
        }
    }

    fn resolve_through(&mut self, next_record_id: u64) {
        while self
            .entries
            .front()
            .is_some_and(|pending| pending.next_record_id <= next_record_id)
        {
            let pending = self
                .entries
                .pop_front()
                .expect("the pending finalizer entry was just observed");
            pending.finalizers.update_status(EventStatus::Delivered);
        }
    }

    fn mark_errored(&mut self) {
        for pending in self.entries.drain(..) {
            pending.finalizers.update_status(EventStatus::Errored);
        }
    }
}

impl DurabilityObserver for IngressFinalizerTracker {
    type Pending = EventFinalizerGroups;

    fn register(&mut self, next_record_id: u64, finalizers: EventFinalizerGroups) {
        if finalizers.is_empty() {
            return;
        }
        if next_record_id <= self.data_synced_next_record_id {
            finalizers.update_status(EventStatus::Delivered);
            return;
        }
        debug_assert!(
            self.entries
                .back()
                .is_none_or(|pending| pending.next_record_id < next_record_id),
            "writer-assigned record IDs must increase"
        );
        self.entries.push_back(PendingIngressFinalizer {
            next_record_id,
            finalizers,
        });
    }

    fn on_data_synced(&mut self, through: Position) {
        let next_record_id = through.next_record_id();
        debug_assert!(next_record_id >= self.data_synced_next_record_id);
        self.data_synced_next_record_id = next_record_id;
        self.resolve_through(next_record_id);
    }

    fn on_failure(&mut self) {
        self.mark_errored();
    }
}

impl Drop for IngressFinalizerTracker {
    fn drop(&mut self) {
        // Once a record has been accepted by the actor, an unexpected actor
        // cancellation or panic must not resolve its finalizers through their
        // delivered-equivalent default `Dropped` status.
        self.mark_errored();
    }
}

fn fail_command_with_reason(command: WriterCommand, reason: &str) {
    let error = || DiskBufferError::ActorFailed {
        reason: Arc::from(reason),
    };
    match command {
        WriterCommand::Append { record } => record.mark_errored(),
        WriterCommand::Flush { reply }
        | WriterCommand::Sync { reply }
        | WriterCommand::Shutdown { reply } => {
            _ = reply.send(Err(error()));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use vector_common::finalization::{
        BatchNotifier, BatchStatus, EventFinalizer, EventFinalizerGroups, EventFinalizers,
    };

    use super::{DurabilityObserver, IngressFinalizerTracker, Position};

    fn finalizers() -> (
        EventFinalizerGroups,
        vector_common::finalization::BatchStatusReceiver,
    ) {
        let (batch, status) = BatchNotifier::new_with_receiver();
        let finalizers =
            EventFinalizerGroups::from_flat(EventFinalizers::new(EventFinalizer::new(batch)));
        (finalizers, status)
    }

    #[tokio::test]
    async fn resolves_only_the_durable_finalizer_prefix() {
        let mut pending = IngressFinalizerTracker::new(Position::at_segment_start(0));
        let (first, first_status) = finalizers();
        let (second, second_status) = finalizers();
        pending.register(2, first);
        pending.register(4, second);

        pending.on_data_synced(Position::new(0, 10, 2));
        assert_eq!(first_status.await, BatchStatus::Delivered);
        tokio::pin!(second_status);
        assert!(
            timeout(Duration::from_millis(20), &mut second_status)
                .await
                .is_err()
        );

        pending.on_data_synced(Position::new(0, 20, 4));
        assert_eq!(second_status.await, BatchStatus::Delivered);
    }

    #[tokio::test]
    async fn marks_the_unsynchronized_finalizer_suffix_as_errored() {
        let mut pending = IngressFinalizerTracker::new(Position::at_segment_start(0));
        let (durable, durable_status) = finalizers();
        let (unsynchronized, unsynchronized_status) = finalizers();
        pending.register(2, durable);
        pending.register(4, unsynchronized);

        pending.on_data_synced(Position::new(0, 10, 2));
        pending.on_failure();

        assert_eq!(durable_status.await, BatchStatus::Delivered);
        assert_eq!(unsynchronized_status.await, BatchStatus::Errored);
    }

    #[tokio::test]
    async fn unexpected_queue_drop_marks_retained_finalizers_as_errored() {
        let mut pending = IngressFinalizerTracker::new(Position::at_segment_start(0));
        let (finalizers, status) = finalizers();
        pending.register(2, finalizers);

        drop(pending);

        assert_eq!(status.await, BatchStatus::Errored);
    }

    #[tokio::test]
    async fn registration_after_the_callback_resolves_immediately() {
        let mut pending = IngressFinalizerTracker::new(Position::at_segment_start(0));
        pending.on_data_synced(Position::new(0, 10, 2));
        let (finalizers, status) = finalizers();

        pending.register(2, finalizers);

        assert_eq!(status.await, BatchStatus::Delivered);
    }
}
