use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval_at},
};

use super::{
    acknowledgement::{AcknowledgementToken, AcknowledgementTracker},
    checkpoint::ReaderCheckpoint,
    disk_buffer::{DiskBufferError, PreparedRecord},
    frame::encode_frame,
    logical_capacity::{LogicalCapacity, LogicalCapacityReservation},
    position::Position,
    segmented_log_writer::{FilesystemSegmentStorage, SegmentedLogWriter},
};

/// Retained progress that allows producers and the consumer to observe the
/// actor without depending on edge-triggered notifications.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublishedWriterState {
    pub(crate) committed: Position,
    pub(crate) data_synced: Position,
    pub(crate) observed_acknowledged: Position,
    pub(crate) reclaimable: Position,
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
        reservation: LogicalCapacityReservation,
    },
    Flush {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
    Sync {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
    RegisterRead {
        position: Position,
        frame_bytes: usize,
        reply: oneshot::Sender<Result<AcknowledgementToken, DiskBufferError>>,
    },
    Acknowledge {
        acknowledgement: AcknowledgementToken,
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<Position, DiskBufferError>>,
    },
}

pub(super) struct WriterActor {
    writer: SegmentedLogWriter<FilesystemSegmentStorage>,
    checkpoint: ReaderCheckpoint,
    capacity: LogicalCapacity,
    acknowledgements: AcknowledgementTracker,
    commands: mpsc::Receiver<WriterCommand>,
    state: watch::Sender<PublishedWriterState>,
    sync_interval: Duration,
    max_frame_len: usize,
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
        acknowledgements: AcknowledgementTracker,
        commands: mpsc::Receiver<WriterCommand>,
        state: watch::Sender<PublishedWriterState>,
        config: WriterActorConfig,
    ) -> Self {
        Self {
            writer,
            checkpoint,
            capacity,
            acknowledgements,
            commands,
            state,
            sync_interval: config.sync_interval,
            max_frame_len: config.max_frame_len,
        }
    }

    pub(super) async fn run(mut self) {
        let first_sync = Instant::now() + self.sync_interval;
        let mut sync = interval_at(first_sync, self.sync_interval);
        sync.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
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
                _ = sync.tick() => {
                    if let Err(error) = self.writer.sync_committed().await {
                        self.fail(&DiskBufferError::Writer { source: error });
                        return;
                    }
                    self.publish(WriterStatus::Running);
                }
            }
        }
    }

    /// Returns `true` when the actor reached a terminal state.
    async fn handle(&mut self, command: WriterCommand) -> bool {
        match command {
            WriterCommand::Append {
                record,
                reservation,
            } => {
                if let Err(error) = self.append(&record, reservation).await {
                    record.mark_errored();
                    self.fail(&error);
                    return true;
                }
                self.publish(WriterStatus::Running);
            }
            WriterCommand::Flush { reply } => match self.writer.flush().await {
                Ok(position) => {
                    self.publish(WriterStatus::Running);
                    _ = reply.send(Ok(position));
                }
                Err(source) => {
                    let error = DiskBufferError::Writer { source };
                    let reason: Arc<str> = error.to_string().into();
                    _ = reply.send(Err(error));
                    self.fail_reason(&reason);
                    return true;
                }
            },
            WriterCommand::Sync { reply } => match self.writer.sync_all().await {
                Ok(position) => {
                    self.publish(WriterStatus::Running);
                    _ = reply.send(Ok(position));
                }
                Err(source) => {
                    let error = DiskBufferError::Writer { source };
                    let reason: Arc<str> = error.to_string().into();
                    _ = reply.send(Err(error));
                    self.fail_reason(&reason);
                    return true;
                }
            },
            WriterCommand::RegisterRead {
                position,
                frame_bytes,
                reply,
            } => {
                let result = self
                    .acknowledgements
                    .track_read(position, frame_bytes)
                    .map_err(|source| DiskBufferError::Acknowledgement { source });
                _ = reply.send(result);
                self.publish(WriterStatus::Running);
            }
            WriterCommand::Acknowledge {
                acknowledgement,
                reply,
            } => {
                let observed = match self.acknowledgements.acknowledge(acknowledgement) {
                    Ok(observed) => observed,
                    Err(source) => {
                        _ = reply.send(Err(DiskBufferError::Acknowledgement { source }));
                        return false;
                    }
                };
                let result = self.checkpoint_observed(observed).await;
                match result {
                    Ok(position) => {
                        self.publish(WriterStatus::Running);
                        _ = reply.send(Ok(position));
                    }
                    Err(error) => {
                        let reason: Arc<str> = error.to_string().into();
                        _ = reply.send(Err(error));
                        self.fail_reason(&reason);
                        return true;
                    }
                }
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

    async fn append(
        &mut self,
        record: &PreparedRecord,
        reservation: LogicalCapacityReservation,
    ) -> Result<(), DiskBufferError> {
        let frame = encode_frame(
            self.writer.next_record_id(),
            record.event_count(),
            record.codec_metadata(),
            record.payload(),
            self.max_frame_len,
        )
        .map_err(|source| DiskBufferError::Frame { source })?;
        reservation
            .validate(&self.capacity, frame.bytes().len())
            .map_err(|source| DiskBufferError::Capacity { source })?;
        self.writer
            .append(frame)
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        reservation.commit();
        Ok(())
    }

    async fn checkpoint_observed(
        &mut self,
        observed: Position,
    ) -> Result<Position, DiskBufferError> {
        let Some(candidate) = self.acknowledgements.checkpoint_candidate() else {
            return Ok(observed);
        };

        self.writer
            .sync_all()
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        self.checkpoint
            .persist(candidate.position())
            .map_err(|source| DiskBufferError::Checkpoint { source })?;
        self.capacity
            .release(candidate.bytes())
            .map_err(|source| DiskBufferError::Capacity { source })?;
        self.acknowledgements.mark_checkpointed(candidate);
        Ok(candidate.position())
    }

    async fn close(&mut self) -> Result<Position, DiskBufferError> {
        let position = self
            .writer
            .sync_all()
            .await
            .map_err(|source| DiskBufferError::Writer { source })?;
        if let Some(candidate) = self.acknowledgements.checkpoint_candidate() {
            self.checkpoint
                .persist(candidate.position())
                .map_err(|source| DiskBufferError::Checkpoint { source })?;
            self.capacity
                .release(candidate.bytes())
                .map_err(|source| DiskBufferError::Capacity { source })?;
            self.acknowledgements.mark_checkpointed(candidate);
        }
        Ok(position)
    }

    fn publish(&self, status: WriterStatus) {
        let next = PublishedWriterState {
            committed: self.writer.committed(),
            data_synced: self.writer.data_synced(),
            observed_acknowledged: self.acknowledgements.observed_position(),
            reclaimable: self.acknowledgements.reclaimable_position(),
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

pub(super) fn fail_command(command: WriterCommand) {
    fail_command_with_reason(command, "disk buffer writer is unavailable");
}

fn fail_command_with_reason(command: WriterCommand, reason: &str) {
    let error = || DiskBufferError::ActorFailed {
        reason: Arc::from(reason),
    };
    match command {
        WriterCommand::Append { record, .. } => record.mark_errored(),
        WriterCommand::Flush { reply }
        | WriterCommand::Sync { reply }
        | WriterCommand::Shutdown { reply }
        | WriterCommand::Acknowledge { reply, .. } => {
            _ = reply.send(Err(error()));
        }
        WriterCommand::RegisterRead { reply, .. } => {
            _ = reply.send(Err(error()));
        }
    }
}
