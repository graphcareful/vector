use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_recursion::async_recursion;
use derivative::Derivative;
use tokio::{
    sync::{Mutex, mpsc},
    time::MissedTickBehavior,
};
use tracing::Span;
use vector_common::{
    finalization::Finalizable,
    internal_event::{InternalEventHandle, Registered, register},
};

use super::limited_queue::LimitedSender;
use crate::{
    BufferInstrumentation, Bufferable, WhenFull,
    buffer_usage_data::BufferUsageHandle,
    internal_events::BufferSendDuration,
    variants::disk_v2::{self, ProductionFilesystem},
};

/// Adapter for papering over various sender backends.
#[derive(Clone, Debug)]
pub enum SenderAdapter<T: Bufferable> {
    /// The in-memory channel buffer.
    InMemory(LimitedSender<T>),

    /// The disk v2 buffer.
    DiskV2 {
        /// The buffer writer, shared with the background flush task.
        writer: Arc<Mutex<disk_v2::BufferWriter<T, ProductionFilesystem>>>,

        /// Held only so its `Drop` signals shutdown: once this sender and all of its clones are
        /// gone, the channel closes and the background flush task performs its final durable flush
        /// and closes the writer. Never sent on.
        _shutdown: mpsc::Sender<()>,
    },
}

impl<T: Bufferable> From<LimitedSender<T>> for SenderAdapter<T> {
    fn from(v: LimitedSender<T>) -> Self {
        Self::InMemory(v)
    }
}

impl<T: Bufferable + Finalizable> From<disk_v2::BufferWriter<T, ProductionFilesystem>>
    for SenderAdapter<T>
{
    fn from(v: disk_v2::BufferWriter<T, ProductionFilesystem>) -> Self {
        let flush_interval = v.flush_interval();
        let writer = Arc::new(Mutex::new(v));
        // The task gets its own strong reference so the writer stays alive until the task has run
        // its final flush; the channel's receiver tells the task when all senders are gone.
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        spawn_flush_task(Arc::clone(&writer), flush_interval, shutdown_rx);
        Self::DiskV2 {
            writer,
            _shutdown: shutdown_tx,
        }
    }
}

/// How long the final shutdown flush may take before we give up and close the writer anyway.
///
/// A graceful shutdown should fsync the buffer's tail so its finalizers fire `Delivered`, but a
/// wedged disk must not block the writer from ever closing -- that would stall the reader and hold
/// the ledger lock, blocking a restart. If the flush exceeds this deadline we abandon it and close;
/// any still-pending finalizers then fire `Errored`, so the source retransmits rather than the
/// process hanging.
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// Background task that owns a disk buffer's deferred-ack flushing.
///
/// It holds a strong reference to the writer and drives two things:
///
/// - **Idle flushing:** every flush interval it fsyncs and fires any parked finalizers as
///   `Delivered`, so the tail of a batch written just before traffic goes idle is acknowledged
///   promptly instead of waiting for the next write.
/// - **Shutdown flushing:** `shutdown.recv()` resolves (with `None`) once this buffer's sender and
///   all of its clones have been dropped -- i.e. every upstream writer is gone. The task then
///   performs one final durable flush (bounded by [`SHUTDOWN_FLUSH_TIMEOUT`]), marks the writer
///   done via `close()`, and exits, releasing its strong reference so the writer can drop. Because
///   this runs in async context, the final flush can `await sync_all()` -- something the
///   synchronous `Drop` path cannot do, which is the whole reason this lives here.
///
/// INVARIANT: this task must hold the `Receiver` only, never a `Sender` clone. The shutdown signal
/// is "all senders dropped"; if the task held a sender, the channel could never close, so the task
/// would never exit, never release its strong reference, and the writer would never close -- a
/// deadlock that stalls the reader and holds the ledger lock open against a restart.
fn spawn_flush_task<T>(
    writer: Arc<Mutex<disk_v2::BufferWriter<T, ProductionFilesystem>>>,
    interval: Duration,
    mut shutdown: mpsc::Receiver<()>,
) where
    T: Bufferable + Finalizable,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = writer.lock().await.flush_pending_finalizers().await {
                        error!(
                            %error,
                            "Disk buffer writer encountered an unrecoverable error during periodic flush."
                        );
                        break;
                    }
                }

                _ = shutdown.recv() => {
                    // All senders are gone. Durably flush the tail (firing `Delivered`), then close.
                    let mut guard = writer.lock().await;
                    match tokio::time::timeout(
                        SHUTDOWN_FLUSH_TIMEOUT,
                        guard.flush_pending_finalizers(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => error!(
                            %error,
                            "Disk buffer writer failed its final flush on shutdown; pending records will be retransmitted."
                        ),
                        Err(_) => error!(
                            "Disk buffer writer timed out on its final flush on shutdown; closing anyway. Pending records will be retransmitted."
                        ),
                    }
                    guard.close();
                    break;
                }
            }
        }
    });
}

impl<T> SenderAdapter<T>
where
    T: Bufferable + Finalizable,
{
    pub(crate) async fn send(&mut self, item: T) -> crate::Result<()> {
        match self {
            Self::InMemory(tx) => tx.send(item).await.map_err(Into::into),
            Self::DiskV2 { writer, .. } => {
                let mut writer = writer.lock().await;

                writer.write_record(item).await.map(|_| ()).map_err(|e| {
                    // TODO: Could some errors be handled and not be unrecoverable? Right now,
                    // encoding should theoretically be recoverable -- encoded value was too big, or
                    // error during encoding -- but the traits don't allow for recovering the
                    // original event value because we have to consume it to do the encoding... but
                    // that might not always be the case.
                    error!("Disk buffer writer has encountered an unrecoverable error.");

                    e.into()
                })
            }
        }
    }

    pub(crate) async fn try_send(&mut self, item: T) -> crate::Result<Option<T>> {
        match self {
            Self::InMemory(tx) => tx
                .try_send(item)
                .map(|()| None)
                .or_else(|e| Ok(Some(e.into_inner()))),
            Self::DiskV2 { writer, .. } => {
                let mut writer = writer.lock().await;

                writer.try_write_record(item).await.map_err(|e| {
                    // TODO: Could some errors be handled and not be unrecoverable? Right now,
                    // encoding should theoretically be recoverable -- encoded value was too big, or
                    // error during encoding -- but the traits don't allow for recovering the
                    // original event value because we have to consume it to do the encoding... but
                    // that might not always be the case.
                    error!("Disk buffer writer has encountered an unrecoverable error.");

                    e.into()
                })
            }
        }
    }

    pub(crate) async fn flush(&mut self) -> crate::Result<()> {
        match self {
            Self::InMemory(_) => Ok(()),
            Self::DiskV2 { writer, .. } => {
                let mut writer = writer.lock().await;
                writer.flush().await.map_err(|e| {
                    // Errors on the I/O path, which is all that flushing touches, are never recoverable.
                    error!("Disk buffer writer has encountered an unrecoverable error.");

                    e.into()
                })
            }
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        match self {
            Self::InMemory(tx) => Some(tx.available_capacity()),
            Self::DiskV2 { .. } => None,
        }
    }
}

/// A buffer sender.
///
/// The sender handles sending events into the buffer, as well as the behavior around handling
/// events when the internal channel is full.
///
/// When creating a buffer sender/receiver pair, callers can specify the "when full" behavior of the
/// sender.  This controls how events are handled when the internal channel is full.  Three modes
/// are possible:
/// - block
/// - drop newest
/// - overflow
///
/// In "block" mode, callers are simply forced to wait until the channel has enough capacity to
/// accept the event.  In "drop newest" mode, any event being sent when the channel is full will be
/// dropped and proceed no further. In "overflow" mode, events will be sent to another buffer
/// sender.  Callers can specify the overflow sender to use when constructing their buffers initially.
///
/// TODO: We should eventually rework `BufferSender`/`BufferReceiver` so that they contain a vector
/// of the fields we already have here, but instead of cascading via calling into `overflow`, we'd
/// linearize the nesting instead, so that `BufferSender` would only ever be calling the underlying
/// `SenderAdapter` instances instead... which would let us get rid of the boxing and
/// `#[async_recursion]` stuff.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BufferSender<T: Bufferable> {
    base: SenderAdapter<T>,
    overflow: Option<Box<BufferSender<T>>>,
    when_full: WhenFull,
    usage_instrumentation: Option<BufferUsageHandle>,
    #[derivative(Debug = "ignore")]
    send_duration: Option<Registered<BufferSendDuration>>,
    #[derivative(Debug = "ignore")]
    custom_instrumentation: Option<Arc<dyn BufferInstrumentation<T>>>,
}

impl<T: Bufferable> BufferSender<T> {
    /// Creates a new [`BufferSender`] wrapping the given channel sender.
    pub fn new(base: SenderAdapter<T>, when_full: WhenFull) -> Self {
        Self {
            base,
            overflow: None,
            when_full,
            usage_instrumentation: None,
            send_duration: None,
            custom_instrumentation: None,
        }
    }

    /// Creates a new [`BufferSender`] wrapping the given channel sender and overflow sender.
    pub fn with_overflow(base: SenderAdapter<T>, overflow: BufferSender<T>) -> Self {
        Self {
            base,
            overflow: Some(Box::new(overflow)),
            when_full: WhenFull::Overflow,
            usage_instrumentation: None,
            send_duration: None,
            custom_instrumentation: None,
        }
    }

    /// Converts this sender into an overflowing sender using the given `BufferSender<T>`.
    ///
    /// Note: this resets the internal state of this sender, and so this should not be called except
    /// when initially constructing `BufferSender<T>`.
    #[cfg(test)]
    pub fn switch_to_overflow(&mut self, overflow: BufferSender<T>) {
        self.overflow = Some(Box::new(overflow));
        self.when_full = WhenFull::Overflow;
    }

    /// Configures this sender to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.usage_instrumentation = Some(handle);
    }

    /// Configures this sender to instrument the send duration.
    pub fn with_send_duration_instrumentation(&mut self, stage: usize, span: &Span) {
        let _enter = span.enter();
        self.send_duration = Some(register(BufferSendDuration { stage }));
    }

    /// Configures this sender to invoke a custom instrumentation hook.
    pub fn with_custom_instrumentation(&mut self, instrumentation: impl BufferInstrumentation<T>) {
        self.custom_instrumentation = Some(Arc::new(instrumentation));
    }
}

impl<T: Bufferable + Finalizable> BufferSender<T> {
    #[cfg(test)]
    pub(crate) fn get_base_ref(&self) -> &SenderAdapter<T> {
        &self.base
    }

    #[cfg(test)]
    pub(crate) fn get_overflow_ref(&self) -> Option<&BufferSender<T>> {
        self.overflow.as_ref().map(AsRef::as_ref)
    }

    #[async_recursion]
    pub async fn send(
        &mut self,
        mut item: T,
        send_reference: Option<Instant>,
    ) -> crate::Result<()> {
        if let Some(instrumentation) = self.custom_instrumentation.as_ref() {
            instrumentation.on_send(&mut item);
        }
        let item_sizing = self
            .usage_instrumentation
            .as_ref()
            .map(|_| (item.event_count(), item.size_of()));

        let mut was_dropped = false;

        if let Some(instrumentation) = self.usage_instrumentation.as_ref()
            && let Some((item_count, item_size)) = item_sizing
        {
            instrumentation
                .increment_received_event_count_and_byte_size(item_count as u64, item_size as u64);
        }
        match self.when_full {
            WhenFull::Block => self.base.send(item).await?,
            WhenFull::DropNewest => {
                if self.base.try_send(item).await?.is_some() {
                    was_dropped = true;
                }
            }
            WhenFull::Overflow => {
                if let Some(item) = self.base.try_send(item).await? {
                    was_dropped = true;
                    self.overflow
                        .as_mut()
                        .unwrap_or_else(|| unreachable!("overflow must exist"))
                        .send(item, send_reference)
                        .await?;
                }
            }
        }

        if let Some(instrumentation) = self.usage_instrumentation.as_ref()
            && let Some((item_count, item_size)) = item_sizing
            && was_dropped
        {
            instrumentation.increment_dropped_event_count_and_byte_size(
                item_count as u64,
                item_size as u64,
                true,
            );
        }
        if let Some(send_duration) = self.send_duration.as_ref()
            && let Some(send_reference) = send_reference
        {
            send_duration.emit(send_reference.elapsed());
        }

        Ok(())
    }

    #[async_recursion]
    pub async fn flush(&mut self) -> crate::Result<()> {
        self.base.flush().await?;
        if let Some(overflow) = self.overflow.as_mut() {
            overflow.flush().await?;
        }

        Ok(())
    }
}
