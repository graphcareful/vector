use std::{sync::Arc, time::Instant};

use derivative::Derivative;
use tokio::sync::Mutex;
use tracing::Span;
use vector_common::internal_event::{InternalEventHandle, Registered, register};

use super::limited_queue::LimitedSender;
use crate::{
    BufferInstrumentation, Bufferable, WhenFull,
    buffer_usage_data::BufferUsageHandle,
    internal_events::BufferSendDuration,
    variants::disk_v2::{self, ProductionFilesystem},
};

/// Internal backend dispatch for buffer senders.
#[derive(Clone, Debug)]
enum SenderBackend<T: Bufferable> {
    InMemory(LimitedSender<T>),
    DiskV2(Arc<Mutex<disk_v2::BufferWriter<T, ProductionFilesystem>>>),
}

/// A buffer sender.
///
/// The sender handles sending events into the buffer, as well as the behavior around handling
/// events when the internal channel is full.
///
/// When creating a buffer sender/receiver pair, callers can specify the "when full" behavior of the
/// sender.  This controls how events are handled when the internal channel is full.  Two modes
/// are possible:
/// - block
/// - drop newest
///
/// In "block" mode, callers are simply forced to wait until the channel has enough capacity to
/// accept the event.  In "drop newest" mode, any event being sent when the channel is full will be
/// dropped and proceed no further.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BufferSender<T: Bufferable> {
    backend: SenderBackend<T>,
    when_full: WhenFull,
    usage_instrumentation: Option<BufferUsageHandle>,
    #[derivative(Debug = "ignore")]
    send_duration: Option<Registered<BufferSendDuration>>,
    #[derivative(Debug = "ignore")]
    custom_instrumentation: Option<Arc<dyn BufferInstrumentation<T>>>,
}

impl<T: Bufferable> BufferSender<T> {
    /// Creates a new [`BufferSender`] backed by an in-memory channel.
    pub fn memory(sender: LimitedSender<T>, when_full: WhenFull) -> Self {
        Self {
            backend: SenderBackend::InMemory(sender),
            when_full,
            usage_instrumentation: None,
            send_duration: None,
            custom_instrumentation: None,
        }
    }

    /// Creates a new [`BufferSender`] backed by a disk v2 buffer.
    pub fn disk_v2(
        writer: disk_v2::BufferWriter<T, ProductionFilesystem>,
        when_full: WhenFull,
    ) -> Self {
        Self {
            backend: SenderBackend::DiskV2(Arc::new(Mutex::new(writer))),
            when_full,
            usage_instrumentation: None,
            send_duration: None,
            custom_instrumentation: None,
        }
    }

    /// Sets the "when full" behavior for this sender.
    pub fn set_when_full(&mut self, when_full: WhenFull) {
        self.when_full = when_full;
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

    /// Returns the available capacity of the underlying channel, if applicable.
    pub fn capacity(&self) -> Option<usize> {
        match &self.backend {
            SenderBackend::InMemory(tx) => Some(tx.available_capacity()),
            SenderBackend::DiskV2(_) => None,
        }
    }
}

impl<T: Bufferable> BufferSender<T> {
    async fn send_inner(&mut self, item: T) -> crate::Result<()> {
        match &mut self.backend {
            SenderBackend::InMemory(tx) => tx.send(item).await.map_err(Into::into),
            SenderBackend::DiskV2(writer) => {
                let mut writer = writer.lock().await;
                writer.write_record(item).await.map(|_| ()).map_err(|e| {
                    error!("Disk buffer writer has encountered an unrecoverable error.");
                    e.into()
                })
            }
        }
    }

    async fn try_send_inner(&mut self, item: T) -> crate::Result<Option<T>> {
        match &mut self.backend {
            SenderBackend::InMemory(tx) => tx
                .try_send(item)
                .map(|()| None)
                .or_else(|e| Ok(Some(e.into_inner()))),
            SenderBackend::DiskV2(writer) => {
                let mut writer = writer.lock().await;
                writer.try_write_record(item).await.map_err(|e| {
                    error!("Disk buffer writer has encountered an unrecoverable error.");
                    e.into()
                })
            }
        }
    }

    async fn flush_inner(&mut self) -> crate::Result<()> {
        match &mut self.backend {
            SenderBackend::InMemory(_) => Ok(()),
            SenderBackend::DiskV2(writer) => {
                let mut writer = writer.lock().await;
                writer.flush().await.map_err(|e| {
                    error!("Disk buffer writer has encountered an unrecoverable error.");
                    e.into()
                })
            }
        }
    }

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
            WhenFull::Block => self.send_inner(item).await?,
            WhenFull::DropNewest => {
                if self.try_send_inner(item).await?.is_some() {
                    was_dropped = true;
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

    pub async fn flush(&mut self) -> crate::Result<()> {
        self.flush_inner().await
    }
}
