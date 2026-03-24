use std::{error::Error, num::NonZeroUsize};

use async_trait::async_trait;
use snafu::Snafu;

use super::channel::ChannelMetricMetadata;
use crate::{
    Bufferable, WhenFull,
    buffer_usage_data::BufferUsageHandle,
    config::MemoryBufferSize,
    topology::channel::{BufferReceiver, BufferSender, limited},
};

/// Value that can be used as a buffer implementation.
#[async_trait]
pub trait IntoBuffer<T: Bufferable>: Send {
    /// Gets whether or not this buffer provides its own instrumentation, or if it should be
    /// instrumented from the outside.
    ///
    /// As some buffers, like the in-memory channel, never have a chance to catch the values
    /// in the middle of the channel without introducing an unnecessary hop, [`BufferSender`] and
    /// [`BufferReceiver`] can be configured to instrument all events flowing through directly.
    ///
    /// When instrumentation is provided in this way, [`vector_common::byte_size_of::ByteSizeOf`]
    ///  is used to calculate the size of the event going both into and out of the buffer.
    fn provides_instrumentation(&self) -> bool {
        false
    }

    /// Converts this value into a sender and receiver pair.
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Snafu)]
pub enum TopologyError {
    #[snafu(display("failed to build buffer stage: {}", source))]
    FailedToBuildStage {
        source: Box<dyn Error + Send + Sync>,
    },
}

/// Creates a memory-only buffer.
///
/// This is a convenience method for `vector` as it is used for inter-transform channels, and we
/// can simplify needing to require callers to do all the boilerplate to create the buffer,
/// install buffer usage metrics that aren't required, and so on.
#[allow(clippy::print_stderr)]
pub fn standalone_memory<T: Bufferable>(
    max_events: NonZeroUsize,
    when_full: WhenFull,
    receiver_span: &tracing::Span,
    metadata: Option<ChannelMetricMetadata>,
    ewma_half_life_seconds: Option<f64>,
) -> (BufferSender<T>, BufferReceiver<T>) {
    let usage_handle = BufferUsageHandle::noop();
    usage_handle.set_buffer_limits(None, Some(max_events.get()));

    let limit = MemoryBufferSize::MaxEvents(max_events);
    let (sender, receiver) = limited(limit, metadata, ewma_half_life_seconds);

    let mut sender = BufferSender::memory(sender, when_full);
    sender.with_send_duration_instrumentation(0, receiver_span);
    let receiver = BufferReceiver::memory(receiver);

    (sender, receiver)
}

/// Creates a memory-only buffer with the given buffer usage handle.
///
/// This is specifically required for the tests that occur under `buffers`, as we assert things
/// like channel capacity left, which cannot be done on in-memory v1 buffers as they use the
/// more abstract `Sink`-based adapters.
#[cfg(test)]
pub fn standalone_memory_test<T: Bufferable>(
    max_events: NonZeroUsize,
    when_full: WhenFull,
    usage_handle: BufferUsageHandle,
    metadata: Option<ChannelMetricMetadata>,
) -> (BufferSender<T>, BufferReceiver<T>) {
    usage_handle.set_buffer_limits(None, Some(max_events.get()));

    let limit = MemoryBufferSize::MaxEvents(max_events);
    let (sender, receiver) = limited(limit, metadata, None);

    let mut sender = BufferSender::memory(sender, when_full);
    let mut receiver = BufferReceiver::memory(receiver);

    sender.with_usage_instrumentation(usage_handle.clone());
    receiver.with_usage_instrumentation(usage_handle);

    (sender, receiver)
}
