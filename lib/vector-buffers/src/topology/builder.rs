use std::{error::Error, num::NonZeroUsize};

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use tracing::Span;

use super::channel::{ChannelMetricMetadata, ReceiverAdapter, SenderAdapter};
use crate::{
    Bufferable, WhenFull,
    buffer_usage_data::{BufferUsage, BufferUsageHandle},
    config::MemoryBufferSize,
    topology::channel::{BufferReceiver, BufferSender, limited},
};

/// Value that can be used as a stage in a buffer topology.
#[async_trait]
pub trait IntoBuffer<T: Bufferable>: Send {
    /// Gets whether or not this buffer stage provides its own instrumentation, or if it should be
    /// instrumented from the outside.
    ///
    /// As some buffer stages, like the in-memory channel, never have a chance to catch the values
    /// in the middle of the channel without introducing an unnecessary hop, [`BufferSender`] and
    /// [`BufferReceiver`] can be configured to instrument all events flowing through directly.
    ///
    /// When instrumentation is provided in this way, [`vector_common::byte_size_of::ByteSizeOf`]
    ///  is used to calculate the size of the event going both into and out of the buffer.
    fn provides_instrumentation(&self) -> bool {
        false
    }

    /// Converts this value into a sender and receiver pair suitable for use in a buffer topology.
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(SenderAdapter<T>, ReceiverAdapter<T>), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Snafu)]
pub enum TopologyError {
    #[snafu(display("buffer topology cannot be empty"))]
    EmptyTopology,
    #[snafu(display("buffer topology must contain exactly one stage, but has {}", stage_count))]
    TooManyStages { stage_count: usize },
    #[snafu(display("failed to build individual stage {}: {}", stage_idx, source))]
    FailedToBuildStage {
        stage_idx: usize,
        source: Box<dyn Error + Send + Sync>,
    },
    #[snafu(display(
        "multiple components with segmented acknowledgements cannot be used in the same buffer"
    ))]
    StackedAcks,
}

struct TopologyStage<T: Bufferable> {
    untransformed: Box<dyn IntoBuffer<T>>,
    when_full: WhenFull,
}

/// Builder for constructing buffer topologies.
pub struct TopologyBuilder<T: Bufferable> {
    stages: Vec<TopologyStage<T>>,
}

impl<T: Bufferable> TopologyBuilder<T> {
    /// Adds a new stage to the buffer topology.
    ///
    /// Callers can configure what to do when a buffer is full by setting `when_full`.  Two modes
    /// are available -- block and drop newest -- which are documented in more detail by
    /// [`BufferSender`].
    pub fn stage<S>(&mut self, stage: S, when_full: WhenFull) -> &mut Self
    where
        S: IntoBuffer<T> + 'static,
    {
        self.stages.push(TopologyStage {
            untransformed: Box::new(stage),
            when_full,
        });
        self
    }

    /// Consumes this builder, returning the sender and receiver that can be used by components.
    ///
    /// # Errors
    ///
    /// If there was a configuration error with one of the stages, an error variant will be returned
    /// explaining the issue.
    pub async fn build(
        self,
        buffer_id: String,
        span: Span,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), TopologyError> {
        let stage_count = self.stages.len();
        if stage_count == 0 {
            return Err(TopologyError::EmptyTopology);
        }
        if stage_count > 1 {
            return Err(TopologyError::TooManyStages { stage_count });
        }

        let mut buffer_usage = BufferUsage::from_span(span.clone());

        let stage = self.stages.into_iter().next().unwrap();
        let stage_idx = 0;

        let usage_handle = buffer_usage.add_stage(stage_idx);
        let provides_instrumentation = stage.untransformed.provides_instrumentation();
        let (sender, receiver) = stage
            .untransformed
            .into_buffer_parts(usage_handle.clone())
            .await
            .context(FailedToBuildStageSnafu { stage_idx })?;

        let mut sender = BufferSender::new(sender, stage.when_full);
        let mut receiver = BufferReceiver::new(receiver);

        sender.with_send_duration_instrumentation(stage_idx, &span);
        if !provides_instrumentation {
            sender.with_usage_instrumentation(usage_handle.clone());
            receiver.with_usage_instrumentation(usage_handle);
        }

        // Install the buffer usage handler since we successfully created the buffer topology.  This
        // spawns it in the background and periodically emits aggregated metrics about each of the
        // buffer stages.
        buffer_usage.install(buffer_id.as_str());

        Ok((sender, receiver))
    }
}

impl<T: Bufferable> TopologyBuilder<T> {
    /// Creates a memory-only buffer topology.
    ///
    /// This is a convenience method for `vector` as it is used for inter-transform channels, and we
    /// can simplifying needing to require callers to do all the boilerplate to create the builder,
    /// create the stage, installing buffer usage metrics that aren't required, and so on.
    #[allow(clippy::print_stderr)]
    pub fn standalone_memory(
        max_events: NonZeroUsize,
        when_full: WhenFull,
        receiver_span: &Span,
        metadata: Option<ChannelMetricMetadata>,
        ewma_half_life_seconds: Option<f64>,
    ) -> (BufferSender<T>, BufferReceiver<T>) {
        let usage_handle = BufferUsageHandle::noop();
        usage_handle.set_buffer_limits(None, Some(max_events.get()));

        let limit = MemoryBufferSize::MaxEvents(max_events);
        let (sender, receiver) = limited(limit, metadata, ewma_half_life_seconds);

        let mut sender = BufferSender::new(sender.into(), when_full);
        sender.with_send_duration_instrumentation(0, receiver_span);
        let receiver = BufferReceiver::new(receiver.into());

        (sender, receiver)
    }

    /// Creates a memory-only buffer topology with the given buffer usage handle.
    ///
    /// This is specifically required for the tests that occur under `buffers`, as we assert things
    /// like channel capacity left, which cannot be done on in-memory v1 buffers as they use the
    /// more abstract `Sink`-based adapters.
    ///
    /// This is a convenience method for `vector` as it is used for inter-transform channels, and we
    /// can simplifying needing to require callers to do all the boilerplate to create the builder,
    /// create the stage, installing buffer usage metrics that aren't required, and so on.
    #[cfg(test)]
    pub fn standalone_memory_test(
        max_events: NonZeroUsize,
        when_full: WhenFull,
        usage_handle: BufferUsageHandle,
        metadata: Option<ChannelMetricMetadata>,
    ) -> (BufferSender<T>, BufferReceiver<T>) {
        usage_handle.set_buffer_limits(None, Some(max_events.get()));

        let limit = MemoryBufferSize::MaxEvents(max_events);
        let (sender, receiver) = limited(limit, metadata, None);

        let mut sender = BufferSender::new(sender.into(), when_full);
        let mut receiver = BufferReceiver::new(receiver.into());

        sender.with_usage_instrumentation(usage_handle.clone());
        receiver.with_usage_instrumentation(usage_handle);

        (sender, receiver)
    }
}

impl<T: Bufferable> Default for TopologyBuilder<T> {
    fn default() -> Self {
        Self { stages: Vec::new() }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tracing::Span;

    use super::TopologyBuilder;
    use crate::{
        WhenFull,
        topology::test_util::{Sample, assert_current_send_capacity},
        variants::MemoryBuffer,
    };

    #[tokio::test]
    async fn single_stage_topology_block() {
        let mut builder = TopologyBuilder::<Sample>::default();
        builder.stage(
            MemoryBuffer::with_max_events(NonZeroUsize::new(1).unwrap()),
            WhenFull::Block,
        );
        let result = builder.build(String::from("test"), Span::none()).await;
        assert!(result.is_ok());

        let (mut sender, _) = result.unwrap();
        assert_current_send_capacity(&mut sender, Some(1));
    }

    #[tokio::test]
    async fn single_stage_topology_drop_newest() {
        let mut builder = TopologyBuilder::<Sample>::default();
        builder.stage(
            MemoryBuffer::with_max_events(NonZeroUsize::new(1).unwrap()),
            WhenFull::DropNewest,
        );
        let result = builder.build(String::from("test"), Span::none()).await;
        assert!(result.is_ok());

        let (mut sender, _) = result.unwrap();
        assert_current_send_capacity(&mut sender, Some(1));
    }

}
