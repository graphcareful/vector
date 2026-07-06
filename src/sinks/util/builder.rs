use std::{
    convert::Infallible,
    fmt,
    future::Future,
    hash::Hash,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Stream, StreamExt, stream::Map};
use pin_project::pin_project;
use tower::Service;
use tracing::Span;
use vector_lib::{
    ByteSizeOf,
    event::{Finalizable, Metric},
    partition::Partitioner,
    stream::{
        ConcurrentMap, Driver, DriverResponse, ExpirationQueue, PartitionedBatcher,
        batcher::{Batcher, config::BatchConfig},
    },
};

use super::{
    IncrementalRequestBuilder, Normalizer, RequestBuilder, buffer::metrics::MetricNormalize,
};

impl<T: ?Sized> SinkBuilderExt for T where T: Stream {}

pub trait SinkBuilderExt: Stream {
    /// Converts a stream of infallible results by unwrapping them.
    ///
    /// For a stream of `Result<T, Infallible>` items, this turns it into a stream of `T` items.
    fn unwrap_infallible<T>(self) -> UnwrapInfallible<Self>
    where
        Self: Stream<Item = Result<T, Infallible>> + Sized,
    {
        UnwrapInfallible { st: self }
    }

    /// Batches the stream based on the given partitioner and batch settings.
    ///
    /// The stream will yield batches of events, with their partition key, when either a batch fills
    /// up or times out. [`Partitioner`] operates on a per-event basis, and has access to the event
    /// itself, and so can access any and all fields of an event.
    ///
    /// The `settings` closure receives the partition key for each new partition, allowing callers
    /// to vary the batch configuration (e.g. byte size limit) per partition.
    fn batched_partitioned<P, C, F, B>(
        self,
        partitioner: P,
        timeout: Duration,
        settings: F,
    ) -> PartitionedBatcher<Self, P, ExpirationQueue<P::Key>, C, F, B>
    where
        Self: Stream<Item = P::Item> + Sized,
        P: Partitioner + Unpin,
        P::Key: Eq + Hash + Clone,
        P::Item: ByteSizeOf,
        C: BatchConfig<P::Item>,
        F: Fn(&P::Key) -> C + Send,
    {
        PartitionedBatcher::new(self, partitioner, timeout, settings)
    }

    /// Batches the stream based on the given batch settings and item size calculator.
    ///
    /// The stream will yield batches of events, when either a batch fills
    /// up or times out. The `item_size_calculator` determines the "size" of each input
    /// in a batch. The units of "size" are intentionally not defined, so you can choose
    /// whatever is needed.
    fn batched<C>(self, config: C) -> Batcher<Self, C>
    where
        C: BatchConfig<Self::Item>,
        Self: Sized,
    {
        Batcher::new(self, config)
    }

    /// Maps the items in the stream concurrently, up to the configured limit.
    ///
    /// For every item, the given mapper is invoked, and the future that is returned is spawned
    /// and awaited concurrently.  A limit can be passed: `None` is self-describing, as it imposes
    /// no concurrency limit, and `Some(n)` limits this stage to `n` concurrent operations at any
    /// given time.
    ///
    /// If the spawned future panics, the panic will be carried through and resumed on the task
    /// calling the stream.
    fn concurrent_map<F, T>(self, limit: NonZeroUsize, f: F) -> ConcurrentMap<Self, T>
    where
        Self: Sized,
        F: Fn(Self::Item) -> Pin<Box<dyn Future<Output = T> + Send + 'static>> + Send + 'static,
        T: Send + 'static,
    {
        ConcurrentMap::new(self, Some(limit), f)
    }

    /// Constructs a [`Stream`] which transforms the input into a request suitable for sending to
    /// downstream services.
    ///
    /// Each input is transformed concurrently, up to the given limit.  A limit of `n` limits
    /// this stage to `n` concurrent operations at any given time.
    ///
    /// Encoding and compression are handled internally, deferring to the builder at the necessary
    /// checkpoints for adjusting the event before encoding/compression, as well as generating the
    /// correct request object with the result of encoding/compressing the events.
    fn request_builder<B>(
        self,
        limit: NonZeroUsize,
        builder: B,
    ) -> ConcurrentMap<Self, Result<B::Request, B::Error>>
    where
        Self: Sized,
        Self::Item: Send + 'static,
        B: RequestBuilder<<Self as Stream>::Item> + Send + Sync + 'static,
        B::Error: Send,
        B::Request: Send,
    {
        let builder = Arc::new(builder);

        // The future passed into the concurrent map is spawned in a tokio thread so we must preserve
        // the span context in order to propagate the sink's automatic tags.
        let span = Arc::new(Span::current());

        self.concurrent_map(limit, move |input| {
            let builder = Arc::clone(&builder);
            let span = Arc::clone(&span);

            Box::pin(async move {
                let _entered = span.enter();

                // Split the input into metadata and events.
                let (metadata, request_metadata_builder, events) = builder.split_input(input);

                // Encode the events.
                let payload = builder.encode_events(events)?;

                // Note: it would be nice for the RequestMetadataBuilder to build be created from the
                // events here, and not need to be required by split_input(). But this then requires
                // each Event type to implement Serialize, and that causes conflicts with the Serialize
                // implementation for EstimatedJsonEncodedSizeOf.

                // Build the request metadata.
                let request_metadata = request_metadata_builder.build(&payload);

                // Now build the actual request.
                Ok(builder.build_request(metadata, request_metadata, payload))
            })
        })
    }

    /// Constructs a [`Stream`] which transforms the input into a number of requests suitable for
    /// sending to downstream services.
    ///
    /// Unlike `request_builder`, which depends on the `RequestBuilder` trait,
    /// `incremental_request_builder` depends on the `IncrementalRequestBuilder` trait, which is
    /// designed specifically for sinks that have more stringent requirements around the generated
    /// requests.
    ///
    /// As an example, the normal `request_builder` doesn't allow for a batch of input events to be
    /// split up: all events must be split at the beginning, encoded separately (and all together),
    /// and then reassembled into the request.  If the encoding of these events caused a payload to
    /// be generated that was, say, too large, you would have to back out the operation entirely by
    /// failing the batch.
    ///
    /// With `incremental_request_builder`, the builder is given all of the events in a single shot,
    /// and can generate multiple payloads.  This is the maximally flexible approach to encoding,
    /// but means that the trait doesn't provide any default methods like `RequestBuilder` does.
    ///
    /// Each input is transformed serially.
    ///
    /// Encoding and compression are handled internally, deferring to the builder at the necessary
    /// checkpoints for adjusting the event before encoding/compression, as well as generating the
    /// correct request object with the result of encoding/compressing the events.
    fn incremental_request_builder<B>(
        self,
        mut builder: B,
    ) -> Map<Self, Box<dyn FnMut(Self::Item) -> Vec<Result<B::Request, B::Error>> + Send + Sync>>
    where
        Self: Sized,
        Self::Item: Send + 'static,
        B: IncrementalRequestBuilder<<Self as Stream>::Item> + Send + Sync + 'static,
        B::Error: Send,
        B::Request: Send,
    {
        self.map(Box::new(move |input| {
            builder
                .encode_events_incremental(input)
                .into_iter()
                .map(|result| {
                    result.map(|(metadata, payload)| builder.build_request(metadata, payload))
                })
                .collect()
        }))
    }

    /// Normalizes a stream of [`Metric`] events with the provided normalizer.
    ///
    /// An implementation of [`MetricNormalize`] is used to either drop metrics which cannot be
    /// supported by the sink, or to modify them.  Such modifications typically include converting
    /// absolute metrics to incremental metrics by tracking the change over time for a particular
    /// series, or emitting absolute metrics based on incremental updates.
    fn normalized<N>(self, normalizer: N) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize,
    {
        Normalizer::new(self, normalizer)
    }

    /// Normalizes a stream of [`Metric`] events with a default normalizer.
    ///
    /// An implementation of [`MetricNormalize`] is used to either drop metrics which cannot be
    /// supported by the sink, or to modify them.  Such modifications typically include converting
    /// absolute metrics to incremental metrics by tracking the change over time for a particular
    /// series, or emitting absolute metrics based on incremental updates.
    fn normalized_with_default<N>(self) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize + Default,
    {
        Normalizer::new(self, N::default())
    }

    /// Normalizes a stream of [`Metric`] events with a normalizer and an optional TTL.
    fn normalized_with_ttl<N>(self, maybe_ttl_secs: Option<f64>) -> Normalizer<Self, N>
    where
        Self: Stream<Item = Metric> + Unpin + Sized,
        N: MetricNormalize + Default,
    {
        match maybe_ttl_secs {
            None => Normalizer::new(self, N::default()),
            Some(ttl) => {
                Normalizer::new_with_ttl(self, N::default(), Duration::from_secs(ttl as u64))
            }
        }
    }

    /// Creates a [`Driver`] that uses the configured event stream as the input to the given
    /// service.
    ///
    /// This is typically a terminal step in building a sink, bridging the gap from the processing
    /// that must be performed by Vector (in the stream) to the underlying sink itself (the
    /// service).
    fn into_driver<Svc>(self, service: Svc) -> Driver<Self, Svc>
    where
        Self: Sized,
        Self::Item: Finalizable,
        Svc: Service<Self::Item>,
        Svc::Error: fmt::Debug + 'static,
        Svc::Future: Send + 'static,
        Svc::Response: DriverResponse,
    {
        Driver::new(self, service)
    }
}

#[pin_project]
pub struct UnwrapInfallible<St> {
    #[pin]
    st: St,
}

impl<St, T> Stream for UnwrapInfallible<St>
where
    St: Stream<Item = Result<T, Infallible>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.st
            .poll_next(cx)
            .map(|maybe| maybe.map(|result| result.unwrap()))
    }
}

#[cfg(test)]
mod disk_buffer_driver_repro_tests {
    //! Full-pipeline reproducer for the zero-fault "a durable record is never delivered" symptom.
    //!
    //! Every isolated layer -- the raw disk reader, the `BufferReceiverStream` adapter,
    //! `ready_chunks` over it, and the `Driver` fed by a channel -- has been shown to wake and
    //! deliver a lone record arriving after going idle. This assembles the real sink shape a disk
    //! buffer feeds: `receiver.into_stream() -> batched() -> map(request) -> into_driver(service)`,
    //! backed by an actual `disk_v2` buffer, and drives exactly the failing scenario: let the sink
    //! go idle, then write ONE record and require the service to receive it. If the wakeup is lost
    //! in the integration, the service never sees it and the test times out.
    use std::{
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use futures::{StreamExt, future::BoxFuture};
    use tower::Service;
    use tracing::Span;
    use vector_lib::buffers::{BufferConfig, BufferType, WhenFull};
    use vector_lib::{
        internal_event::CountByteSize,
        json_size::JsonSize,
        request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata},
        stream::DriverResponse,
    };

    use crate::{
        event::{
            Event, EventArray, EventContainer, EventFinalizers, EventStatus, Finalizable, LogEvent,
        },
        sinks::util::{BatchConfig, RealtimeEventBasedDefaultBatchSettings, SinkBuilderExt},
    };

    // Minimal request: a batch of event arrays plus request metadata, so it satisfies the
    // `Driver`'s `Finalizable + MetaDescriptive` bounds (raw `Vec<EventArray>` isn't
    // `MetaDescriptive`). Sink buffers carry `EventArray`, so that is the buffered item type.
    struct BatchRequest {
        arrays: Vec<EventArray>,
        metadata: RequestMetadata,
    }

    impl Finalizable for BatchRequest {
        fn take_finalizers(&mut self) -> EventFinalizers {
            self.arrays.take_finalizers()
        }
    }

    impl MetaDescriptive for BatchRequest {
        fn get_metadata(&self) -> &RequestMetadata {
            &self.metadata
        }
        fn metadata_mut(&mut self) -> &mut RequestMetadata {
            &mut self.metadata
        }
    }

    struct CountResponse {
        sent: GroupedCountByteSize,
    }

    impl DriverResponse for CountResponse {
        fn event_status(&self) -> EventStatus {
            EventStatus::Delivered
        }
        fn events_sent(&self) -> &GroupedCountByteSize {
            &self.sent
        }
    }

    #[derive(Clone)]
    struct CountingSink {
        received: Arc<AtomicUsize>,
    }

    impl Service<BatchRequest> for CountingSink {
        type Response = CountResponse;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<'static, Result<CountResponse, std::convert::Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: BatchRequest) -> Self::Future {
            let n: usize = req.arrays.iter().map(EventContainer::len).sum();
            self.received.fetch_add(n, Ordering::Relaxed);
            Box::pin(async move {
                Ok(CountResponse {
                    sent: CountByteSize(n, JsonSize::new(n)).into(),
                })
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disk_buffer_sink_delivers_lone_record_arriving_after_idle() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Build a real disk_v2 buffer of `EventArray` via the public config path (which wires up
        // the correct Event encoding). `max_size` must clear the disk buffer's minimum.
        let config = BufferConfig::Single(BufferType::DiskV2 {
            max_size: NonZeroU64::new(268_435_488).unwrap(),
            when_full: WhenFull::Block,
        });
        let (mut sender, receiver) = config
            .build::<EventArray>(
                Some(dir.path().to_path_buf()),
                "repro".to_string(),
                Span::current(),
            )
            .await
            .expect("build buffer");

        let received = Arc::new(AtomicUsize::new(0));
        let service = CountingSink {
            received: Arc::clone(&received),
        };

        let batch_settings = BatchConfig::<RealtimeEventBasedDefaultBatchSettings>::default()
            .into_batcher_settings()
            .expect("batch settings");

        let driver = receiver
            .into_stream()
            .batched(batch_settings.as_byte_size_config())
            .map(|arrays: Vec<EventArray>| BatchRequest {
                arrays,
                metadata: RequestMetadata::default(),
            })
            .into_driver(service);
        let driver_task = tokio::spawn(driver.run());

        // Let the sink reach steady-state idle, parked on the empty buffer.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // A single record arrives while the sink is idle.
        sender
            .send(EventArray::from(Event::Log(LogEvent::from("lone"))), None)
            .await
            .expect("send should not fail");

        // It must be delivered to the service within a bounded time.
        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            received.load(Ordering::Relaxed),
            1,
            "a lone record arriving after the disk-buffered sink went idle must be delivered"
        );

        drop(sender);
        let _ = tokio::time::timeout(Duration::from_secs(2), driver_task).await;
    }
}
