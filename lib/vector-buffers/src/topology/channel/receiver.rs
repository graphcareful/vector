use std::{
    mem,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures::Stream;
use tokio_util::sync::ReusableBoxFuture;
use vector_common::internal_event::emit;

use super::limited_queue::LimitedReceiver;
use crate::{
    Bufferable,
    buffer_usage_data::BufferUsageHandle,
    variants::disk_v2::{self, ProductionFilesystem},
};

/// Internal backend dispatch for buffer receivers.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum ReceiverBackend<T: Bufferable> {
    InMemory(LimitedReceiver<T>),
    DiskV2(disk_v2::BufferReader<T, ProductionFilesystem>),
}

/// A buffer receiver.
///
/// The receiver handles retrieving events from the buffer, regardless of the overall buffer configuration.
#[derive(Debug)]
pub struct BufferReceiver<T: Bufferable> {
    backend: ReceiverBackend<T>,
    instrumentation: Option<BufferUsageHandle>,
}

impl<T: Bufferable> BufferReceiver<T> {
    /// Creates a new [`BufferReceiver`] backed by an in-memory channel.
    pub fn memory(receiver: LimitedReceiver<T>) -> Self {
        Self {
            backend: ReceiverBackend::InMemory(receiver),
            instrumentation: None,
        }
    }

    /// Creates a new [`BufferReceiver`] backed by a disk v2 buffer.
    pub fn disk_v2(reader: disk_v2::BufferReader<T, ProductionFilesystem>) -> Self {
        Self {
            backend: ReceiverBackend::DiskV2(reader),
            instrumentation: None,
        }
    }

    /// Configures this receiver to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.instrumentation = Some(handle);
    }

    pub async fn next(&mut self) -> Option<T> {
        let item = match &mut self.backend {
            ReceiverBackend::InMemory(rx) => rx.next().await?,
            ReceiverBackend::DiskV2(reader) => loop {
                match reader.next().await {
                    Ok(Some(item)) => break item,
                    Ok(None) => return None,
                    Err(e) => match e.as_recoverable_error() {
                        Some(re) => {
                            emit(re);
                        }
                        None => panic!("Reader encountered unrecoverable error: {e:?}"),
                    },
                }
            },
        };

        if let Some(handle) = self.instrumentation.as_ref() {
            handle.increment_sent_event_count_and_byte_size(
                item.event_count() as u64,
                item.size_of() as u64,
            );
        }

        Some(item)
    }

    pub fn into_stream(self) -> BufferReceiverStream<T> {
        BufferReceiverStream::new(self)
    }
}

#[allow(clippy::large_enum_variant)]
enum StreamState<T: Bufferable> {
    Idle(BufferReceiver<T>),
    Polling,
    Closed,
}

pub struct BufferReceiverStream<T: Bufferable> {
    state: StreamState<T>,
    recv_fut: ReusableBoxFuture<'static, (Option<T>, BufferReceiver<T>)>,
}

impl<T: Bufferable> BufferReceiverStream<T> {
    pub fn new(receiver: BufferReceiver<T>) -> Self {
        Self {
            state: StreamState::Idle(receiver),
            recv_fut: ReusableBoxFuture::new(make_recv_future(None)),
        }
    }
}

impl<T: Bufferable> Stream for BufferReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, StreamState::Polling) {
                s @ StreamState::Closed => {
                    self.state = s;
                    return Poll::Ready(None);
                }
                StreamState::Idle(receiver) => {
                    self.recv_fut.set(make_recv_future(Some(receiver)));
                }
                StreamState::Polling => {
                    let (result, receiver) = ready!(self.recv_fut.poll(cx));
                    self.state = if result.is_none() {
                        StreamState::Closed
                    } else {
                        StreamState::Idle(receiver)
                    };

                    return Poll::Ready(result);
                }
            }
        }
    }
}

async fn make_recv_future<T: Bufferable>(
    receiver: Option<BufferReceiver<T>>,
) -> (Option<T>, BufferReceiver<T>) {
    match receiver {
        None => panic!("invalid to poll future in uninitialized state"),
        Some(mut receiver) => {
            let result = receiver.next().await;
            (result, receiver)
        }
    }
}
