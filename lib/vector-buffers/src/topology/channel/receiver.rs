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

/// Adapter for papering over various receiver backends.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ReceiverAdapter<T: Bufferable> {
    /// The in-memory channel buffer.
    InMemory(LimitedReceiver<T>),

    /// The disk v2 buffer.
    DiskV2(disk_v2::BufferReader<T, ProductionFilesystem>),
}

impl<T: Bufferable> From<LimitedReceiver<T>> for ReceiverAdapter<T> {
    fn from(v: LimitedReceiver<T>) -> Self {
        Self::InMemory(v)
    }
}

impl<T: Bufferable> From<disk_v2::BufferReader<T, ProductionFilesystem>> for ReceiverAdapter<T> {
    fn from(v: disk_v2::BufferReader<T, ProductionFilesystem>) -> Self {
        Self::DiskV2(v)
    }
}

impl<T> ReceiverAdapter<T>
where
    T: Bufferable,
{
    pub(crate) async fn next(&mut self) -> Option<T> {
        match self {
            ReceiverAdapter::InMemory(rx) => rx.next().await,
            ReceiverAdapter::DiskV2(reader) => loop {
                match reader.next().await {
                    Ok(result) => break result,
                    Err(e) => match e.as_recoverable_error() {
                        Some(re) => {
                            // If we've hit a recoverable error, we'll emit an event to indicate as much but we'll still
                            // keep trying to read the next available record.
                            emit(re);
                        }
                        None => panic!("Reader encountered unrecoverable error: {e:?}"),
                    },
                }
            },
        }
    }
}

/// A buffer receiver.
///
/// The receiver handles retrieving events from the buffer, regardless of the overall buffer configuration.
#[derive(Debug)]
pub struct BufferReceiver<T: Bufferable> {
    base: ReceiverAdapter<T>,
    instrumentation: Option<BufferUsageHandle>,
}

impl<T: Bufferable> BufferReceiver<T> {
    /// Creates a new [`BufferReceiver`] wrapping the given channel receiver.
    pub fn new(base: ReceiverAdapter<T>) -> Self {
        Self {
            base,
            instrumentation: None,
        }
    }

    /// Configures this receiver to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.instrumentation = Some(handle);
    }

    pub async fn next(&mut self) -> Option<T> {
        let item = self.base.next().await?;

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
