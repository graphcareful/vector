use std::error::Error;

use async_trait::async_trait;

use crate::{
    Bufferable, WhenFull,
    buffer_usage_data::BufferUsageHandle,
    config::MemoryBufferSize,
    topology::{
        builder::IntoBuffer,
        channel::{BufferReceiver, BufferSender, limited},
    },
};

pub struct MemoryBuffer {
    capacity: MemoryBufferSize,
}

impl MemoryBuffer {
    pub fn new(capacity: MemoryBufferSize) -> Self {
        MemoryBuffer { capacity }
    }
}

#[async_trait]
impl<T> IntoBuffer<T> for MemoryBuffer
where
    T: Bufferable,
{
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), Box<dyn Error + Send + Sync>> {
        let (max_bytes, max_size) = match self.capacity {
            MemoryBufferSize::MaxEvents(max_events) => (None, Some(max_events.get())),
            MemoryBufferSize::MaxSize(max_size) => (None, Some(max_size.get())),
        };

        usage_handle.set_buffer_limits(max_bytes, max_size);

        let (tx, rx) = limited(self.capacity, None, None);
        Ok((
            BufferSender::memory(tx, WhenFull::default()),
            BufferReceiver::memory(rx),
        ))
    }
}
