use std::{error, fmt, num::NonZeroUsize};

use bytes::{Buf, BufMut};
use vector_common::{
    byte_size_of::ByteSizeOf,
    finalization::{AddBatchNotifier, BatchNotifier},
};

use super::builder::standalone_memory_test;
use crate::{
    Bufferable, EventCount, WhenFull,
    buffer_usage_data::BufferUsageHandle,
    encoding::FixedEncodable,
    topology::channel::{BufferReceiver, BufferSender},
};

const SINGLE_VALUE_FLAG: u8 = 0;
const HEAP_ALLOCATED_VALUES_FLAG: u8 = 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Sample {
    SingleValue(u64),
    HeapAllocatedValues(Vec<u64>),
}

impl From<u64> for Sample {
    fn from(v: u64) -> Self {
        Self::SingleValue(v)
    }
}

impl From<Sample> for u64 {
    fn from(v: Sample) -> Self {
        match v {
            Sample::SingleValue(sv) => sv,
            Sample::HeapAllocatedValues(_) => {
                panic!("Cannot use this API with other enum states of this type.")
            }
        }
    }
}

impl Sample {
    pub fn new(value: u64) -> Self {
        Self::SingleValue(value)
    }

    pub fn new_with_heap_allocated_values(n: usize) -> Self {
        Self::HeapAllocatedValues(vec![0; n])
    }
}

impl AddBatchNotifier for Sample {
    fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        drop(batch); // We never check acknowledgements for this type
    }
}

impl ByteSizeOf for Sample {
    fn allocated_bytes(&self) -> usize {
        match self {
            Self::SingleValue(_) => 0,
            Self::HeapAllocatedValues(uints) => uints.len() * 8,
        }
    }
}

// Silly implementation of `Encodable` to fulfill `Bufferable` for our test buffer code.
impl FixedEncodable for Sample {
    type EncodeError = BasicError;
    type DecodeError = BasicError;

    // Serialization format:
    // - Encode type flag
    // - if single flag encode int value
    // - otherwise encode array length and encode array contents
    fn encode<B>(self, buffer: &mut B) -> Result<(), Self::EncodeError>
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            Self::SingleValue(uint) => {
                buffer.put_u8(SINGLE_VALUE_FLAG);
                buffer.put_u64(uint);
            }
            Self::HeapAllocatedValues(uints) => {
                buffer.put_u8(HEAP_ALLOCATED_VALUES_FLAG);
                // Prepend with array size
                buffer.put_u32(u32::try_from(uints.len()).unwrap());
                for v in uints {
                    buffer.put_u64(v);
                }
            }
        }
        Ok(())
    }

    fn decode<B>(mut buffer: B) -> Result<Self, Self::DecodeError>
    where
        B: Buf,
    {
        match buffer.get_u8() {
            SINGLE_VALUE_FLAG => Ok(Self::SingleValue(buffer.get_u64())),
            HEAP_ALLOCATED_VALUES_FLAG => {
                let length = buffer.get_u32();
                let values = (0..length).map(|_| buffer.get_u64()).collect();
                Ok(Self::HeapAllocatedValues(values))
            }
            _ => Err(BasicError(
                "Unknown serialization flag observed".to_string(),
            )),
        }
    }
}

impl EventCount for Sample {
    fn event_count(&self) -> usize {
        1
    }
}

#[derive(Debug)]
#[allow(dead_code)] // The inner _is_ read by the `Debug` impl, but that's ignored
pub struct BasicError(pub(crate) String);

impl fmt::Display for BasicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl error::Error for BasicError {}

/// Builds a buffer using in-memory channels.
pub(crate) fn build_buffer(
    capacity: usize,
    mode: WhenFull,
) -> (
    BufferSender<Sample>,
    BufferReceiver<Sample>,
    BufferUsageHandle,
) {
    let handle = BufferUsageHandle::noop();
    let (tx, rx) = standalone_memory_test(
        NonZeroUsize::new(capacity).expect("capacity must be nonzero"),
        mode,
        handle.clone(),
        None,
    );

    (tx, rx, handle)
}

/// Gets the current capacity of the underlying base channel of the given sender.
fn get_base_sender_capacity<T: Bufferable>(sender: &BufferSender<T>) -> Option<usize> {
    sender.get_base_ref().capacity()
}

/// Asserts the given sender's base capacity matches the given value.
#[allow(clippy::missing_panics_doc)]
pub fn assert_current_send_capacity<T>(
    sender: &mut BufferSender<T>,
    base_expected: Option<usize>,
) where
    T: Bufferable,
{
    assert_eq!(get_base_sender_capacity(sender), base_expected);
}
