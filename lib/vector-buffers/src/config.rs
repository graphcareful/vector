use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Deserializer, Serialize, de};
use snafu::Snafu;
use tracing::Span;
use vector_common::{config::ComponentKey, finalization::Finalizable};
use vector_config::configurable_component;

use crate::{
    Bufferable, WhenFull,
    buffer_usage_data::BufferUsage,
    topology::{
        builder::TopologyError,
        channel::{BufferReceiver, BufferSender},
    },
    variants::{DiskV2Buffer, MemoryBuffer},
};

#[derive(Debug, Snafu)]
pub enum BufferBuildError {
    #[snafu(display("the configured buffer type requires `data_dir` be specified"))]
    RequiresDataDir,
    #[snafu(display("error occurred when building buffer: {}", source))]
    FailedToBuildTopology { source: TopologyError },
    #[snafu(display("`max_events` must be greater than zero"))]
    InvalidMaxEvents,
}

#[derive(Deserialize, Serialize)]
enum BufferTypeKind {
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "disk")]
    DiskV2,
}

const ALL_FIELDS: [&str; 4] = ["type", "max_events", "max_size", "when_full"];

struct BufferTypeVisitor;

impl BufferTypeVisitor {
    fn visit_map_impl<'de, A>(mut map: A) -> Result<BufferType, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut kind: Option<BufferTypeKind> = None;
        let mut max_events: Option<NonZeroUsize> = None;
        let mut max_size: Option<NonZeroU64> = None;
        let mut when_full: Option<WhenFull> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    if kind.is_some() {
                        return Err(de::Error::duplicate_field("type"));
                    }
                    kind = Some(map.next_value()?);
                }
                "max_events" => {
                    if max_events.is_some() {
                        return Err(de::Error::duplicate_field("max_events"));
                    }
                    max_events = Some(map.next_value()?);
                }
                "max_size" => {
                    if max_size.is_some() {
                        return Err(de::Error::duplicate_field("max_size"));
                    }
                    max_size = Some(map.next_value()?);
                }
                "when_full" => {
                    if when_full.is_some() {
                        return Err(de::Error::duplicate_field("when_full"));
                    }
                    when_full = Some(map.next_value()?);
                }
                other => {
                    return Err(de::Error::unknown_field(other, &ALL_FIELDS));
                }
            }
        }
        let kind = kind.unwrap_or(BufferTypeKind::Memory);
        let when_full = when_full.unwrap_or_default();
        match kind {
            BufferTypeKind::Memory => {
                let size = match (max_events, max_size) {
                    (Some(_), Some(_)) => {
                        return Err(de::Error::unknown_field(
                            "max_events",
                            &["type", "max_size", "when_full"],
                        ));
                    }
                    (_, Some(max_size)) => {
                        if let Ok(bounded_max_bytes) = usize::try_from(max_size.get()) {
                            MemoryBufferSize::MaxSize(NonZeroUsize::new(bounded_max_bytes).unwrap())
                        } else {
                            return Err(de::Error::invalid_value(
                                de::Unexpected::Unsigned(max_size.into()),
                                &format!(
                                    "Value for max_bytes must be a positive integer <= {}",
                                    usize::MAX
                                )
                                .as_str(),
                            ));
                        }
                    }
                    _ => MemoryBufferSize::MaxEvents(
                        max_events.unwrap_or_else(memory_buffer_default_max_events),
                    ),
                };
                Ok(BufferType::Memory { size, when_full })
            }
            BufferTypeKind::DiskV2 => {
                if max_events.is_some() {
                    return Err(de::Error::unknown_field(
                        "max_events",
                        &["type", "max_size", "when_full"],
                    ));
                }
                Ok(BufferType::DiskV2 {
                    max_size: max_size.ok_or_else(|| de::Error::missing_field("max_size"))?,
                    when_full,
                })
            }
        }
    }
}

impl<'de> de::Visitor<'de> for BufferTypeVisitor {
    type Value = BufferType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("enum BufferType")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        BufferTypeVisitor::visit_map_impl(map)
    }
}

impl<'de> Deserialize<'de> for BufferType {
    fn deserialize<D>(deserializer: D) -> Result<BufferType, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BufferTypeVisitor)
    }
}

pub const fn memory_buffer_default_max_events() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(500) }
}

/// Disk usage configuration for disk-backed buffers.
#[derive(Debug)]
pub struct DiskUsage {
    id: ComponentKey,
    data_dir: PathBuf,
    max_size: NonZeroU64,
}

impl DiskUsage {
    /// Creates a new `DiskUsage` with the given usage configuration.
    pub fn new(id: ComponentKey, data_dir: PathBuf, max_size: NonZeroU64) -> Self {
        Self {
            id,
            data_dir,
            max_size,
        }
    }

    /// Gets the component key for the component this buffer is attached to.
    pub fn id(&self) -> &ComponentKey {
        &self.id
    }

    /// Gets the maximum size, in bytes, that this buffer can consume on disk.
    pub fn max_size(&self) -> u64 {
        self.max_size.get()
    }

    /// Gets the data directory path that this buffer will store its files on disk.
    pub fn data_dir(&self) -> &Path {
        self.data_dir.as_path()
    }
}

/// Enumeration to define exactly what terms the bounds of the buffer is expressed in: length, or
/// `byte_size`.
#[configurable_component(no_deser)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryBufferSize {
    /// The maximum number of events allowed in the buffer.
    MaxEvents(#[serde(default = "memory_buffer_default_max_events")] NonZeroUsize),

    // Doc string is duplicated here as a workaround due to a name collision with the `max_size`
    // field with the DiskV2 variant of `BufferType`.
    /// The maximum allowed amount of allocated memory the buffer can hold.
    ///
    /// If `type = "disk"` then must be at least ~256 megabytes (268435488 bytes).
    MaxSize(#[configurable(metadata(docs::type_unit = "bytes"))] NonZeroUsize),
}

/// A specific type of buffer stage.
#[configurable_component(no_deser)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
#[configurable(metadata(docs::enum_tag_description = "The type of buffer to use."))]
pub enum BufferType {
    /// A buffer stage backed by an in-memory channel provided by `tokio`.
    ///
    /// This is more performant, but less durable. Data will be lost if Vector is restarted
    /// forcefully or crashes.
    #[configurable(title = "Events are buffered in memory.")]
    Memory {
        /// The terms around how to express buffering limits, can be in size or `bytes_size`.
        #[serde(flatten)]
        size: MemoryBufferSize,

        #[configurable(derived)]
        #[serde(default)]
        when_full: WhenFull,
    },

    /// A buffer stage backed by disk.
    ///
    /// This is less performant, but more durable. Data that has been synchronized to disk will not
    /// be lost if Vector is restarted forcefully or crashes.
    ///
    /// Data is synchronized to disk every 500ms.
    #[configurable(title = "Events are buffered on disk.")]
    #[serde(rename = "disk")]
    DiskV2 {
        /// The maximum size of the buffer on disk.
        ///
        /// Must be at least ~256 megabytes (268435488 bytes).
        #[configurable(
            validation(range(min = 268435488)),
            metadata(docs::type_unit = "bytes")
        )]
        max_size: NonZeroU64,

        #[configurable(derived)]
        #[serde(default)]
        when_full: WhenFull,
    },
}

impl BufferType {
    /// Gets the metadata around disk usage by the buffer, if supported.
    ///
    /// For buffer types that write to disk, `Some(value)` is returned with their usage metadata,
    /// such as maximum size and data directory path.
    ///
    /// Otherwise, `None` is returned.
    pub fn disk_usage(
        &self,
        global_data_dir: Option<PathBuf>,
        id: &ComponentKey,
    ) -> Option<DiskUsage> {
        // All disk-backed buffers require the global data directory to be specified, and
        // non-disk-backed buffers do not require it to be set... so if it's not set here, we ignore
        // it because either:
        // - it's a non-disk-backed buffer, in which case we can just ignore, or
        // - this method is being called at a point before we actually check that a global data
        //   directory is specified because we have a disk buffer present
        //
        // Since we're not able to emit/surface errors about a lack of a global data directory from
        // where this method is called, we simply return `None` to let it reach the code that _does_
        // emit/surface those errors... and once those errors are fixed, this code can return valid
        // disk usage information, which will then be validated and emit any errors for _that_
        // aspect.
        match global_data_dir {
            None => None,
            Some(global_data_dir) => match self {
                Self::Memory { .. } => None,
                Self::DiskV2 { max_size, .. } => {
                    let data_dir = crate::variants::disk_v2::get_disk_v2_data_dir_path(
                        &global_data_dir,
                        id.id(),
                    );

                    Some(DiskUsage::new(id.clone(), data_dir, *max_size))
                }
            },
        }
    }

}

/// Buffer configuration.
///
/// Buffers are configured for sinks, where backpressure from the sink can be handled by the
/// buffer.  This allows absorbing temporary load, or potentially adding write-ahead-log behavior
/// to a sink to increase the durability of a given Vector pipeline.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[configurable(
    title = "Configures the buffering behavior for this sink.",
    description = r#"More information about the individual buffer types, and buffer behavior, can be found in the
[Buffering Model][buffering_model] section.

[buffering_model]: /docs/architecture/buffering-model/"#
)]
pub struct BufferConfig(pub BufferType);

impl Default for BufferConfig {
    fn default() -> Self {
        Self(BufferType::Memory {
            size: MemoryBufferSize::MaxEvents(memory_buffer_default_max_events()),
            when_full: WhenFull::default(),
        })
    }
}

impl BufferConfig {
    /// Gets the buffer type for this configuration.
    pub fn buffer_type(&self) -> &BufferType {
        &self.0
    }

    /// Builds the buffer components represented by this configuration.
    ///
    /// The caller gets back a `Sink` and `Stream` implementation that represent a way to push items
    /// into the buffer, as well as pop items out of the buffer, respectively.
    ///
    /// # Errors
    ///
    /// If a disk buffer stage is configured and the data directory provided is `None`, an error
    /// variant will be thrown.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn build<T>(
        &self,
        data_dir: Option<PathBuf>,
        buffer_id: String,
        span: Span,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), BufferBuildError>
    where
        T: Bufferable + Clone + Finalizable,
    {
        use crate::topology::builder::IntoBuffer;

        let mut buffer_usage = BufferUsage::from_span(span.clone());
        let usage_handle = buffer_usage.add_stage(0);

        let (stage, when_full): (Box<dyn IntoBuffer<T>>, WhenFull) = match self.0 {
            BufferType::Memory { size, when_full } => {
                (Box::new(MemoryBuffer::new(size)), when_full)
            }
            BufferType::DiskV2 {
                when_full,
                max_size,
            } => {
                let data_dir = data_dir.ok_or(BufferBuildError::RequiresDataDir)?;
                (
                    Box::new(DiskV2Buffer::new(buffer_id.clone(), data_dir, max_size)),
                    when_full,
                )
            }
        };

        let provides_instrumentation = stage.provides_instrumentation();
        let (sender, receiver) = stage
            .into_buffer_parts(usage_handle.clone())
            .await
            .map_err(|source| BufferBuildError::FailedToBuildTopology {
                source: TopologyError::FailedToBuildStage { source },
            })?;

        let mut sender = BufferSender::new(sender, when_full);
        let mut receiver = BufferReceiver::new(receiver);

        sender.with_send_duration_instrumentation(0, &span);
        if !provides_instrumentation {
            sender.with_usage_instrumentation(usage_handle.clone());
            receiver.with_usage_instrumentation(usage_handle);
        }

        buffer_usage.install(buffer_id.as_str());

        Ok((sender, receiver))
    }
}

#[cfg(test)]
mod test {
    use std::num::{NonZeroU64, NonZeroUsize};

    use crate::{BufferConfig, BufferType, MemoryBufferSize, WhenFull};

    fn check_single_stage(source: &str, expected: BufferType) {
        let config: BufferConfig = serde_yaml::from_str(source).unwrap();
        assert_eq!(config.buffer_type(), &expected);
    }

    #[test]
    fn parse_without_type_tag() {
        check_single_stage(
            r"
          max_events: 100
          ",
            BufferType::Memory {
                size: MemoryBufferSize::MaxEvents(NonZeroUsize::new(100).unwrap()),
                when_full: WhenFull::Block,
            },
        );
    }

    #[test]
    fn parse_memory_with_byte_size_option() {
        check_single_stage(
            r"
        max_size: 4096
        ",
            BufferType::Memory {
                size: MemoryBufferSize::MaxSize(NonZeroUsize::new(4096).unwrap()),
                when_full: WhenFull::Block,
            },
        );
    }

    #[test]
    fn ensure_field_defaults_for_all_types() {
        check_single_stage(
            r"
          type: memory
          ",
            BufferType::Memory {
                size: MemoryBufferSize::MaxEvents(NonZeroUsize::new(500).unwrap()),
                when_full: WhenFull::Block,
            },
        );

        check_single_stage(
            r"
          type: memory
          max_events: 100
          ",
            BufferType::Memory {
                size: MemoryBufferSize::MaxEvents(NonZeroUsize::new(100).unwrap()),
                when_full: WhenFull::Block,
            },
        );

        check_single_stage(
            r"
          type: memory
          when_full: drop_newest
          ",
            BufferType::Memory {
                size: MemoryBufferSize::MaxEvents(NonZeroUsize::new(500).unwrap()),
                when_full: WhenFull::DropNewest,
            },
        );

        check_single_stage(
            r"
          type: disk
          max_size: 1024
          ",
            BufferType::DiskV2 {
                max_size: NonZeroU64::new(1024).unwrap(),
                when_full: WhenFull::Block,
            },
        );
    }
}
