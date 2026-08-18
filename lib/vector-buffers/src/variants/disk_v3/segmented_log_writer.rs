use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use snafu::Snafu;
use tokio::{fs::OpenOptions, io::AsyncWrite};

use super::{
    frame::PreparedFrame,
    position::Position,
    segmented_log_reader::{SegmentedLogReader, SegmentedLogReaderError},
    writable_segment::{
        SealedSegment, SegmentEnd, WritableSegment, WritableSegmentError, segment_file_name,
    },
};

pub(crate) const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(500);

/// Storage operations used by the single writer actor.
pub(crate) trait SegmentStorage: Clone + Send + Sync + 'static {
    type File: AsyncWrite + Send + Sync + Unpin + 'static;

    async fn create_segment(&self, base_offset: u64) -> io::Result<Self::File>;
    async fn sync_segment(&self, base_offset: u64) -> io::Result<()>;
    async fn sync_directory(&self) -> io::Result<()>;
}

/// Production segment storage rooted at one exclusively owned buffer
/// directory.
#[derive(Clone, Debug)]
pub(crate) struct FilesystemSegmentStorage {
    directory: Arc<PathBuf>,
}

impl FilesystemSegmentStorage {
    pub(crate) fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: Arc::new(directory.into()),
        }
    }

    #[must_use]
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    fn segment_path(&self, base_offset: u64) -> PathBuf {
        self.directory.join(segment_file_name(base_offset))
    }

    async fn open_segment_for_append(
        &self,
        base_offset: u64,
        expected_len: u64,
    ) -> io::Result<tokio::fs::File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(self.segment_path(base_offset))
            .await?;
        let actual_len = file.metadata().await?.len();
        if actual_len != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected {base_offset}.log to contain {expected_len} bytes, but it contains {actual_len}"
                ),
            ));
        }
        Ok(file)
    }
}

impl SegmentStorage for FilesystemSegmentStorage {
    type File = tokio::fs::File;

    async fn create_segment(&self, base_offset: u64) -> io::Result<Self::File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.segment_path(base_offset))
            .await
    }

    async fn sync_segment(&self, base_offset: u64) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.segment_path(base_offset))
            .await?;
        file.sync_all().await
    }

    async fn sync_directory(&self) -> io::Result<()> {
        let directory = Arc::clone(&self.directory);

        #[cfg(unix)]
        {
            let directory = tokio::fs::File::open(directory.as_ref()).await?;
            directory.sync_all().await
        }

        #[cfg(windows)]
        {
            tokio::task::spawn_blocking(move || {
                use std::os::windows::fs::OpenOptionsExt;

                const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

                std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                    .open(directory.as_ref())?
                    .sync_all()
            })
            .await
            .map_err(io::Error::other)?
        }

        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "directory synchronization is unsupported on this platform",
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentedLogWriterConfig {
    pub(crate) segment_size: u64,
    pub(crate) batch_size: usize,
    pub(crate) sync_interval: Duration,
}

impl SegmentedLogWriterConfig {
    pub(crate) fn validate(self) -> Result<Self, SegmentedLogWriterError> {
        if self.segment_size == 0 || self.batch_size == 0 {
            return Err(SegmentedLogWriterError::InvalidConfig {
                segment_size: self.segment_size,
                batch_size: self.batch_size,
            });
        }

        Ok(self)
    }
}

#[derive(Debug)]
struct AggregationBuffer {
    bytes: BytesMut,
}

impl AggregationBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: BytesMut::with_capacity(capacity),
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn append(&mut self, bytes: &Bytes) {
        self.bytes.put_slice(bytes);
    }

    fn take(&mut self, replacement_capacity: usize) -> Bytes {
        std::mem::replace(
            &mut self.bytes,
            BytesMut::with_capacity(replacement_capacity),
        )
        .freeze()
    }
}

/// Single-writer append engine for complete encoded frames.
///
/// The owning actor must await each method to completion. Producer
/// cancellation cannot cancel these operations because producers communicate
/// with the actor through the MPSC command queue.
pub(crate) struct SegmentedLogWriter<S>
where
    S: SegmentStorage,
{
    storage: S,
    active_segment: WritableSegment<S::File>,
    committed: Position,
    data_synced: Position,
    next_record_id: u64,
    batch: AggregationBuffer,
    last_sync: Instant,
    config: SegmentedLogWriterConfig,
}

impl<S> SegmentedLogWriter<S>
where
    S: SegmentStorage,
{
    /// Creates the first empty segment with `create_new` semantics.
    pub(crate) async fn create(
        storage: S,
        first_record_id: u64,
        config: SegmentedLogWriterConfig,
    ) -> Result<Self, SegmentedLogWriterError> {
        let config = config.validate()?;
        let position = Position::at_segment_start(first_record_id);
        let file = storage
            .create_segment(first_record_id)
            .await
            .map_err(|source| SegmentedLogWriterError::Create {
                segment_base_offset: first_record_id,
                source,
            })?;
        storage
            .sync_directory()
            .await
            .map_err(|source| SegmentedLogWriterError::SyncDirectory { source })?;
        let active_segment = WritableSegment::new(file, first_record_id, config.segment_size)
            .map_err(|source| SegmentedLogWriterError::Segment { source })?;
        Ok(Self {
            storage,
            active_segment,
            committed: position,
            data_synced: position,
            next_record_id: first_record_id,
            batch: AggregationBuffer::new(config.batch_size),
            last_sync: Instant::now(),
            config,
        })
    }

    #[must_use]
    pub(crate) const fn committed(&self) -> Position {
        self.committed
    }

    #[must_use]
    pub(crate) const fn data_synced(&self) -> Position {
        self.data_synced
    }

    /// Accepts one complete frame, rotating first when it cannot fit in the
    /// active segment. Reaching the batch byte limit publishes the complete batch.
    pub(crate) async fn append(
        &mut self,
        frame: PreparedFrame,
    ) -> Result<Option<SealedSegment>, SegmentedLogWriterError> {
        self.validate_frame(&frame)?;

        let frame_len = u64::try_from(frame.bytes().len())
            .map_err(|_| SegmentedLogWriterError::PositionOverflow)?;
        let mut sealed_segment = None;
        if frame_len > self.remaining_segment_capacity()? {
            sealed_segment = Some(self.rotate().await?);
        }

        let end = self
            .pending_end()?
            .checked_advance(frame_len, frame.event_count().get())
            .ok_or(SegmentedLogWriterError::PositionOverflow)?;
        self.batch.append(frame.bytes());
        self.next_record_id = end.next_record_id;

        if self.batch.len() >= self.config.batch_size {
            self.publish().await?;
        }

        Ok(sealed_segment)
    }

    /// Publishes every buffered frame and opportunistically synchronizes data
    /// when the configured interval has elapsed.
    ///
    /// The surrounding buffer adapter must drive this by forwarding its
    /// `BufferSender::flush` call to the writer owner.
    pub(crate) async fn flush(&mut self) -> Result<Position, SegmentedLogWriterError> {
        self.publish().await?;
        if self.last_sync.elapsed() >= self.config.sync_interval {
            self.sync_committed().await?;
        }
        Ok(self.committed)
    }

    /// Publishes and unconditionally synchronizes all buffered data.
    /// Shutdown uses this before synchronizing its checkpoint.
    pub(crate) async fn sync_all(&mut self) -> Result<Position, SegmentedLogWriterError> {
        self.publish().await?;
        self.sync_committed().await?;
        Ok(self.data_synced)
    }

    fn validate_frame(&self, frame: &PreparedFrame) -> Result<(), SegmentedLogWriterError> {
        if frame.bytes().is_empty() {
            return Err(SegmentedLogWriterError::EmptyFrame);
        }
        let frame_len = u64::try_from(frame.bytes().len())
            .map_err(|_| SegmentedLogWriterError::PositionOverflow)?;
        if frame_len > self.config.segment_size {
            return Err(SegmentedLogWriterError::FrameTooLarge {
                frame_len,
                segment_size: self.config.segment_size,
            });
        }
        Ok(())
    }

    fn remaining_segment_capacity(&self) -> Result<u64, SegmentedLogWriterError> {
        let occupied = self.pending_end()?.segment_byte_offset;
        self.config
            .segment_size
            .checked_sub(occupied)
            .ok_or_else(|| SegmentedLogWriterError::PositionInvariant {
                reason: format!(
                    "{}.log has {occupied} occupied bytes but its target size is {} bytes",
                    self.active_segment.base_offset(),
                    self.config.segment_size
                ),
            })
    }

    fn pending_end(&self) -> Result<Position, SegmentedLogWriterError> {
        let segment_base_offset = self.active_segment.base_offset();
        let buffered_len = u64::try_from(self.batch.len())
            .map_err(|_| SegmentedLogWriterError::PositionOverflow)?;
        let segment_byte_offset = self
            .active_segment
            .write_offset()
            .checked_add(buffered_len)
            .ok_or(SegmentedLogWriterError::PositionOverflow)?;

        Ok(Position::new(
            segment_base_offset,
            segment_byte_offset,
            self.next_record_id,
        ))
    }

    async fn publish(&mut self) -> Result<(), SegmentedLogWriterError> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let end = self.pending_end()?;
        // Once I/O starts, an error is terminal because the file may contain a
        // partial batch. The batch is therefore moved into this operation and
        // is not restored for an unsafe in-place retry.
        let batch = self.batch.take(self.config.batch_size);
        let segment_base_offset = self.active_segment.base_offset();
        let segment_byte_offset = self.active_segment.write_offset();
        self.active_segment
            .write_all(&batch)
            .await
            .map_err(|source| SegmentedLogWriterError::Write {
                segment_base_offset,
                segment_byte_offset,
                source,
            })?;
        self.active_segment
            .flush()
            .await
            .map_err(|source| SegmentedLogWriterError::Flush {
                segment_base_offset,
                segment_byte_offset,
                source,
            })?;
        self.committed = end;
        Ok(())
    }

    async fn rotate(&mut self) -> Result<SealedSegment, SegmentedLogWriterError> {
        self.publish().await?;
        if self.committed != self.pending_end()? {
            return Err(SegmentedLogWriterError::PositionInvariant {
                reason: "rotation started before all buffered frames were published".to_owned(),
            });
        }
        self.sync_committed().await?;

        let start = Position::at_segment_start(self.next_record_id);
        let file = self
            .storage
            .create_segment(start.segment_base_offset)
            .await
            .map_err(|source| SegmentedLogWriterError::Create {
                segment_base_offset: start.segment_base_offset,
                source,
            })?;
        self.storage
            .sync_directory()
            .await
            .map_err(|source| SegmentedLogWriterError::SyncDirectory { source })?;
        let next_segment =
            WritableSegment::new(file, start.segment_base_offset, self.config.segment_size)
                .map_err(|source| SegmentedLogWriterError::Segment { source })?;
        let previous_segment = std::mem::replace(&mut self.active_segment, next_segment);
        let sealed = previous_segment
            .into_sealed(SegmentEnd::new(
                self.committed.segment_byte_offset,
                self.committed.next_record_id,
            ))
            .map_err(|source| SegmentedLogWriterError::Segment { source })?;
        Ok(sealed)
    }

    async fn sync_data(&mut self, through: Position) -> Result<(), SegmentedLogWriterError> {
        if through.next_record_id <= self.data_synced.next_record_id {
            return Ok(());
        }

        let segment_base_offset = self.active_segment.base_offset();
        self.storage
            .sync_segment(segment_base_offset)
            .await
            .map_err(|source| SegmentedLogWriterError::SyncSegment {
                segment_base_offset,
                source,
            })?;

        self.data_synced = through;
        Ok(())
    }

    async fn sync_committed(&mut self) -> Result<(), SegmentedLogWriterError> {
        self.sync_data(self.committed).await?;
        self.last_sync = Instant::now();
        Ok(())
    }
}

impl SegmentedLogWriter<FilesystemSegmentStorage> {
    /// Opens the validated tail of an existing log or creates its first segment.
    pub(crate) async fn open_or_create(
        storage: FilesystemSegmentStorage,
        initial_record_id: u64,
        max_frame_len: usize,
        config: SegmentedLogWriterConfig,
    ) -> Result<Self, SegmentedLogWriterError> {
        tokio::fs::create_dir_all(storage.directory())
            .await
            .map_err(|source| SegmentedLogWriterError::CreateDirectory { source })?;

        let Some(tail) = SegmentedLogReader::recover_tail(storage.directory(), max_frame_len)
            .await
            .map_err(|source| SegmentedLogWriterError::Recover { source })?
        else {
            return Self::create(storage, initial_record_id, config).await;
        };
        let position = tail.position();
        Self::resume(storage, position, config).await
    }

    /// Reopens the active segment at a previously validated frame boundary.
    async fn resume(
        storage: FilesystemSegmentStorage,
        position: Position,
        config: SegmentedLogWriterConfig,
    ) -> Result<Self, SegmentedLogWriterError> {
        let config = config.validate()?;
        let segment_base_offset = position.segment_base_offset();
        let segment_byte_offset = position.segment_byte_offset();
        let file = storage
            .open_segment_for_append(segment_base_offset, segment_byte_offset)
            .await
            .map_err(|source| SegmentedLogWriterError::Open {
                segment_base_offset,
                source,
            })?;
        let active_segment = WritableSegment::resume(
            file,
            segment_base_offset,
            segment_byte_offset,
            config.segment_size,
        )
        .map_err(|source| SegmentedLogWriterError::Segment { source })?;

        Ok(Self {
            storage,
            active_segment,
            committed: position,
            data_synced: position,
            next_record_id: position.next_record_id(),
            batch: AggregationBuffer::new(config.batch_size),
            last_sync: Instant::now(),
            config,
        })
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum SegmentedLogWriterError {
    #[snafu(display(
        "segment and batch byte limits must be nonzero (segment={segment_size}, batch_bytes={batch_size})"
    ))]
    InvalidConfig {
        segment_size: u64,
        batch_size: usize,
    },

    #[snafu(display("failed to create the segment directory: {source}"))]
    CreateDirectory { source: io::Error },

    #[snafu(display("failed to create segment {segment_base_offset}.log: {source}"))]
    Create {
        segment_base_offset: u64,
        source: io::Error,
    },

    #[snafu(display("failed to reopen segment {segment_base_offset}.log for append: {source}"))]
    Open {
        segment_base_offset: u64,
        source: io::Error,
    },

    #[snafu(display("failed to recover the existing segmented log: {source}"))]
    Recover { source: SegmentedLogReaderError },

    #[snafu(display("writable segment error: {source}"))]
    Segment { source: WritableSegmentError },

    #[snafu(display("an encoded frame must not be empty"))]
    EmptyFrame,

    #[snafu(display(
        "encoded frame of {frame_len} bytes exceeds the segment size of {segment_size} bytes"
    ))]
    FrameTooLarge { frame_len: u64, segment_size: u64 },

    #[snafu(display("record or byte position overflowed"))]
    PositionOverflow,

    #[snafu(display("position invariant failed: {reason}"))]
    PositionInvariant { reason: String },

    #[snafu(display(
        "write failed in {segment_base_offset}.log at byte {segment_byte_offset}: {source}"
    ))]
    Write {
        segment_base_offset: u64,
        segment_byte_offset: u64,
        source: io::Error,
    },

    #[snafu(display(
        "flush failed in {segment_base_offset}.log at byte {segment_byte_offset}: {source}"
    ))]
    Flush {
        segment_base_offset: u64,
        segment_byte_offset: u64,
        source: io::Error,
    },

    #[snafu(display("failed to synchronize {segment_base_offset}.log: {source}"))]
    SyncSegment {
        segment_base_offset: u64,
        source: io::Error,
    },

    #[snafu(display("failed to synchronize the buffer directory: {source}"))]
    SyncDirectory { source: io::Error },
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        num::NonZeroU32,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use tokio::io::AsyncWrite;

    use super::*;
    use crate::variants::disk_v3::frame::encode_frame;

    #[derive(Default)]
    struct TestStorageState {
        files: Mutex<BTreeMap<u64, Arc<Mutex<Vec<u8>>>>>,
        synced: Mutex<Vec<u64>>,
        sync_failure: Mutex<Option<u64>>,
        directory_syncs: Mutex<usize>,
    }

    #[derive(Clone)]
    struct TestStorage {
        state: Arc<TestStorageState>,
        max_write: usize,
    }

    impl TestStorage {
        fn new(max_write: usize) -> Self {
            assert!(max_write > 0);
            Self {
                state: Arc::new(TestStorageState::default()),
                max_write,
            }
        }

        fn bytes(&self, start: u64) -> Vec<u8> {
            self.state
                .files
                .lock()
                .unwrap()
                .get(&start)
                .unwrap()
                .lock()
                .unwrap()
                .clone()
        }

        fn fail_next_segment_sync(&self, start: u64) {
            *self.state.sync_failure.lock().unwrap() = Some(start);
        }
    }

    struct TestFile {
        bytes: Arc<Mutex<Vec<u8>>>,
        max_write: usize,
    }

    impl AsyncWrite for TestFile {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let written = bytes.len().min(self.max_write);
            self.bytes
                .lock()
                .unwrap()
                .extend_from_slice(&bytes[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl SegmentStorage for TestStorage {
        type File = TestFile;

        async fn create_segment(&self, start: u64) -> io::Result<Self::File> {
            let mut files = self.state.files.lock().unwrap();
            if files.contains_key(&start) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "segment already exists",
                ));
            }
            let bytes = Arc::new(Mutex::new(Vec::new()));
            files.insert(start, Arc::clone(&bytes));
            Ok(TestFile {
                bytes,
                max_write: self.max_write,
            })
        }

        async fn sync_segment(&self, start: u64) -> io::Result<()> {
            let mut sync_failure = self.state.sync_failure.lock().unwrap();
            if *sync_failure == Some(start) {
                *sync_failure = None;
                return Err(io::Error::other("injected segment sync failure"));
            }
            drop(sync_failure);

            self.state.synced.lock().unwrap().push(start);
            Ok(())
        }

        async fn sync_directory(&self) -> io::Result<()> {
            *self.state.directory_syncs.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn config(segment_size: u64) -> SegmentedLogWriterConfig {
        SegmentedLogWriterConfig {
            segment_size,
            batch_size: 1024,
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }

    #[tokio::test]
    async fn reaching_the_batch_byte_limit_publishes_immediately() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer = SegmentedLogWriter::create(
            storage.clone(),
            10,
            SegmentedLogWriterConfig {
                segment_size: 64,
                batch_size: 6,
                sync_interval: DEFAULT_SYNC_INTERVAL,
            },
        )
        .await
        .unwrap();

        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"abc")))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"def")))
                .await
                .unwrap()
                .is_none()
        );

        assert_eq!(storage.bytes(10), b"abcdef");
        assert_eq!(writer.committed().segment_base_offset(), 10);
        assert_eq!(writer.committed().segment_byte_offset(), 6);
        assert_eq!(writer.committed().next_record_id(), 12);
    }

    #[tokio::test]
    async fn manual_flush_publishes_a_partial_batch() {
        let storage = TestStorage::new(2);
        let mut writer = SegmentedLogWriter::create(storage.clone(), 0, config(64))
            .await
            .unwrap();
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(
                    b"abcdef"
                )))
                .await
                .unwrap()
                .is_none()
        );

        assert!(storage.bytes(0).is_empty());
        assert_eq!(writer.committed().segment_byte_offset(), 0);

        let committed = writer.flush().await.unwrap();
        assert_eq!(committed.segment_byte_offset(), 6);
        assert_eq!(committed.next_record_id(), 1);
        assert_eq!(storage.bytes(0), b"abcdef");
        assert!(storage.state.synced.lock().unwrap().is_empty());
        assert_eq!(writer.data_synced().segment_byte_offset(), 0);
        assert_eq!(*storage.state.directory_syncs.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn frame_event_count_advances_the_next_record_id() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer = SegmentedLogWriter::create(storage, 10, config(1024))
            .await
            .unwrap();
        let frame = encode_frame(10, NonZeroU32::new(3).unwrap(), 0, b"payload", 1024).unwrap();

        assert!(writer.append(frame).await.unwrap().is_none());
        let committed = writer.flush().await.unwrap();

        assert_eq!(committed.next_record_id(), 13);
    }

    #[tokio::test]
    async fn rotates_before_accepting_a_frame_that_does_not_fit() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer = SegmentedLogWriter::create(storage.clone(), 0, config(5))
            .await
            .unwrap();
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"abc")))
                .await
                .unwrap()
                .is_none()
        );

        let sealed = writer
            .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"def")))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(sealed.base_offset(), 0);
        assert_eq!(sealed.end(), SegmentEnd::new(3, 1));
        assert_eq!(writer.committed().segment_byte_offset(), 3);
        assert_eq!(writer.data_synced().segment_byte_offset(), 3);
        assert_eq!(*storage.state.synced.lock().unwrap(), vec![0]);

        let committed = writer.flush().await.unwrap();
        assert_eq!(committed.segment_base_offset(), 1);
        assert_eq!(committed.segment_byte_offset(), 3);
        assert_eq!(committed.next_record_id(), 2);
        assert_eq!(storage.bytes(0), b"abc");
        assert_eq!(storage.bytes(1), b"def");
        assert_eq!(*storage.state.synced.lock().unwrap(), vec![0]);
        assert_eq!(writer.data_synced().segment_byte_offset(), 3);
        assert_eq!(*storage.state.directory_syncs.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn flush_synchronizes_data_when_the_interval_is_due() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer_config = config(64);
        writer_config.sync_interval = Duration::ZERO;
        let mut writer = SegmentedLogWriter::create(storage.clone(), 0, writer_config)
            .await
            .unwrap();
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"abc")))
                .await
                .unwrap()
                .is_none()
        );

        let committed = writer.flush().await.unwrap();
        assert_eq!(committed.segment_byte_offset(), 3);
        assert_eq!(writer.committed(), committed);
        assert_eq!(writer.data_synced(), committed);
        assert_eq!(*storage.state.synced.lock().unwrap(), vec![0]);
        assert_eq!(*storage.state.directory_syncs.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn failed_rotation_sync_prevents_rotation_and_remains_unsynchronized() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer = SegmentedLogWriter::create(storage.clone(), 0, config(3))
            .await
            .unwrap();
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"abc")))
                .await
                .unwrap()
                .is_none()
        );

        storage.fail_next_segment_sync(0);
        assert!(matches!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"def")))
                .await
                .unwrap_err(),
            SegmentedLogWriterError::SyncSegment {
                segment_base_offset: 0,
                ..
            }
        ));
        assert_eq!(writer.committed().segment_byte_offset(), 3);
        assert_eq!(writer.data_synced().segment_byte_offset(), 0);
        assert!(storage.state.synced.lock().unwrap().is_empty());
        assert_eq!(storage.state.files.lock().unwrap().len(), 1);
        assert_eq!(*storage.state.directory_syncs.lock().unwrap(), 1);

        let sealed = writer
            .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"def")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sealed.end(), SegmentEnd::new(3, 1));
        assert_eq!(writer.committed().segment_byte_offset(), 3);
        assert_eq!(writer.data_synced().segment_byte_offset(), 3);
        assert_eq!(*storage.state.synced.lock().unwrap(), vec![0]);
        assert_eq!(*storage.state.directory_syncs.lock().unwrap(), 2);

        assert_eq!(writer.sync_all().await.unwrap().segment_byte_offset(), 3);
        assert_eq!(*storage.state.synced.lock().unwrap(), vec![0, 1]);
        assert_eq!(writer.committed(), writer.data_synced());
    }

    #[tokio::test]
    async fn rejects_oversized_frames_without_changing_position() {
        let storage = TestStorage::new(usize::MAX);
        let mut writer = SegmentedLogWriter::create(storage, 7, config(4))
            .await
            .unwrap();

        assert!(matches!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"abcde")))
                .await,
            Err(SegmentedLogWriterError::FrameTooLarge {
                frame_len: 5,
                segment_size: 4,
            })
        ));
        assert_eq!(writer.committed(), Position::at_segment_start(7));
        assert_eq!(writer.next_record_id, 7);
        assert!(writer.batch.is_empty());
    }

    #[tokio::test]
    async fn filesystem_storage_creates_record_base_named_segment() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-writer").unwrap();
        let storage = FilesystemSegmentStorage::new(directory.path());
        let mut writer = SegmentedLogWriter::create(storage, 42, config(64))
            .await
            .unwrap();
        assert!(
            writer
                .append(PreparedFrame::from_test_bytes(Bytes::from_static(b"frame")))
                .await
                .unwrap()
                .is_none()
        );

        let synced = writer.sync_all().await.unwrap();
        assert_eq!(synced.segment_byte_offset(), 5);
        assert_eq!(synced.next_record_id(), 43);

        assert_eq!(
            tokio::fs::read(directory.path().join("42.log"))
                .await
                .unwrap(),
            b"frame"
        );
    }
}
