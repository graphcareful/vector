use std::{collections::BTreeSet, ffi::OsString, num::NonZeroU32, path::Path};

use tokio::fs;

use crate::variants::disk_v3::{
    frame::{PreparedFrame, encode_frame},
    position::Position,
    segmented_log_reader::SegmentedLogReader,
    segmented_log_writer::{
        DEFAULT_SYNC_INTERVAL, FilesystemSegmentStorage, SegmentedLogWriter,
        SegmentedLogWriterConfig,
    },
    writable_segment::segment_file_name,
};

pub(super) struct DiskV3Harness {
    directory: temp_dir::TempDir,
    max_frame_len: usize,
}

impl DiskV3Harness {
    pub(super) fn new(prefix: &str, max_frame_len: usize) -> Self {
        Self {
            directory: temp_dir::TempDir::with_prefix(prefix).unwrap(),
            max_frame_len,
        }
    }

    pub(super) fn path(&self) -> &Path {
        self.directory.path()
    }

    pub(super) fn frame(
        &self,
        record_id: u64,
        event_count: NonZeroU32,
        codec_metadata: u32,
        payload: impl Into<Vec<u8>>,
    ) -> TestFrame {
        TestFrame::new(
            record_id,
            event_count,
            codec_metadata,
            payload.into(),
            self.max_frame_len,
        )
    }

    pub(super) async fn create_writer(
        &self,
        first_record_id: u64,
        config: SegmentedLogWriterConfig,
    ) -> SegmentedLogWriter<FilesystemSegmentStorage> {
        SegmentedLogWriter::create(
            FilesystemSegmentStorage::new(self.path()),
            first_record_id,
            config,
        )
        .await
        .unwrap()
    }

    pub(super) async fn open_writer(
        &self,
        initial_record_id: u64,
        config: SegmentedLogWriterConfig,
    ) -> SegmentedLogWriter<FilesystemSegmentStorage> {
        SegmentedLogWriter::open_or_create(
            FilesystemSegmentStorage::new(self.path()),
            initial_record_id,
            self.max_frame_len,
            config,
        )
        .await
        .unwrap()
    }

    pub(super) async fn open_reader(&self, position: Position) -> SegmentedLogReader {
        SegmentedLogReader::open(self.path(), position, self.max_frame_len)
            .await
            .unwrap()
    }

    pub(super) async fn entry_names(&self) -> BTreeSet<OsString> {
        let mut entries = fs::read_dir(self.path()).await.unwrap();
        let mut names = BTreeSet::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.insert(entry.file_name());
        }
        names
    }

    pub(super) async fn segment_bytes(&self, segment_base_offset: u64) -> Vec<u8> {
        fs::read(self.path().join(segment_file_name(segment_base_offset)))
            .await
            .unwrap()
    }
}

pub(super) fn writer_config(segment_size: u64, batch_size: usize) -> SegmentedLogWriterConfig {
    SegmentedLogWriterConfig {
        segment_size,
        batch_size,
        sync_interval: DEFAULT_SYNC_INTERVAL,
    }
}

pub(super) struct TestFrame {
    record_id: u64,
    event_count: NonZeroU32,
    codec_metadata: u32,
    payload: Vec<u8>,
    prepared: PreparedFrame,
}

impl TestFrame {
    fn new(
        record_id: u64,
        event_count: NonZeroU32,
        codec_metadata: u32,
        payload: Vec<u8>,
        max_frame_len: usize,
    ) -> Self {
        let prepared = encode_frame(
            record_id,
            event_count,
            codec_metadata,
            &payload,
            max_frame_len,
        )
        .unwrap();
        Self {
            record_id,
            event_count,
            codec_metadata,
            payload,
            prepared,
        }
    }

    pub(super) const fn record_id(&self) -> u64 {
        self.record_id
    }

    pub(super) const fn event_count(&self) -> NonZeroU32 {
        self.event_count
    }

    pub(super) const fn codec_metadata(&self) -> u32 {
        self.codec_metadata
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(super) fn prepared(&self) -> &PreparedFrame {
        &self.prepared
    }

    pub(super) fn encoded_bytes(&self) -> &[u8] {
        self.prepared.bytes()
    }

    pub(super) fn encoded_len(&self) -> usize {
        self.prepared.bytes().len()
    }
}
