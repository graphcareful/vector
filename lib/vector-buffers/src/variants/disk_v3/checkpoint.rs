use std::{io, mem::size_of, path::Path};

use crc32fast::Hasher;
use memmap2::MmapMut;
use snafu::Snafu;
use tokio::fs::OpenOptions;

use super::position::Position;

pub(crate) const CHECKPOINT_FILE_NAME: &str = "reader.checkpoint";

const CHECKPOINT_MAGIC: [u8; 8] = *b"VDB3RCP\0";
const CHECKPOINT_VERSION: u32 = 1;

/// Fixed mmap layout. Field order avoids internal and trailing padding, while
/// `repr(C)` makes that layout stable for this checkpoint version.
#[derive(Clone, Copy)]
#[repr(C)]
struct CheckpointRecord {
    magic: [u8; 8],
    version: u32,
    checksum: u32,
    segment_base_offset: u64,
    segment_byte_offset: u64,
    next_record_id: u64,
}

const CHECKPOINT_LEN: usize = size_of::<CheckpointRecord>();
const _: () =
    assert!(CHECKPOINT_LEN == size_of::<[u8; 8]>() + 3 * size_of::<u64>() + 2 * size_of::<u32>());

impl CheckpointRecord {
    fn new(position: Position) -> Self {
        let mut record = Self {
            magic: CHECKPOINT_MAGIC,
            version: CHECKPOINT_VERSION.to_le(),
            checksum: 0,
            segment_base_offset: position.segment_base_offset().to_le(),
            segment_byte_offset: position.segment_byte_offset().to_le(),
            next_record_id: position.next_record_id().to_le(),
        };
        record.checksum = record.calculate_checksum().to_le();
        record
    }

    fn decode(self) -> Result<Position, CheckpointDecodeError> {
        if self.magic != CHECKPOINT_MAGIC {
            return Err(CheckpointDecodeError::InvalidMagic { actual: self.magic });
        }

        let expected = u32::from_le(self.checksum);
        let actual = self.calculate_checksum();
        if expected != actual {
            return Err(CheckpointDecodeError::ChecksumMismatch { expected, actual });
        }

        let version = u32::from_le(self.version);
        if version != CHECKPOINT_VERSION {
            return Err(CheckpointDecodeError::UnsupportedVersion { version });
        }

        Ok(Position::new(
            u64::from_le(self.segment_base_offset),
            u64::from_le(self.segment_byte_offset),
            u64::from_le(self.next_record_id),
        ))
    }

    fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_ne_bytes());
        hasher.update(&self.segment_base_offset.to_ne_bytes());
        hasher.update(&self.segment_byte_offset.to_ne_bytes());
        hasher.update(&self.next_record_id.to_ne_bytes());
        hasher.finalize()
    }
}

/// Durable, single-slot snapshot of the acknowledged reader position.
///
/// A checksum distinguishes a complete snapshot from a torn update. When the
/// snapshot is invalid, startup deliberately falls back to the earliest
/// retained segment and accepts duplicate delivery.
pub(crate) struct ReaderCheckpoint {
    map: MmapMut,
    uninitialized: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointLoad {
    Uninitialized,
    Position(Position),
}

impl ReaderCheckpoint {
    /// Opens the checkpoint file, creating and synchronizing its fixed-size
    /// backing file when necessary.
    pub(crate) async fn open(directory: &Path) -> Result<Self, CheckpointError> {
        let path = directory.join(CHECKPOINT_FILE_NAME);
        let (file, created) = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => (file, true),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await
                    .map_err(|source| CheckpointError::Open { source })?;
                (file, false)
            }
            Err(source) => return Err(CheckpointError::Create { source }),
        };

        let actual_len = file
            .metadata()
            .await
            .map_err(|source| CheckpointError::Metadata { source })?
            .len();
        let checkpoint_len =
            u64::try_from(CHECKPOINT_LEN).expect("the checkpoint length is representable as u64");
        if actual_len == 0 {
            file.set_len(checkpoint_len)
                .await
                .map_err(|source| CheckpointError::Resize { source })?;
            file.sync_all()
                .await
                .map_err(|source| CheckpointError::SyncFile { source })?;
        } else if actual_len != checkpoint_len {
            return Err(CheckpointError::InvalidLength {
                expected: checkpoint_len,
                actual: actual_len,
            });
        }

        if created {
            sync_directory(directory).await?;
        }

        let std_file = file.into_std().await;
        // SAFETY: The checkpoint file is exclusively owned by this disk buffer,
        // remains fixed at CHECKPOINT_LEN while mapped, and is not truncated.
        let map = unsafe { MmapMut::map_mut(&std_file) }
            .map_err(|source| CheckpointError::Map { source })?;
        Ok(Self {
            map,
            uninitialized: actual_len == 0,
        })
    }

    /// Decodes the last snapshot, distinguishing an uninitialized file from an
    /// invalid record. Recovery policy belongs to the caller.
    pub(crate) fn load(&self) -> Result<CheckpointLoad, CheckpointDecodeError> {
        if self.uninitialized {
            return Ok(CheckpointLoad::Uninitialized);
        }

        read_record(&self.map)
            .decode()
            .map(CheckpointLoad::Position)
    }

    /// Replaces and synchronously flushes the reader position snapshot.
    pub(crate) fn persist(&mut self, position: Position) -> Result<(), CheckpointError> {
        write_record(&mut self.map, CheckpointRecord::new(position));
        self.map
            .flush()
            .map_err(|source| CheckpointError::Flush { source })?;
        self.uninitialized = false;
        Ok(())
    }
}

fn read_record(map: &MmapMut) -> CheckpointRecord {
    debug_assert_eq!(map.len(), CHECKPOINT_LEN);
    // SAFETY: Every bit pattern is valid for CheckpointRecord's integer and
    // byte-array fields. `read_unaligned` does not require the map's pointer to
    // have the record's alignment.
    unsafe { map.as_ptr().cast::<CheckpointRecord>().read_unaligned() }
}

fn write_record(map: &mut MmapMut, record: CheckpointRecord) {
    debug_assert_eq!(map.len(), CHECKPOINT_LEN);
    // SAFETY: The mapping is writable for CHECKPOINT_LEN bytes and
    // `write_unaligned` does not require its pointer to be record-aligned.
    unsafe {
        map.as_mut_ptr()
            .cast::<CheckpointRecord>()
            .write_unaligned(record);
    }
}

async fn sync_directory(directory: &Path) -> Result<(), CheckpointError> {
    #[cfg(unix)]
    {
        let directory = tokio::fs::File::open(directory)
            .await
            .map_err(|source| CheckpointError::OpenDirectory { source })?;
        directory
            .sync_all()
            .await
            .map_err(|source| CheckpointError::SyncDirectory { source })
    }

    #[cfg(windows)]
    {
        let directory = directory.to_owned();
        tokio::task::spawn_blocking(move || {
            use std::os::windows::fs::OpenOptionsExt;

            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(directory)?
                .sync_all()
        })
        .await
        .map_err(|source| CheckpointError::SyncDirectoryTask { source })?
        .map_err(|source| CheckpointError::SyncDirectory { source })
    }

    #[cfg(not(any(unix, windows)))]
    {
        Err(CheckpointError::SyncDirectory {
            source: io::Error::new(
                io::ErrorKind::Unsupported,
                "directory synchronization is unsupported on this platform",
            ),
        })
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum CheckpointError {
    #[snafu(display("failed to create the reader checkpoint: {source}"))]
    Create { source: io::Error },

    #[snafu(display("failed to open the reader checkpoint: {source}"))]
    Open { source: io::Error },

    #[snafu(display("failed to read reader checkpoint metadata: {source}"))]
    Metadata { source: io::Error },

    #[snafu(display("failed to resize the reader checkpoint: {source}"))]
    Resize { source: io::Error },

    #[snafu(display("failed to synchronize the reader checkpoint file: {source}"))]
    SyncFile { source: io::Error },

    #[snafu(display("failed to open the disk buffer directory: {source}"))]
    OpenDirectory { source: io::Error },

    #[snafu(display("failed to synchronize the disk buffer directory: {source}"))]
    SyncDirectory { source: io::Error },

    #[cfg(windows)]
    #[snafu(display("the directory synchronization task failed: {source}"))]
    SyncDirectoryTask { source: tokio::task::JoinError },

    #[snafu(display("reader checkpoint has length {actual} bytes, expected {expected} bytes"))]
    InvalidLength { expected: u64, actual: u64 },

    #[snafu(display("failed to memory-map the reader checkpoint: {source}"))]
    Map { source: io::Error },

    #[snafu(display("failed to flush the reader checkpoint: {source}"))]
    Flush { source: io::Error },
}

#[derive(Debug, Snafu)]
pub(crate) enum CheckpointDecodeError {
    #[snafu(display("reader checkpoint has invalid magic bytes {actual:?}"))]
    InvalidMagic { actual: [u8; 8] },

    #[snafu(display(
        "reader checkpoint checksum mismatch: stored {expected:#010x}, calculated {actual:#010x}"
    ))]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[snafu(display("reader checkpoint version {version} is not supported"))]
    UnsupportedVersion { version: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_and_reopens_a_position() {
        let directory = temp_dir::TempDir::new().unwrap();
        let position = Position::new(100, 4096, 125);

        let mut checkpoint = ReaderCheckpoint::open(directory.path()).await.unwrap();
        assert_eq!(checkpoint.load().unwrap(), CheckpointLoad::Uninitialized);
        checkpoint.persist(position).unwrap();
        drop(checkpoint);

        let checkpoint = ReaderCheckpoint::open(directory.path()).await.unwrap();
        assert_eq!(
            checkpoint.load().unwrap(),
            CheckpointLoad::Position(position)
        );
    }

    #[tokio::test]
    async fn rejects_a_torn_snapshot_without_failing_to_open() {
        let directory = temp_dir::TempDir::new().unwrap();
        let position = Position::new(100, 4096, 125);

        let mut checkpoint = ReaderCheckpoint::open(directory.path()).await.unwrap();
        checkpoint.persist(position).unwrap();

        let mut record = read_record(&checkpoint.map);
        record.segment_byte_offset ^= 1;
        write_record(&mut checkpoint.map, record);
        checkpoint.map.flush().unwrap();
        drop(checkpoint);

        let checkpoint = ReaderCheckpoint::open(directory.path()).await.unwrap();
        assert!(matches!(
            checkpoint.load(),
            Err(CheckpointDecodeError::ChecksumMismatch { .. })
        ));
    }
}
