use std::{
    cmp::Ordering::{Equal, Greater, Less},
    ffi::OsString,
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use snafu::Snafu;
use tokio::fs;

use super::{position::Position, segment_files::parse_segment_file_name};

/// Calculates the initial logical occupancy from durable log boundaries.
pub(crate) async fn calculate_logical_bytes(
    directory: &Path,
    reclaimable: Position,
    committed: Position,
) -> Result<u64, LogicalCapacityError> {
    let committed_bytes = calculate_bytes_through(directory, committed).await?;
    let reclaimable_bytes = calculate_bytes_through(directory, reclaimable).await?;

    committed_bytes
        .checked_sub(reclaimable_bytes)
        .ok_or(LogicalCapacityError::ReclaimableBeyondCommitted)
}

/// Calculates all bytes from the start of the retained log through a boundary.
async fn calculate_bytes_through(
    directory: &Path,
    position: Position,
) -> Result<u64, LogicalCapacityError> {
    let mut entries = fs::read_dir(directory)
        .await
        .map_err(|source| LogicalCapacityError::ListDirectory { source })?;
    let boundary_base_offset = position.segment_base_offset();
    let mut found_boundary_segment = false;
    let mut bytes_through = 0_u64;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|source| LogicalCapacityError::ListDirectory { source })?
    {
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "log")
        {
            continue;
        }

        let name = entry.file_name();
        let base_offset = parse_segment_file_name(&name)
            .map_err(|_| LogicalCapacityError::InvalidSegmentFileName { name: name.clone() })?;
        let metadata =
            entry
                .metadata()
                .await
                .map_err(|source| LogicalCapacityError::ReadMetadata {
                    name: name.clone(),
                    source,
                })?;
        if !metadata.is_file() {
            return Err(LogicalCapacityError::InvalidSegmentEntry { name });
        }

        let bytes = match base_offset.cmp(&boundary_base_offset) {
            Less => metadata.len(),
            Equal => {
                found_boundary_segment = true;
                if position.segment_byte_offset() > metadata.len() {
                    return Err(LogicalCapacityError::ByteOffsetBeyondSegment {
                        base_offset: boundary_base_offset,
                        byte_offset: position.segment_byte_offset(),
                        file_len: metadata.len(),
                    });
                }
                position.segment_byte_offset()
            }
            Greater => 0,
        };
        bytes_through = bytes_through
            .checked_add(bytes)
            .ok_or(LogicalCapacityError::SizeOverflow)?;
    }

    if !found_boundary_segment {
        return Err(LogicalCapacityError::MissingSegment {
            base_offset: boundary_base_offset,
        });
    }

    Ok(bytes_through)
}

/// Cloneable producer-facing handle for soft logical byte admission.
///
/// Occupancy includes committed, non-reclaimable log bytes and accepted frames
/// queued or batched in memory. It does not include segment slack, checkpoint
/// files, or acknowledged segments awaiting deletion. The limit is a high-water
/// mark: one complete frame may take occupancy across it, after which admission
/// stops until durable acknowledgement brings occupancy below it again.
#[derive(Clone, Debug)]
pub(crate) struct LogicalCapacity {
    inner: Arc<LogicalCapacityInner>,
}

#[derive(Debug)]
struct LogicalCapacityInner {
    limit: u64,
    occupied: AtomicU64,
}

impl LogicalCapacity {
    /// Creates capacity initialized with the logical bytes recovered from disk.
    ///
    /// Recovered occupancy may exceed a newly lowered limit. In that case no
    /// new frames are admitted until checkpoints release enough bytes.
    pub(crate) fn new(limit: u64, recovered_bytes: u64) -> Result<Self, LogicalCapacityError> {
        if limit == 0 {
            return Err(LogicalCapacityError::ZeroLimit);
        }

        Ok(Self {
            inner: Arc::new(LogicalCapacityInner {
                limit,
                occupied: AtomicU64::new(recovered_bytes),
            }),
        })
    }

    /// Whether another complete frame may cross the logical high-water mark.
    #[must_use]
    pub(crate) fn is_below_high_water(&self) -> bool {
        self.occupied_bytes() < self.inner.limit
    }

    /// Atomically charges one complete frame if occupancy is below the logical
    /// high-water mark.
    pub(crate) fn try_acquire(&self, bytes: usize) -> Result<(), LogicalCapacityError> {
        let bytes = u64::try_from(bytes).map_err(|_| LogicalCapacityError::SizeOverflow)?;
        if bytes == 0 {
            return Err(LogicalCapacityError::ZeroFrameSize);
        }

        match self
            .inner
            .occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |occupied| {
                if occupied < self.inner.limit {
                    occupied.checked_add(bytes)
                } else {
                    None
                }
            }) {
            Ok(_) => Ok(()),
            Err(occupied) if occupied >= self.inner.limit => Err(LogicalCapacityError::Full),
            Err(_) => Err(LogicalCapacityError::SizeOverflow),
        }
    }

    /// Releases accepted bytes after their acknowledgement checkpoint is durable.
    pub(crate) fn release(&self, bytes: u64) -> Result<(), LogicalCapacityError> {
        self.inner
            .occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |occupied| {
                occupied.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|occupied| LogicalCapacityError::ReleaseUnderflow { occupied, bytes })
    }

    #[must_use]
    pub(crate) fn occupied_bytes(&self) -> u64 {
        self.inner.occupied.load(Ordering::Acquire)
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum LogicalCapacityError {
    #[snafu(display("failed to list the segment directory: {source}"))]
    ListDirectory { source: io::Error },

    #[snafu(display("invalid segment file name {name:?}"))]
    InvalidSegmentFileName { name: OsString },

    #[snafu(display("failed to read metadata for segment {name:?}: {source}"))]
    ReadMetadata { name: OsString, source: io::Error },

    #[snafu(display("segment entry {name:?} is not a regular file"))]
    InvalidSegmentEntry { name: OsString },

    #[snafu(display("segment {base_offset}.log is missing from the startup scan"))]
    MissingSegment { base_offset: u64 },

    #[snafu(display(
        "byte offset {byte_offset} is beyond the {file_len}-byte segment {base_offset}.log"
    ))]
    ByteOffsetBeyondSegment {
        base_offset: u64,
        byte_offset: u64,
        file_len: u64,
    },

    #[snafu(display("the reclaimable position is beyond the committed position"))]
    ReclaimableBeyondCommitted,

    #[snafu(display("logical capacity limit must be greater than zero"))]
    ZeroLimit,

    #[snafu(display("cannot charge a zero-byte frame"))]
    ZeroFrameSize,

    #[snafu(display("logical capacity is at its high-water mark"))]
    Full,

    #[snafu(display("logical capacity size overflowed u64"))]
    SizeOverflow,

    #[snafu(display(
        "cannot release {bytes} bytes from {occupied} bytes of logical capacity occupancy"
    ))]
    ReleaseUnderflow { occupied: u64, bytes: u64 },
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{Arc, Barrier},
    };

    use tokio::fs;

    use super::*;
    use crate::variants::disk_v3::segment_files::segment_file_name;

    async fn write_segment(directory: &Path, base_offset: u64, len: usize) {
        fs::write(
            directory.join(segment_file_name(base_offset)),
            vec![0_u8; len],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn recovery_excludes_the_unpublished_tail() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-logical-size").unwrap();
        write_segment(directory.path(), 100, 100).await;
        write_segment(directory.path(), 110, 200).await;
        write_segment(directory.path(), 120, 300).await;

        let committed = Position::new(120, 250, 130);
        let reclaimable = Position::new(100, 40, 105);
        assert_eq!(
            calculate_bytes_through(directory.path(), committed)
                .await
                .unwrap(),
            550
        );
        assert_eq!(
            calculate_bytes_through(directory.path(), reclaimable)
                .await
                .unwrap(),
            40
        );
        assert_eq!(
            calculate_logical_bytes(directory.path(), reclaimable, committed)
                .await
                .unwrap(),
            510
        );
    }

    #[test]
    fn lowered_limit_blocks_until_recovered_occupancy_drains() {
        let capacity = LogicalCapacity::new(10, 15).unwrap();
        assert!(matches!(
            capacity.try_acquire(1),
            Err(LogicalCapacityError::Full)
        ));

        capacity.release(6).unwrap();
        capacity.try_acquire(1).unwrap();
        assert_eq!(capacity.occupied_bytes(), 10);
    }

    #[test]
    fn one_complete_frame_can_cross_the_soft_limit() {
        let capacity = LogicalCapacity::new(10, 9).unwrap();

        capacity.try_acquire(8).unwrap();
        assert_eq!(capacity.occupied_bytes(), 17);
        assert!(matches!(
            capacity.try_acquire(1),
            Err(LogicalCapacityError::Full)
        ));

        capacity.release(8).unwrap();
        assert_eq!(capacity.occupied_bytes(), 9);

        capacity.try_acquire(20).unwrap();
        assert_eq!(capacity.occupied_bytes(), 29);
        capacity.release(20).unwrap();
        assert_eq!(capacity.occupied_bytes(), 9);
    }

    #[test]
    fn concurrent_producers_admit_only_one_frame_across_the_limit() {
        const PRODUCERS: usize = 8;

        let capacity = LogicalCapacity::new(1, 0).unwrap();
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let mut producers = Vec::with_capacity(PRODUCERS);

        for _ in 0..PRODUCERS {
            let capacity = capacity.clone();
            let start = Arc::clone(&start);
            producers.push(std::thread::spawn(move || {
                start.wait();
                capacity.try_acquire(10).is_ok()
            }));
        }

        start.wait();

        assert_eq!(
            producers
                .into_iter()
                .map(|producer| producer.join().unwrap())
                .filter(|acquired| *acquired)
                .count(),
            1
        );
    }
}
