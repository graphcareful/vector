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
use tokio::{fs, sync::Notify};

use super::{position::Position, writable_segment::parse_segment_file_name};

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

/// Cloneable producer-facing handle for logical byte admission.
///
/// Occupancy includes both committed, non-reclaimable log bytes and outstanding
/// frame reservations, including reservations waiting to be accepted. It does
/// not include segment slack, checkpoint files, or acknowledged segments
/// awaiting deletion.
#[derive(Clone, Debug)]
pub(crate) struct LogicalCapacity {
    inner: Arc<LogicalCapacityInner>,
}

#[derive(Debug)]
struct LogicalCapacityInner {
    limit: u64,
    occupied: AtomicU64,
    released: Notify,
}

impl LogicalCapacity {
    /// Creates capacity initialized with the logical bytes recovered from disk.
    ///
    /// Recovered occupancy may exceed a newly lowered limit. In that case no
    /// new reservations are admitted until checkpoints release enough bytes.
    pub(crate) fn new(limit: u64, recovered_bytes: u64) -> Result<Self, LogicalCapacityError> {
        if limit == 0 {
            return Err(LogicalCapacityError::ZeroLimit);
        }

        Ok(Self {
            inner: Arc::new(LogicalCapacityInner {
                limit,
                occupied: AtomicU64::new(recovered_bytes),
                released: Notify::new(),
            }),
        })
    }

    /// Waits until the requested logical bytes can be admitted.
    ///
    /// Cancelling this future before it returns does not consume capacity. If
    /// the returned reservation is dropped before append accepts it, its bytes
    /// are returned automatically.
    pub(crate) async fn reserve(
        &self,
        bytes: usize,
    ) -> Result<LogicalCapacityReservation, LogicalCapacityError> {
        loop {
            // Register before checking occupancy so a concurrent release
            // cannot occur between the failed check and waiter registration.
            let notified = self.inner.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.try_reserve(bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(LogicalCapacityError::Full { .. }) => notified.await,
                Err(error) => return Err(error),
            }
        }
    }

    /// Attempts admission without waiting for capacity to become available.
    pub(crate) fn try_reserve(
        &self,
        bytes: usize,
    ) -> Result<LogicalCapacityReservation, LogicalCapacityError> {
        let bytes = u64::try_from(bytes).map_err(|_| LogicalCapacityError::SizeOverflow)?;
        if bytes == 0 {
            return Err(LogicalCapacityError::ZeroReservation);
        }
        if bytes > self.inner.limit {
            return Err(LogicalCapacityError::RequestExceedsLimit {
                requested: bytes,
                limit: self.inner.limit,
            });
        }

        self.inner
            .occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |occupied| {
                occupied
                    .checked_add(bytes)
                    .filter(|updated| *updated <= self.inner.limit)
            })
            .map_err(|occupied| LogicalCapacityError::Full {
                requested: bytes,
                available: self.inner.limit.saturating_sub(occupied),
            })?;

        Ok(LogicalCapacityReservation {
            capacity: self.clone(),
            bytes,
            release_on_drop: true,
        })
    }

    /// Releases occupied bytes and wakes producers waiting for admission.
    ///
    /// `DiskBuffer` uses this for accepted bytes only after their acknowledgement
    /// checkpoint is durable. An unaccepted reservation uses it when dropped.
    pub(crate) fn release(&self, bytes: u64) -> Result<(), LogicalCapacityError> {
        self.release_inner(bytes)?;
        self.inner.released.notify_waiters();
        Ok(())
    }

    fn release_inner(&self, bytes: u64) -> Result<(), LogicalCapacityError> {
        self.inner
            .occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |occupied| {
                occupied.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|occupied| LogicalCapacityError::ReleaseUnderflow { occupied, bytes })
    }

    #[must_use]
    pub(crate) fn limit(&self) -> u64 {
        self.inner.limit
    }

    #[must_use]
    pub(crate) fn occupied_bytes(&self) -> u64 {
        self.inner.occupied.load(Ordering::Acquire)
    }

    #[must_use]
    pub(crate) fn available_bytes(&self) -> u64 {
        self.limit().saturating_sub(self.occupied_bytes())
    }
}

/// Owned admission for one frame's exact encoded byte length.
///
/// This value is intentionally neither cloneable nor constructible by callers.
/// It moves with the frame until `DiskBuffer` accepts both.
#[derive(Debug)]
#[must_use = "dropping an unaccepted capacity reservation returns its bytes"]
pub(crate) struct LogicalCapacityReservation {
    capacity: LogicalCapacity,
    bytes: u64,
    release_on_drop: bool,
}

impl LogicalCapacityReservation {
    pub(super) fn validate(
        &self,
        capacity: &LogicalCapacity,
        frame_bytes: usize,
    ) -> Result<(), LogicalCapacityError> {
        if !Arc::ptr_eq(&self.capacity.inner, &capacity.inner) {
            return Err(LogicalCapacityError::WrongBuffer);
        }

        let frame_bytes =
            u64::try_from(frame_bytes).map_err(|_| LogicalCapacityError::SizeOverflow)?;
        if self.bytes != frame_bytes {
            return Err(LogicalCapacityError::ReservationSizeMismatch {
                reserved: self.bytes,
                frame: frame_bytes,
            });
        }

        Ok(())
    }

    pub(super) fn commit(mut self) {
        // The reservation becomes part of buffer occupancy. Its bytes remain
        // charged until a durable acknowledgement checkpoint releases them.
        self.release_on_drop = false;
    }
}

impl Drop for LogicalCapacityReservation {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.capacity
                .release(self.bytes)
                .expect("an outstanding capacity reservation cannot underflow occupancy");
        }
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

    #[snafu(display("cannot reserve zero logical bytes"))]
    ZeroReservation,

    #[snafu(display("a {requested}-byte frame exceeds the {limit}-byte logical capacity limit"))]
    RequestExceedsLimit { requested: u64, limit: u64 },

    #[snafu(display(
        "logical capacity is full: requested {requested} bytes but only {available} are available"
    ))]
    Full { requested: u64, available: u64 },

    #[snafu(display("logical capacity size overflowed u64"))]
    SizeOverflow,

    #[snafu(display(
        "cannot release {bytes} bytes from {occupied} bytes of logical capacity occupancy"
    ))]
    ReleaseUnderflow { occupied: u64, bytes: u64 },

    #[snafu(display("capacity reservation belongs to a different disk buffer"))]
    WrongBuffer,

    #[snafu(display(
        "capacity reservation covers {reserved} bytes but the frame contains {frame} bytes"
    ))]
    ReservationSizeMismatch { reserved: u64, frame: u64 },
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration};

    use tokio::{fs, time::timeout};

    use super::*;
    use crate::variants::disk_v3::writable_segment::segment_file_name;

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
    fn dropping_an_unaccepted_reservation_returns_capacity() {
        let capacity = LogicalCapacity::new(10, 0).unwrap();
        let reservation = capacity.try_reserve(10).unwrap();
        assert_eq!(capacity.occupied_bytes(), 10);

        drop(reservation);

        assert_eq!(capacity.occupied_bytes(), 0);
        assert_eq!(capacity.available_bytes(), 10);
    }

    #[tokio::test]
    async fn reservation_waits_for_durable_capacity_release() {
        let capacity = LogicalCapacity::new(10, 10).unwrap();
        let waiting_capacity = capacity.clone();
        let waiter = tokio::spawn(async move { waiting_capacity.reserve(10).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());

        capacity.release(10).unwrap();
        let reservation = timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capacity.occupied_bytes(), 10);
        drop(reservation);
        assert_eq!(capacity.occupied_bytes(), 0);
    }

    #[test]
    fn lowered_limit_blocks_until_recovered_occupancy_drains() {
        let capacity = LogicalCapacity::new(10, 15).unwrap();
        assert!(matches!(
            capacity.try_reserve(1),
            Err(LogicalCapacityError::Full { available: 0, .. })
        ));

        capacity.release(6).unwrap();
        let reservation = capacity.try_reserve(1).unwrap();
        reservation.commit();
        assert_eq!(capacity.occupied_bytes(), 10);
    }

    #[test]
    fn oversized_request_is_rejected_instead_of_waiting_forever() {
        let capacity = LogicalCapacity::new(10, 0).unwrap();

        assert!(matches!(
            capacity.try_reserve(11),
            Err(LogicalCapacityError::RequestExceedsLimit {
                requested: 11,
                limit: 10,
            })
        ));
    }
}
