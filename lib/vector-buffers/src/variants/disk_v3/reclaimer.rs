use std::{collections::BTreeSet, io, path::PathBuf, sync::Arc, time::Duration};

use snafu::Snafu;
use tokio::sync::watch;
use tracing::error;

use crate::variants::disk_v3::segment_files::{
    SegmentFileError, list_segment_base_offsets, segment_file_name,
};

const RECLAIM_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(crate) struct SegmentReclaimer {
    directory: Arc<PathBuf>,
    segments: BTreeSet<u64>,
    last_reclaimed_segment: Option<u64>,
    directory_dirty: bool,
    reclaimer_rx: watch::Receiver<u64>,
}

impl SegmentReclaimer {
    pub(crate) fn new(directory: Arc<PathBuf>, reclaimer_rx: watch::Receiver<u64>) -> Self {
        Self {
            directory,
            segments: BTreeSet::new(),
            last_reclaimed_segment: None,
            directory_dirty: false,
            reclaimer_rx,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut reclaim_segment = *self.reclaimer_rx.borrow_and_update();
        let mut retry_pending = self.reclaim_needs_retry(reclaim_segment).await;

        loop {
            tokio::select! {
                result = self.reclaimer_rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    reclaim_segment = *self.reclaimer_rx.borrow_and_update();
                    retry_pending = self.reclaim_needs_retry(reclaim_segment).await;
                }
                () = tokio::time::sleep(RECLAIM_RETRY_DELAY), if retry_pending => {
                    retry_pending = self.reclaim_needs_retry(reclaim_segment).await;
                }
            }
        }
    }

    async fn reclaim_needs_retry(&mut self, reclaim_segment: u64) -> bool {
        match self.try_reclaim(reclaim_segment).await {
            Ok(()) => {
                self.last_reclaimed_segment = Some(reclaim_segment);
                false
            }
            Err(error) => {
                error!(%error, "Disk buffer segment reclamation failed; retrying.");
                true
            }
        }
    }

    async fn try_reclaim(&mut self, reclaim_segment: u64) -> Result<(), SegmentReclaimerError> {
        if self.last_reclaimed_segment != Some(reclaim_segment) {
            self.segments = list_segment_base_offsets(&self.directory)
                .await
                .map_err(|source| SegmentReclaimerError::ListSegmentFiles { source })?;
        }

        let mut reclaimable_segments = Vec::new();
        if let Some(max_ondisk_base_offset) = self.segments.last().copied() {
            reclaimable_segments.extend(self.segments.iter().copied().filter(|&base_offset| {
                base_offset < max_ondisk_base_offset && base_offset < reclaim_segment
            }));
        }

        let mut removal_error = None;
        for segment_base_offset in reclaimable_segments {
            let basename = segment_file_name(segment_base_offset);
            if let Err(source) = tokio::fs::remove_file(self.directory.join(basename)).await {
                removal_error = Some(SegmentReclaimerError::RemoveSegment {
                    segment_base_offset,
                    source,
                });
                break;
            }

            let removed = self.segments.remove(&segment_base_offset);
            debug_assert!(removed, "segment should have existed in the segment cache");
            self.directory_dirty = true;
        }

        // Synchronize successful removals even when a later removal in the
        // same batch failed. Keep the dirty bit set when synchronization fails
        // so a later retry does not depend on the removed files being listed.
        if self.directory_dirty {
            sync_directory(Arc::clone(&self.directory))
                .await
                .map_err(|source| SegmentReclaimerError::SyncDirectory { source })?;
            self.directory_dirty = false;
        }

        if let Some(error) = removal_error {
            return Err(error);
        }

        Ok(())
    }
}

async fn sync_directory(directory: Arc<PathBuf>) -> io::Result<()> {
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
        _ = directory;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "directory synchronization is unsupported on this platform",
        ))
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum SegmentReclaimerError {
    #[snafu(display("failed to synchronize the disk buffer directory: {source}"))]
    SyncDirectory { source: io::Error },

    #[snafu(display("failed to list disk buffer segment files: {source}"))]
    ListSegmentFiles { source: SegmentFileError },

    #[snafu(display("failed to remove segment {segment_base_offset}.log: {source}"))]
    RemoveSegment {
        segment_base_offset: u64,
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn write_segment(directory: &std::path::Path, base_offset: u64) {
        tokio::fs::write(directory.join(segment_file_name(base_offset)), b"segment")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn initial_watermark_reclaims_only_older_segments() {
        let directory = temp_dir::TempDir::with_prefix("vector-disk-v3-reclaimer").unwrap();
        for base_offset in [0, 10, 20] {
            write_segment(directory.path(), base_offset).await;
        }

        let (reclaimer_tx, reclaimer_rx) = watch::channel(20);
        drop(reclaimer_tx);
        SegmentReclaimer::new(Arc::new(directory.path().to_path_buf()), reclaimer_rx)
            .run()
            .await;

        assert_eq!(
            list_segment_base_offsets(directory.path()).await.unwrap(),
            BTreeSet::from([20])
        );
    }
}
