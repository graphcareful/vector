use std::{
    collections::BTreeSet,
    ffi::{OsStr, OsString},
    io,
    path::Path,
};

use snafu::Snafu;
use tokio::fs;

#[derive(Debug, Snafu)]
pub(crate) enum SegmentFileNameError {
    #[snafu(display("segment file name is not valid UTF-8"))]
    NonUtf8,

    #[snafu(display("segment file name must end with '.log'"))]
    InvalidExtension,

    #[snafu(display("segment file name must contain a canonical u64 record base offset"))]
    InvalidOffset,
}

#[derive(Debug, Snafu)]
pub(crate) enum SegmentFileError {
    #[snafu(display("failed to list the segment directory: {source}"))]
    ListDirectory { source: io::Error },

    #[snafu(display("invalid segment file name {name:?}"))]
    InvalidSegmentFileName { name: OsString },
}

/// Returns the canonical file name for a segment whose first record ID is
/// `base_offset`.
#[must_use]
pub(crate) fn segment_file_name(base_offset: u64) -> String {
    format!("{base_offset}.log")
}

/// Parses a canonical `<base_offset>.log` segment file name.
pub(crate) fn parse_segment_file_name(name: &OsStr) -> Result<u64, SegmentFileNameError> {
    let name = name.to_str().ok_or(SegmentFileNameError::NonUtf8)?;
    let offset = name
        .strip_suffix(".log")
        .ok_or(SegmentFileNameError::InvalidExtension)?;

    if offset.is_empty()
        || !offset.bytes().all(|byte| byte.is_ascii_digit())
        || (offset.len() > 1 && offset.starts_with('0'))
    {
        return Err(SegmentFileNameError::InvalidOffset);
    }

    offset
        .parse()
        .map_err(|_| SegmentFileNameError::InvalidOffset)
}

/// Lists the unique base offsets encoded in segment file names.
pub(crate) async fn list_segment_base_offsets(
    directory: &Path,
) -> Result<BTreeSet<u64>, SegmentFileError> {
    let mut entries = fs::read_dir(directory)
        .await
        .map_err(|source| SegmentFileError::ListDirectory { source })?;
    let mut base_offsets = BTreeSet::new();

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|source| SegmentFileError::ListDirectory { source })?
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
            .map_err(|_| SegmentFileError::InvalidSegmentFileName { name: name.clone() })?;
        base_offsets.insert(base_offset);
    }

    Ok(base_offsets)
}
