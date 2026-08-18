/// A restartable boundary immediately before the next frame in the segmented log.
///
/// `segment_base_offset` is a record ID and selects `<base_offset>.log`.
/// `segment_byte_offset` is a byte coordinate local to that file. The two
/// values deliberately use different names because they are not arithmetically
/// related. Rotation names the next segment with `next_record_id` and resets
/// its byte offset to zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Position {
    /// Record ID encoded in the current segment's `<base_offset>.log` name.
    pub(super) segment_base_offset: u64,
    /// Byte offset relative to the beginning of the current segment file.
    pub(super) segment_byte_offset: u64,
    /// Record ID assigned to the frame immediately after this boundary.
    pub(super) next_record_id: u64,
}

impl Position {
    #[must_use]
    pub(crate) const fn new(
        segment_base_offset: u64,
        segment_byte_offset: u64,
        next_record_id: u64,
    ) -> Self {
        Self {
            segment_base_offset,
            segment_byte_offset,
            next_record_id,
        }
    }

    #[must_use]
    pub(crate) const fn at_segment_start(base_offset: u64) -> Self {
        Self::new(base_offset, 0, base_offset)
    }

    #[must_use]
    pub(crate) const fn segment_base_offset(self) -> u64 {
        self.segment_base_offset
    }

    #[must_use]
    pub(crate) const fn segment_byte_offset(self) -> u64 {
        self.segment_byte_offset
    }

    #[must_use]
    pub(crate) const fn next_record_id(self) -> u64 {
        self.next_record_id
    }

    /// Advances within the current segment. Rotation creates a new position
    /// with [`Self::at_segment_start`].
    pub(crate) fn checked_advance(self, frame_len: u64, event_count: u32) -> Option<Self> {
        let segment_byte_offset = self.segment_byte_offset.checked_add(frame_len)?;
        let next_record_id = self.next_record_id.checked_add(u64::from(event_count))?;

        Some(Self::new(
            self.segment_base_offset,
            segment_byte_offset,
            next_record_id,
        ))
    }
}
