use snafu::Snafu;

use super::position::Position;

/// The durable reader position and logical bytes associated with one unit of ordered read
/// progress.
///
/// The token is intentionally neither `Clone` nor `Copy`. It is registered directly with an
/// ordered finalizer when a frame is read, then transferred to the writer actor after downstream
/// finalization completes.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ReadProgressToken {
    end_position: Position,
    logical_bytes: u64,
}

impl ReadProgressToken {
    /// Creates the progress token returned alongside one frame read from disk.
    pub(crate) fn for_frame(
        end_position: Position,
        frame_len: usize,
    ) -> Result<Self, ReadProgressError> {
        let logical_bytes =
            u64::try_from(frame_len).map_err(|_| ReadProgressError::FrameLengthOverflow)?;
        Ok(Self {
            end_position,
            logical_bytes,
        })
    }

    pub(crate) fn into_parts(self) -> (Position, u64) {
        (self.end_position, self.logical_bytes)
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum ReadProgressError {
    #[snafu(display("the encoded frame length cannot be represented as u64"))]
    FrameLengthOverflow,
}
