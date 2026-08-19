use snafu::Snafu;

use super::position::Position;

/// The durable reader position and logical bytes associated with one emitted frame.
///
/// The token is intentionally neither `Clone` nor `Copy`. It is registered
/// directly with an ordered finalizer when the frame is read, then transferred
/// to the writer actor after downstream finalization completes.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AcknowledgementToken {
    end_position: Position,
    frame_bytes: u64,
}

impl AcknowledgementToken {
    /// Creates the capability returned alongside one frame read from disk.
    pub(crate) fn new(
        end_position: Position,
        frame_bytes: usize,
    ) -> Result<Self, AcknowledgementError> {
        let frame_bytes =
            u64::try_from(frame_bytes).map_err(|_| AcknowledgementError::FrameLengthOverflow)?;
        Ok(Self {
            end_position,
            frame_bytes,
        })
    }

    pub(crate) fn into_parts(self) -> (Position, u64) {
        (self.end_position, self.frame_bytes)
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum AcknowledgementError {
    #[snafu(display("the encoded frame length cannot be represented as u64"))]
    FrameLengthOverflow,
}
