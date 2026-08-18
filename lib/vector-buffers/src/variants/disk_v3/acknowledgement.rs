use std::collections::VecDeque;

use snafu::Snafu;

use super::position::Position;

/// An opaque capability proving that one frame was emitted by the buffer.
///
/// The token is intentionally neither `Clone` nor `Copy`: the downstream
/// finalizer transfers it back to the acknowledgement tracker exactly once.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AcknowledgementToken {
    sequence: u64,
}

/// Tracks emitted frames and contiguous downstream acknowledgement progress.
#[derive(Debug)]
pub(crate) struct AcknowledgementTracker {
    next_sequence: u64,
    pending: VecDeque<PendingAcknowledgement>,
    observed_position: Position,
    reclaimable_position: Position,
    uncheckpointed_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointCandidate {
    position: Position,
    bytes: u64,
}

impl CheckpointCandidate {
    #[must_use]
    pub(crate) const fn position(self) -> Position {
        self.position
    }

    #[must_use]
    pub(crate) const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[derive(Debug)]
struct PendingAcknowledgement {
    sequence: u64,
    end_position: Position,
    frame_bytes: u64,
}

impl AcknowledgementTracker {
    pub(crate) fn new(reclaimable_position: Position) -> Self {
        Self {
            next_sequence: 0,
            pending: VecDeque::new(),
            observed_position: reclaimable_position,
            reclaimable_position,
            uncheckpointed_bytes: 0,
        }
    }

    /// Records one emitted frame and returns its single-use token.
    pub(crate) fn track_read(
        &mut self,
        end_position: Position,
        frame_bytes: usize,
    ) -> Result<AcknowledgementToken, AcknowledgementError> {
        let sequence = self.next_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(AcknowledgementError::SequenceOverflow)?;
        let frame_bytes =
            u64::try_from(frame_bytes).map_err(|_| AcknowledgementError::FrameLengthOverflow)?;

        self.pending.push_back(PendingAcknowledgement {
            sequence,
            end_position,
            frame_bytes,
        });
        self.next_sequence = next_sequence;

        Ok(AcknowledgementToken { sequence })
    }

    /// Advances the observed position by one frame in original read order.
    #[allow(clippy::needless_pass_by_value)] // Consuming the token enforces one acknowledgement.
    pub(crate) fn acknowledge(
        &mut self,
        token: AcknowledgementToken,
    ) -> Result<Position, AcknowledgementError> {
        let AcknowledgementToken { sequence } = token;
        let pending = self
            .pending
            .front()
            .ok_or(AcknowledgementError::NoOutstandingAcknowledgement)?;
        if sequence != pending.sequence {
            return Err(AcknowledgementError::OutOfOrder {
                expected: pending.sequence,
                actual: sequence,
            });
        }
        let uncheckpointed_bytes = self
            .uncheckpointed_bytes
            .checked_add(pending.frame_bytes)
            .ok_or(AcknowledgementError::ByteCountOverflow)?;

        let pending = self
            .pending
            .pop_front()
            .expect("the pending acknowledgement was checked above");
        self.observed_position = pending.end_position;
        self.uncheckpointed_bytes = uncheckpointed_bytes;

        Ok(pending.end_position)
    }

    /// Largest contiguous position whose downstream finalizers have completed.
    #[must_use]
    pub(crate) const fn observed_position(&self) -> Position {
        self.observed_position
    }

    /// Durable acknowledged position through which capacity may be reused.
    #[must_use]
    pub(crate) const fn reclaimable_position(&self) -> Position {
        self.reclaimable_position
    }

    /// Returns the acknowledgement prefix waiting to be made durable.
    #[must_use]
    pub(crate) fn checkpoint_candidate(&self) -> Option<CheckpointCandidate> {
        (self.observed_position != self.reclaimable_position).then_some(CheckpointCandidate {
            position: self.observed_position,
            bytes: self.uncheckpointed_bytes,
        })
    }

    /// Promotes an acknowledgement prefix after its checkpoint was flushed.
    pub(crate) fn mark_checkpointed(&mut self, candidate: CheckpointCandidate) {
        debug_assert_eq!(candidate.position, self.observed_position);
        debug_assert_eq!(candidate.bytes, self.uncheckpointed_bytes);
        self.reclaimable_position = candidate.position;
        self.uncheckpointed_bytes = 0;
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum AcknowledgementError {
    #[snafu(display("there is no outstanding disk buffer acknowledgement"))]
    NoOutstandingAcknowledgement,

    #[snafu(display(
        "acknowledgement {actual} arrived out of order; expected acknowledgement {expected}"
    ))]
    OutOfOrder { expected: u64, actual: u64 },

    #[snafu(display("the disk buffer acknowledgement sequence overflowed u64"))]
    SequenceOverflow,

    #[snafu(display("the encoded frame length cannot be represented as u64"))]
    FrameLengthOverflow,

    #[snafu(display("uncheckpointed acknowledged bytes overflowed u64"))]
    ByteCountOverflow,
}
