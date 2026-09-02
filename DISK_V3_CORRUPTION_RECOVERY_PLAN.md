# Disk V3 Corruption Recovery Plan

This document breaks disk-v3 corruption recovery into implementable changes.
It builds on [DISK_V3_DESIGN.md](DISK_V3_DESIGN.md) and resolves the reader
corruption finding in [DISK_V3_CODE_REVIEW.md](DISK_V3_CODE_REVIEW.md).

This is a draft implementation plan, not an accepted public contract.

## Objective

Disk v3 should continue reading after recoverable structural corruption without
allowing the durable reader checkpoint to cross an earlier record that is still
being processed downstream.

Recovery must find a trustworthy frame boundary, represent the skipped range
explicitly, account for its logical bytes, report the data loss, and advance
the checkpoint through the same ordered mechanism used by ordinary records.

## Current behavior

`ReadableSegment::read_next` validates a frame header, payload length, and
checksum before advancing its logical `Position`. On an error, the logical
position remains at the last valid boundary, but the underlying file cursor may
have consumed part or all of the invalid frame.

`DiskBufferReceiver` currently becomes terminal after any reader or typed
decode failure. This prevents later acknowledgements from accidentally moving
the checkpoint past the failed frame, but it also prevents best-effort recovery.

Startup has the same limitation. `SegmentedLogWriter::open_or_create` uses
`SegmentedLogReader::recover_tail`, which fails when any retained frame is
corrupt or when the newest segment has an incomplete crash tail.

## Goals

- Recover from structural corruption by finding the next fully validated frame
  or segment boundary.
- Never use the incidental file cursor as a recovered boundary.
- Never trust a frame length taken from an unvalidated header.
- Preserve ordered downstream acknowledgement semantics across skipped data.
- Release exactly the logical bytes covered by a durable skip checkpoint.
- Recover the writer tail after internal corruption.
- Truncate an incomplete or unrecoverable suffix in the newest segment.
- Emit actionable diagnostics for every skipped range.
- Keep unsupported formats and ordinary I/O failures terminal.

## Non-goals

- Repairing corrupt payloads.
- Reconstructing an event count when no trustworthy frame boundary provides it.
- Hiding data loss from operators.
- Treating every application decode failure as disk corruption.
- Supporting multiple consumers.
- Providing exactly-once delivery.
- Rewriting or compacting a segment that contains an internal corrupt range.

## Required invariants

The implementation must preserve these invariants:

1. Every logical reader position is a validated restart boundary.
2. The reader never scans or reads beyond the writer's committed boundary.
3. A recovered candidate is accepted only after its complete frame checksum
   and header invariants pass.
4. A recovered candidate's record ID never regresses relative to the last
   valid reader position.
5. A corruption skip cannot become durable before every earlier emitted record
   reaches a terminal downstream status.
6. Logical capacity is released only after the skip position is durably
   checkpointed.
7. Startup never truncates bytes before a valid durable reader checkpoint.
8. Unsupported format versions are not silently treated as corrupt data.

## Failure classification

Recovery behavior should depend on the type of failure rather than treating
every read error identically.

### Recoverable structural corruption

These failures may start resynchronization:

- Invalid frame magic.
- A checksum mismatch in a version understood by this implementation.
- A zero event count.
- A nonzero reserved field.
- A declared frame length that is invalid or exceeds configured limits.
- A completely validated frame whose record ID regresses.
- An incomplete suffix in an older segment when a newer segment provides a
  trustworthy resume boundary.

The exact classification should be encoded in one helper instead of repeated
across the segment and buffer layers.

### Incompatible data

These failures remain terminal initially:

- An unsupported frame version.
- Unsupported frame flags.
- An unsupported payload codec.
- Codec metadata that the requested `Bufferable` type does not understand.

These conditions may indicate a newer Vector format rather than damaged data.
Automatically skipping them could destroy valid data during a downgrade or
mixed-version deployment.

### Application decode failures

A frame with a valid header and checksum has a trustworthy ending position.
Mechanically, it is safe to skip when `T::decode` fails or when the decoded
event count disagrees with the frame.

Semantically, these errors may indicate a software compatibility problem or a
decoder bug. They should remain terminal in the first structural-corruption
implementation. A later, separately named policy may allow operators to skip
undecodable records.

### I/O and actor failures

Filesystem I/O errors, checkpoint errors, actor failure, and position overflow
remain terminal. They do not provide evidence that a later byte boundary is
safe or durable.

### Newest incomplete tail

An incomplete suffix in the newest segment is handled by startup repair, not by
normal corruption skipping. Startup truncates the active file to the last
validated boundary, synchronizes the repaired file, and resumes writing there.

## Proposed data flow

```text
read committed bytes
        |
        +-- valid frame --> decode T --> emit downstream
        |                                  |
        |                                  v
        |                         ordered finalization
        |
        +-- structural corruption
                    |
                    v
          locate next valid boundary
                    |
                    +-- no safe boundary --> terminal error
                    |
                    v
             report CorruptRange
                    |
                    v
         immediate ordered progress token
                    |
                    v
         writer syncs committed data
                    |
                    v
          persist reader checkpoint
                    |
                    v
        release skipped logical bytes
```

The receiver may scan over multiple corrupt ranges in one `next()` call before
returning the next valid `T`.

## Proposed internal types

The exact names may change during implementation, but the responsibilities
should remain explicit.

```rust
struct CorruptRange {
    start: Position,
    resume_at: Position,
    logical_bytes: u64,
    expected_record_id: u64,
    resumed_record_id: u64,
    cause: CorruptionCause,
}

enum SegmentedRead {
    Frame {
        frame: OwnedDecodedFrame,
        position: Position,
    },
    CorruptRange(CorruptRange),
    EndOfAvailableData,
    IncompleteTail,
}
```

`AcknowledgementToken` should be generalized because a skipped range is read
progress but is not a downstream acknowledgement:

```rust
struct ReadProgressToken {
    end_position: Position,
    logical_bytes: u64,
}
```

For an ordinary frame, `logical_bytes` is its framed length. For a skipped
range, it is the exact logical byte distance from `start` to `resume_at`,
including complete intervening segments when recovery crosses segment files.

## Component responsibilities

### Frame codec

Files:

- `frame.rs`

Responsibilities:

- Continue to encode and validate one candidate frame.
- Expose enough structured error information to classify failures.
- Avoid embedding scan, checkpoint, logging, or skip policy.
- Keep unsupported-version errors distinguishable from structural corruption.

No frame API should claim that bytes following an invalid header are a valid
boundary.

### Readable segment

Files:

- `readable_segment.rs`

Responsibilities:

- Preserve the last validated logical position on every error.
- Restore the file cursor to that logical position before recovery begins.
- Search within one segment starting one byte after the failing boundary.
- Locate occurrences of `FRAME_MAGIC` without loading the whole segment.
- Fully validate each candidate header and payload checksum.
- Reject candidates whose record ID is below the expected record ID.
- Stop at an explicit scan limit rather than reading arbitrary file suffixes.
- Return a candidate boundary and skipped byte range without performing
  checkpoint or capacity operations.

The scan limit is the committed byte position for the active segment and the
file end for a sealed segment.

The scanner must advance at least one byte after every rejected candidate so
corrupt input cannot cause an infinite loop.

### Segmented log reader

Files:

- `segmented_log_reader.rs`
- `position.rs`

Responsibilities:

- Coordinate recovery across segment files.
- Ask `ReadableSegment` for a valid candidate in the current segment.
- Fall forward to the smallest greater segment base offset when the current
  segment has no valid candidate.
- Use the next segment's base offset as its trustworthy next record ID.
- Enforce record-ID monotonicity across recovered boundaries.
- Calculate the exact logical byte count covered by a skipped range.
- Produce `SegmentedRead::CorruptRange` rather than silently changing position.
- Never scan beyond the caller-provided committed position.

The reader's logical position should move to `resume_at` only as part of
producing the explicit corrupt-range result. A retry after an unhandled error
must not continue from an incidental file cursor.

### Typed disk buffer receiver

Files:

- `disk_buffer.rs`

Responsibilities:

- Continue looping when `read_next_committed` returns a recoverable
  `CorruptRange`.
- Emit the data-loss diagnostic before continuing.
- Convert the range into an immediately completed ordered progress token.
- Keep valid decoded records on the existing downstream finalization path.
- Keep incompatible formats, application decode failures, and I/O failures
  terminal initially.
- Stop setting the receiver's terminal state for successfully skipped
  structural corruption.

The current `failed: bool` may remain for terminal failures. If more terminal
states become necessary, replace it with a small state enum rather than adding
more booleans.

### Ordered progress and writer actor

Files:

- `acknowledgement.rs`
- `disk_buffer.rs`
- `writer_actor.rs`

Responsibilities:

- Rename or generalize `AcknowledgementToken` to `ReadProgressToken`.
- Register normal frames and corrupt ranges with the same
  `OrderedFinalizer<ReadProgressToken>`.
- Complete a corrupt-range token immediately, without emitting an application
  record.
- Allow the ordered finalizer to hold that token behind any earlier unfinished
  frame.
- Synchronize committed writer data before checkpointing the token.
- Persist `end_position`.
- Release `logical_bytes` only after checkpoint persistence succeeds.
- Publish the new reclaimable position to the segment reclaimer.

No separate skip command should bypass the ordered finalizer.

### Checkpoint and logical capacity

Files:

- `checkpoint.rs`
- `logical_capacity.rs`

Responsibilities:

- Keep the existing versioned `Position` checkpoint format.
- Accept only positions established as valid recovery boundaries.
- Generalize byte accounting from one frame to one contiguous progress range.
- Share a helper for calculating logical byte distance across segments if the
  reader and startup calculation would otherwise duplicate it.
- Retain checked underflow and overflow handling.

The checkpoint does not need to store corruption metadata. Replaying a skip
after a crash before checkpoint persistence is acceptable and preserves the
existing at-least-once progress model.

### Startup recovery and writer resume

Files:

- `disk_buffer.rs`
- `segmented_log_reader.rs`
- `segmented_log_writer.rs`
- `writable_segment.rs`

Responsibilities:

- Pass the valid durable reader checkpoint into tail recovery as a trusted
  lower bound.
- Avoid revalidating or repairing bytes entirely before that checkpoint.
- Use the same candidate scanner to move past internal structural corruption.
- Continue validating later frames so the writer can recover the true append
  tail.
- Treat the highest segment as the active segment.
- Truncate an incomplete or unrecoverable active suffix to the last validated
  boundary at or after the checkpoint.
- Synchronize the truncated file before accepting new writes.
- Never modify internal corrupt ranges that have later valid frames.
- Emit recovery diagnostics for internal skips and truncation.

Startup tail discovery does not advance the consumer checkpoint. The consumer
must still process or explicitly skip unacknowledged ranges through ordered
progress.

### Segment reclaimer

Files:

- `reclaimer.rs`

Responsibilities:

- No new corruption-specific behavior.
- Continue deleting only sealed segments strictly before the durable
  reclaimable segment.

Once a corrupt range becomes durably checkpointed, existing reclamation rules
eventually remove any fully consumed sealed segment containing that range.

### Diagnostics and metrics

Each skipped range should report:

- Buffer identity or directory when available.
- Segment base offset.
- Starting and resumption byte offsets.
- Expected and resumed record IDs.
- Skipped logical bytes.
- Known lost event count, when it can be derived safely.
- The original corruption cause.

Recommended counters:

- Corrupt ranges skipped.
- Corrupt logical bytes skipped.
- Known records or events lost.
- Active-tail bytes truncated during startup.
- Terminal incompatible-format failures.

Unknown event loss should remain unknown rather than being reported as zero.
Diagnostics should be emitted once per recovery attempt; a crash before the
checkpoint becomes durable may legitimately report the same range again.

## Implementation phases

### Phase 1: Generalize ordered reader progress

- [ ] Rename `AcknowledgementToken` to `ReadProgressToken`.
- [ ] Rename `frame_bytes` to `logical_bytes`.
- [ ] Keep ordinary frame acknowledgement behavior unchanged.
- [ ] Add an internal helper that registers an immediately completed progress
  token.
- [ ] Prove that an immediate token remains blocked behind an earlier
  unfinished downstream frame.

Exit criteria:

- Existing acknowledgement, checkpoint, capacity, and reclamation tests pass.
- A synthetic skipped range cannot checkpoint ahead of an earlier record.

### Phase 2: Add single-segment resynchronization

- [ ] Add structured corruption classification for frame decode errors.
- [ ] Restore the physical cursor to the logical position on failed reads.
- [ ] Implement bounded streaming search for `FRAME_MAGIC`.
- [ ] Fully validate every recovery candidate.
- [ ] Reject regressing candidate record IDs.
- [ ] Return the candidate boundary and byte range without checkpointing it.
- [ ] Add protection against zero-progress scan loops.

Exit criteria:

- Corruption between two valid frames finds the later frame.
- Magic-like bytes inside corrupt payload data are rejected unless the entire
  candidate frame validates.
- No error path changes the logical position implicitly.

### Phase 3: Recover across segments

- [ ] Add `CorruptRange` to `SegmentedRead`.
- [ ] Resume at the next higher segment when no candidate remains locally.
- [ ] Calculate skipped logical bytes across segment boundaries.
- [ ] Use the next segment base offset as the resumed record ID.
- [ ] Bound recovery using the writer's committed position.
- [ ] Distinguish an older incomplete segment from the newest incomplete tail.

Exit criteria:

- A corrupt suffix in one sealed segment resumes at the next valid segment.
- Overlapping or regressing segment IDs remain rejected.
- Recovery never consumes uncommitted active-segment bytes.

### Phase 4: Integrate receiver-side skipping

- [ ] Teach `read_next_committed` to surface `CorruptRange` results.
- [ ] Emit data-loss diagnostics.
- [ ] Register immediate ordered progress for the range.
- [ ] Continue until a valid `T`, clean close, or terminal error is reached.
- [ ] Keep application decode and incompatible format failures terminal.
- [ ] Narrow the receiver terminal state to errors that cannot be skipped.

Exit criteria:

- A corrupt frame is skipped and the next valid record is delivered.
- The checkpoint remains behind the corrupt range while an earlier record is
  outstanding.
- Finalizing that earlier record allows both its frame and the corrupt range to
  become reclaimable in order.
- Restart begins at or after the durably skipped range.

### Phase 5: Add startup repair

- [ ] Start tail recovery from a valid durable checkpoint when available.
- [ ] Reuse resynchronization for internal corruption.
- [ ] Track the last validated boundary in the highest segment.
- [ ] Truncate an invalid newest suffix to that boundary.
- [ ] Synchronize truncation before opening the segment for append.
- [ ] Validate that recovered committed progress never precedes the durable
  checkpoint.
- [ ] Report internal corruption and truncated bytes.

Exit criteria:

- Restart succeeds with an incomplete newest header or payload.
- Restart succeeds with internal corruption followed by a valid frame.
- New writes append after the final validated boundary.
- Restart never truncates acknowledged data.

### Phase 6: Fault and integration coverage

- [ ] Corrupt each fixed header field independently.
- [ ] Corrupt payload bytes and checksum bytes.
- [ ] Insert valid magic bytes inside corrupt payload data.
- [ ] Corrupt data at segment start, middle, and end.
- [ ] Corrupt an entire sealed segment between valid segments.
- [ ] Exercise record-ID gaps and regressions.
- [ ] Crash before and after skip checkpoint persistence.
- [ ] Fail checkpoint persistence after a range is detected.
- [ ] Fail capacity release after checkpoint persistence and verify terminal
  handling.
- [ ] Verify reclaimer behavior after a skipped sealed segment becomes
  reclaimable.
- [ ] Run model tests with random corruption and acknowledgement ordering.

Exit criteria:

- All deterministic corruption cases preserve the required invariants.
- No test can move the durable checkpoint beyond unresolved earlier work.
- Restart either resumes at a validated boundary or returns a classified
  terminal error.

## Test matrix

| Scenario | Expected result |
| --- | --- |
| Corrupt payload between valid frames | Report one range, checkpoint it in order, return the later frame |
| Invalid header followed by valid frame | Scan from the failing boundary and validate the later frame |
| Magic bytes inside corrupt data | Reject false candidate and continue scanning |
| Regressing valid frame | Treat as recoverable structural loss and continue scanning |
| Corrupt suffix in sealed segment | Resume at next segment base offset |
| Incomplete newest suffix at startup | Truncate to last validated boundary and resume writer |
| Unsupported frame version | Return terminal incompatible-format error |
| Valid frame with unsupported metadata | Return terminal typed decode error initially |
| Earlier record still in flight | Hold skip progress behind its ordered finalization |
| Crash before skip checkpoint | Rediscover and report the range after restart |
| Crash after skip checkpoint | Resume after the skipped range |
| Checkpoint write failure | Fail actor without releasing logical capacity |

## Delivery sequence

Each phase should be independently reviewable and keep the buffer functional:

1. Generalize acknowledgement tokens without changing behavior.
2. Add single-segment scanning behind tests.
3. Add cross-segment corrupt-range results.
4. Enable runtime receiver skipping.
5. Enable startup repair and active-tail truncation.
6. Add broad fault injection, metrics, and model coverage.

Avoid combining runtime skip policy and startup file mutation in the same first
change. They share scanning machinery but have different durability risks.

## Open decisions

The following decisions should be resolved before enabling recovery by default:

- Whether structural skipping is always enabled or controlled by an internal
  `Halt`/`Skip` policy during rollout.
- Whether unsupported flags and payload codecs are incompatible data or
  recoverable corruption for frame version 1.
- Whether typed decode failures receive a separate opt-in skip policy.
- Which existing Vector internal-event and discarded-event metrics can
  represent a range whose event count is unknown.
- Whether a segment containing internal corruption should be retained for
  diagnostics after its checkpoint becomes reclaimable.
- How much data one recovery attempt may scan before yielding or returning a
  terminal error.

## Completion criteria

Corruption recovery is complete when:

- Runtime reads resume after structural corruption at a fully validated
  boundary.
- Skipped data advances through ordered durable progress.
- Capacity and segment reclamation remain exact.
- Startup can recover internal corruption and incomplete active tails.
- Incompatible formats remain distinguishable from corrupt bytes.
- Operators receive durable, actionable evidence of every data-loss range.
- All focused tests, `make check-clippy`, `make check-fmt`, and
  `make check-markdown` pass.
