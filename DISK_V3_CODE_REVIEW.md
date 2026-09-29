# Disk V3 Code Review

This review covers the production code and tests under
`lib/vector-buffers/src/variants/disk_v3`, along with the related Disk V3 CLI.
It records correctness issues, scalability concerns, design observations, and
known production blockers in the current implementation.

Disk v3 is declared as `#[allow(dead_code)] pub(crate) mod disk_v3` and is not
reachable from the topology, so none of the findings below affect a running
Vector today. They are ordered by the severity they would carry once the
adapter lands.

## Correctness findings

### Critical: ingress finalizers resolve before data is durable

**Status: resolved.** `SegmentedLogWriter` now owns a durability observer and
notifies it at the single point where `data_synced` advances. The production
observer retains ingress finalizers in record order and resolves only the
durable prefix. Fatal failure and unexpected observer drop mark the
unsynchronized suffix as errored.

`PreparedRecord` owns the ingress finalizers. The original writer actor borrowed
the record while appending it and then dropped the record at the end of the
append command arm. At that point, the frame could still exist only in the
writer's aggregation buffer and might not have been published or synchronized.

An event finalizer defaults to `EventStatus::Dropped`, which does not change a
batch's default delivered status. Dropping these finalizers can therefore tell
an upstream source that its data was successfully handled even though a crash
could still lose it.

The implemented fix retains finalizers together with the next record ID at
their frame boundary. It resolves finalizers successfully only after
`data_synced` reaches the corresponding boundary. If the writer encounters a
fatal error, it resolves all queued and retained finalizers as errored.

Regression coverage:

- A finalizer remains pending after a send whose frame is still aggregated.
- Publishing without synchronizing does not resolve the finalizer.
- A successful synchronization resolves all finalizers covered by the new
  durable position.
- A fatal write or synchronization error resolves retained finalizers as
  errored.

### High: acknowledgement publishes unrelated buffered writes

**Status: resolved.** Acknowledgement now calls `sync_committed`, so it makes
already-published data durable without publishing the current partial batch.

The original acknowledgement path called `SegmentedLogWriter::sync_all`. That
method first publishes the current aggregation batch and then synchronizes
committed data.

This created the following behavior:

1. Record A is committed, read, and ready to be acknowledged.
2. Record B is appended but remains in a partial aggregation batch.
3. A is acknowledged.
4. Synchronizing A also publishes B, even though no flush was requested and B
   did not reach the batch threshold.

An acknowledged frame must already have been committed before the receiver
could read it. The acknowledgement path now calls `sync_committed` and does not
publish the current aggregation batch.

A regression test commits and reads A, appends a partial B, acknowledges A, and
verifies that B remains unavailable until an explicit flush publishes it.

### High: a reader failure stalls the buffer without failing it

`DiskBufferReceiver::next` treats every `SegmentedLogReaderError` as terminal
for the receiver and sets its `failed` flag. The writer actor is not informed.
`PublishedWriterState::status` remains `WriterStatus::Running`, so producers
continue to pass `ensure_status_running`, continue to be admitted, and continue
to charge logical capacity.

Once occupancy reaches the high-water mark, `enqueue_append_while_running`
parks on `state.changed()` waiting for a capacity release. Capacity is released
only by `WriterActor::checkpoint_read_progress`, which runs only when the
receiver produces an acknowledgement. The receiver has already stopped
producing them.

A single corrupt frame, a `CommittedDataUnavailable`, or a decode failure
therefore stops the pipeline silently, with a healthy-looking writer status and
no error surfaced on the write side.

Terminal read failure must be propagated to the actor so that the buffer enters
`WriterStatus::Failed`, producers fail fast with the underlying reason, and
retained ingress finalizers resolve as errored. A dedicated failure command, or
treating an early close of the ordered finalizer stream with outstanding tokens
as fatal, would both work.

Required tests:

- A corrupt committed frame fails a blocked producer instead of parking it
  indefinitely.
- A record decode failure moves the published writer status to `Failed`.

### High: a zero-filled crash tail is not recognized as an incomplete tail

`SegmentedLogReader::recover_tail` distinguishes `SegmentRead::IncompleteTail`,
which reports a short final frame, from `ReadableSegmentError::Frame`, which
reports corruption. Only the first case is a candidate for the planned
truncate-and-resume repair.

The most common crash shape does not produce a short file. With delayed
allocation on ext4 and XFS, a crash after the file size has been extended but
before the data reaches the device leaves a full-length file whose tail is
zeros. Those zero bytes decode as `FrameDecodeError::InvalidMagic`, classified
as `RecoverableCorruption`, so `recover_tail` returns a hard error and
`SegmentedLogWriter::open_or_create` fails permanently.

Incomplete-tail recovery must therefore treat an unparseable run at the end of
the newest segment as a tail to truncate, not as sealed-segment corruption. The
distinction that matters is the segment's position in the log, not the specific
frame decode error.

Required tests:

- A newest segment whose final frame is overwritten with zeros recovers by
  truncating to the last validated boundary.
- The same zero-filled region inside a sealed segment remains fatal.

### High: reader corruption is not resynchronized or accounted for

**Status: partially addressed.** An earlier revision of this review stated that
`ReadableSegment::read_next` left the file cursor at an arbitrary offset after a
failed frame. That is no longer true: `restore_after_frame_error` seeks back to
the frame's starting offset, and `invalid_header_restores_the_read_position` and
`checksum_mismatch_restores_the_read_position` cover it.

The remaining behavior is a deterministic permanent stop. Every subsequent
`read_next` re-reads the same corrupt frame and returns the same error, which
combines with the stall described above to wedge the buffer.

Disk v3 should use a best-effort policy rather than stop permanently at a
corrupt frame. After corruption, the reader should preserve its last valid
logical position and explicitly search for the next valid frame boundary. It
should scan forward from one byte after the corrupt frame's starting offset,
look for `FRAME_MAGIC`, validate each candidate header and complete-frame
checksum, and reject candidates whose record ID regresses. Only a completely
validated candidate may become the new read boundary.

The reader should represent the skipped region explicitly, for example with an
internal `SegmentedRead::DataLoss` result containing:

- the segment and skipped byte range;
- the expected record ID before the loss;
- the record ID at which reading resumed, when known; and
- the corruption reason.

The high-level buffer should consume this result internally, emit a warning and
data-loss metric, and continue until it can return the next valid record. The
record-ID difference between the expected position and the next valid frame or
segment base offset provides the number of lost events when such a boundary is
available.

Skipped data must also participate in ordered acknowledgement progress. The
buffer may treat the corrupt span as internally dropped only after all earlier
emitted records have been acknowledged. It may then persist the recovered
position and release the skipped logical bytes. A skipped span must never move
the durable checkpoint past an earlier outstanding acknowledgement.

If no valid frame remains in the current segment, the reader should attempt to
resume at the next segment and use its base offset to identify the record-ID
gap. A completely validated frame whose record ID regresses should be reported
and skipped as invalid or duplicated data rather than terminating the reader.

Required tests:

- A corrupt payload between two valid frames reports one loss region and then
  returns the later valid frame.
- An invalid header resynchronizes at the next fully validated frame rather
  than continuing from the incidental file cursor.
- Magic bytes inside corrupt payload data are not accepted without a valid
  header and complete-frame checksum.
- A validated frame with a regressing record ID is skipped and reading
  continues at the next valid record.
- Corruption through the end of one segment resumes at the next valid segment.
- A corrupt span cannot advance the checkpoint or release logical capacity
  ahead of an earlier outstanding acknowledgement.

### Medium: `incomplete_tail` is a sticky latch reachable from corruption

`ReadableSegment::read_next` sets `self.incomplete_tail` when the declared
frame length would cross `segment_byte_limit`. On a corrupt length field that
records a corruption event as an incomplete tail. The flag is cleared only by
`seek_to`, and `SegmentedLogReader` calls `seek_to` only from
`restore_after_position_error`, so the segment reports `IncompleteTail` for the
rest of its life.

`SegmentedLogReader::read_next` then maps that result to
`CommittedDataUnavailable`, which reports missing committed data when the real
cause is a bad header. The existing
`declared_frame_length_cannot_cross_the_read_limit` test pins this behavior.

Reads should distinguish "these bytes are not published yet" from "these bytes
are unparseable" as separate outcomes rather than folding both into a latched
tail flag. This is also a prerequisite for the resynchronization work described
above, which needs the two cases to be separable.

### Medium: startup logical capacity can be permanently over-counted

`calculate_logical_bytes` derives initial occupancy from raw file bytes between
the reclaimable position and the committed position.
`WriterActor::checkpoint_read_progress` releases occupancy per acknowledged
frame, using that frame's `frame_len`.

The two accounting schemes are different in kind. Every byte in the recovered
span that the reader will never return as a frame is charged at startup and
never released:

- a segment skipped by `SegmentedLogReader::try_open_at`'s fall-forward to the
  next higher base offset when the requested file is missing;
- a record-ID gap inside a segment, which `finish_frame` warns about and
  tolerates; and
- a segment skipped by `advance_segment` when base offsets are not contiguous.

Occupancy then has a permanent floor that survives every restart, and the
buffer eventually admits nothing. The recovery scan and the release path must
agree: either seed occupancy by scanning frames, or have the reader release the
logical bytes of a span at the point where it decides to skip it.

### Medium: shutdown discards in-flight acknowledgements

`WriterCommand::Shutdown` calls `close`, which synchronizes data, then publishes
`Closed`, fails pending commands, and returns. It never polls
`self.finalizations`. Acknowledgements for frames that were already delivered
downstream but whose tokens are still in the ordered stream are therefore
dropped, and those frames replay on restart.

This stays inside at-least-once delivery, but graceful shutdown is exactly the
point where that progress should be harvested. Shutdown should perform a
bounded drain of the finalization stream, checkpoint the largest contiguous
acknowledged prefix, and only then close the command queue.

### Medium: configuration validation occurs after filesystem mutation

`DiskBuffer::open` validates the command queue capacity and synchronization
interval before performing I/O, but other validation occurs later while
opening the writer and logical capacity tracker. Invalid configuration can
therefore leave a checkpoint or segment file behind.

Configuration should be validated in one `DiskBufferConfig::validate` method
before creating the directory or any files. At minimum, it should validate:

- nonzero command queue capacity;
- nonzero synchronization interval;
- nonzero logical capacity;
- nonzero writer batch size;
- nonzero segment size;
- a maximum frame length large enough to hold the fixed frame header;
- a segment size large enough to hold a frame header; and
- a maximum payload representable by the wire format's `u32` payload length.

### Medium: manual acknowledgements can arrive out of order

**Status: resolved.** `DiskBufferReceiver` now registers every emitted frame
with Vector's `OrderedFinalizer<ReadProgressToken>`. Downstream work may
complete out of order, but the writer actor receives completed tokens only in
the original read order. There is no public manual acknowledgement operation,
token sequence number, or separate `AcknowledgementTracker`.

The token retains only the frame's ending position and logical byte count. The
actor synchronizes committed data, persists that position, releases those
bytes, and publishes the new reclaimable position for each ordered completion.
A regression test finalizes the second of two frames first and verifies that
the checkpoint does not advance until the first frame also completes.

### Medium: a validly encoded checkpoint is not bounded by the recovered log

Checkpoint decoding verifies the magic, version, checksum, and stored segment
byte offset. Startup does not explicitly verify that the complete checkpoint
position is at or before the writer's recovered committed position.

For example, a checksum-valid but semantically invalid checkpoint at the end of
a segment can contain an impossible `next_record_id`. With no following frame
header to validate it against, startup can accept the position and report
`ReaderBeyondCommitted` only when the receiver is used.

After recovering the writer tail, startup should validate the checkpoint
against the available log. A checkpoint outside the recovered bounds should be
treated like other invalid checkpoint data: warn and replay from the earliest
retained segment.

### Low: a failed progress-token construction advances the reader silently

`DiskBufferReceiver::next` advances the read position before calling
`register_acknowledgement`. If `ReadProgressToken::for_frame` fails, the error
propagates without setting the receiver's `failed` flag, so the next call reads
the following frame. No token was registered for the skipped frame, so a later
acknowledgement checkpoints past it and its logical bytes are never released.

The failure is unreachable on supported platforms, because it only triggers
when a `usize` frame length does not fit in a `u64`. It nevertheless fails open.
Construct the token before advancing the reader, or set `failed` on this path.

### Low: the reclaimer watermark has a second source of truth

`DiskBuffer::open_inner` seeds the reclaimer's watch channel from
`reclaimable.segment_base_offset()` but seeds the published writer state from
`reader.read_position()`. These differ when `try_open_at` falls forward past a
missing segment.

The current behavior is safe, because the reclaimer receives the lower and more
conservative value, and the first acknowledgement corrects it. It is still an
unnecessary second derivation of the same fact and should read from the
reader's actual position.

## Scalability findings

### Every acknowledgement performs synchronous durability work

`WriterActor::checkpoint_read_progress` synchronizes committed data,
synchronously flushes the memory-mapped checkpoint, and releases logical
capacity for every single acknowledged frame, serialized through the actor.
This is the throughput ceiling of the whole design.

The actor should coalesce instead: greedily drain the ordered finalization
stream, compute the largest contiguous acknowledged prefix, and perform one
synchronization, one checkpoint persist, and one capacity release for the
entire run. Coalescing may additionally be bounded by bytes, elapsed time,
explicit flush, and graceful shutdown.

### The actor's biased select prefers durability work over accepting writes

`WriterActor::run` uses `tokio::select!` with `biased`, ordering the
synchronization timer and the finalization stream ahead of the command queue.
Under load the actor therefore prefers per-frame durability work to accepting
appends. It self-limits rather than deadlocking, because acknowledgements
depend on reads, which depend on commits, which depend on appends, but the
result is a bursty alternation instead of steady progress.

Dropping `biased`, or draining a bounded batch of commands each iteration,
would smooth this out. This interacts directly with acknowledgement coalescing
above and should be addressed together with it.

### Segment synchronization reopens the file by path

`FilesystemSegmentStorage::sync_segment` opens a fresh handle to
`<base_offset>.log` and calls `sync_all` on it. That happens on every
synchronization timer tick and, today, on every acknowledgement.

`WritableSegment` already owns a handle to exactly that file. Giving it a
`sync` method that calls `sync_data` on its own descriptor removes an
open-and-close pair per synchronization, avoids flushing metadata that an
append-only file does not need, and eliminates a path-based operation that can
in principle race with reclamation.

### Recovery validates the entire retained log

`SegmentedLogReader::recover_tail` starts at the earliest retained segment and
reads and checksums every frame. `DiskBuffer::open_inner` then scans the
directory twice more through `calculate_logical_bytes`. Startup time is
therefore proportional to all retained bytes rather than the active segment's
size.

This is reasonable for an initial implementation, but it will become expensive
with large segments and a long retained log. Once sealed segments have durable
metadata or a trusted terminal boundary, recovery should validate the segment
catalog and scan only the newest active segment.

## Design findings

### The durability observer inverts a dependency the actor already owns

`DurabilityObserver` costs a second generic parameter on `SegmentedLogWriter`, a
boxed trait object, `with_durability_observer`, `register_durability`,
`notify_durability_failure`, and a `Drop` implementation whose only job is to
catch a path that the explicit `on_failure` call should already cover. The
writer, which otherwise knows nothing about events, becomes generic over a
pending-item type that exists solely to carry finalizers.

The only reason the callback exists is that `append` can synchronize internally
by way of `rotate`. If `append` reported the resulting `data_synced` position,
or if the actor simply re-read `writer.data_synced()` after each writer call, the
actor could own the pending-finalizer queue outright. It already calls `publish`
at every one of those points. `IngressFinalizerTracker` is actor-local state
reaching back into the writer to learn when to run.

Collapsing this removes the trait, the type parameter, the allocation, and the
drop-based error path, and it makes the durability rule readable in one place.

### Four components independently scan the segment directory

`SegmentedLogReader` lists the directory on every `advance_segment` and every
fall-forward, `SegmentReclaimer` lists it on every watermark change and retry,
`calculate_logical_bytes` lists it twice at startup, and `recover_tail` lists it
once more. Each sees its own snapshot, and no component owns the set of
segments.

The reader's "open the next higher base offset when the requested file is
missing" rule is what makes those divergent snapshots tolerable, and it is also
the rule that silently skips data in the capacity finding above.

A single owned segment catalog, mutated on rotation and on reclamation and read
by everyone else, would remove the repeated `read_dir` calls and the ambient
coupling between the reader, writer, reclaimer, and capacity calculator. This is
the structural change worth making first, because several findings above are
downstream of it.

### One watch channel serves three unrelated consumers

`PublishedWriterState` carries the committed position, the durable position,
acknowledgement progress, the reclaimable position, and status in one struct
behind one watch channel. Readers care about `committed`, producers care about
capacity release, and both care about status.

Because `publish` compares the entire struct with `send_if_modified`, every
commit wakes every producer blocked on capacity and every acknowledgement wakes
the reader. With many producers parked on capacity this is a thundering herd on
each acknowledgement.

Splitting the channel by consumer, for example a `watch<Position>` for
committed data, a `watch<WriterStatus>` for status, and a purpose-built
primitive for capacity, removes the spurious wakeups and makes each wakeup
condition explicit.

### Capacity admission is a hand-rolled, unfair semaphore

`DiskBufferSender::enqueue_append_while_running` checks the high-water mark,
reserves a command-queue permit, rechecks status, calls `try_acquire`, and loops
when another producer won the race. The comment correctly documents that the
race is intentional, but the result is an unfair, wakeup-driven retry loop
reimplementing a semaphore.

`tokio::sync::Semaphore` with byte-denominated permits provides FIFO fairness
and removes the loop. The atomic counter would remain only as the
`occupied_bytes` gauge.

### Corruption is modeled as an error rather than an outcome

Segment reads split their results across `Ok(SegmentRead)` and
`Err(ReadableSegmentError::Frame)`. For this layer, corruption is a routine
outcome rather than an exceptional one; that premise is the entire basis of
`DISK_V3_CORRUPTION_RECOVERY_PLAN.md`.

Folding it into the success type, for example
`Frame | EndOfAvailableData | IncompleteTail | Corrupt { range, reason }`, makes
the resynchronization and loss-accounting design expressible, and it removes the
need for the latched `incomplete_tail` flag described above.

### Position comparisons are not uniform

`SegmentedLogWriter::sync_data` orders positions by `next_record_id` alone.
`SegmentedLogReader::read_next` compares both `next_record_id` and
`segment_base_offset`. `finish_frame` compares `next_record_id` again against a
separately captured expectation.

Each comparison is correct given the current invariants, but the governing
invariant, that `next_record_id` is monotonic across segments, is implicit and
restated informally at each site. A single `Ord` implementation on `Position`
with that invariant documented on the type would make an inconsistent
comparison much harder to introduce.

## Known production blockers

### Incomplete crash tails are not recovered

An incomplete newest frame currently makes tail recovery fail. The writer then
cannot reopen the buffer without manual repair.

Recovery should distinguish an incomplete tail in the newest segment from
corruption in an earlier sealed segment. It should truncate the newest segment
to the last validated frame boundary and synchronize the repaired file before
resuming writes. Corruption in a sealed segment should remain fatal. As noted
above, the truncation rule must key off the segment's position in the log
rather than the specific frame decode error, so that zero-filled tails are
covered.

### The data directory has no exclusive owner lock

The code assumes exclusive ownership of the buffer directory, but it does not
enforce that assumption. Two buffer instances can open the same active segment
for append, bypassing the protection provided by `create_new` during segment
creation. The checkpoint mapping's safety comment also asserts exclusive
ownership that nothing currently guarantees.

Opening a `DiskBuffer` should acquire an exclusive lock for the lifetime of its
sender, receiver, and actor. A second open of the same directory should fail
clearly.

### Acknowledged sealed segments are never deleted

**Status: resolved.** `SegmentReclaimer` runs as its own task, watches the
reclaimable segment base offset published by the writer actor, deletes sealed
segments strictly below it, never deletes the highest base offset on disk, and
synchronizes the directory after successful removals. Deletion failures are
retried on a fixed delay without rolling back the durable checkpoint.

The checkpoint is persisted before the new watermark is published, so a segment
is only eligible for deletion after a durable checkpoint has moved past it.

## Positive observations

The following parts of the implementation are well structured:

- The frame format is isolated from segment and queue concerns and validates
  lengths and checksums.
- Frames never span segments, keeping rotation and recovery boundaries simple.
- `Position` clearly separates segment record base offsets, segment-local byte
  offsets, and the next record ID.
- The single writer actor establishes a clear owner for mutation and preserves
  MPSC command order.
- Rotation always synchronizes the outgoing segment before creating the next
  one, so only the active segment can ever be unsynchronized.
- Logical capacity reserves bytes before enqueueing and uses checked atomic
  updates to prevent overcommit and underflow.
- `FinalizerGuard` is now shared between disk v2 and disk v3 rather than
  duplicated, and it makes every early return on the send path explicit about
  finalizer ownership.
- The writable and readable segment abstractions have focused unit tests,
  while the Disk Buffer harness exercises multi-segment round trips and actor
  behavior.

At the time of this review, all 72 Disk V3 tests passed. Passing tests do not
cover the failure modes identified above, so the listed regression tests should
be added as the corresponding behavior is fixed.
