# Disk V3 Code Review

This review covers the production code and tests under
`lib/vector-buffers/src/variants/disk_v3`, along with the related Disk V3 CLI.
It records correctness issues, scalability concerns, and known production
blockers in the current implementation.

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

### High: reader corruption is skipped without resynchronization or loss accounting

`ReadableSegment::read_next` advances the underlying file cursor while reading
the frame header and payload. If header decoding, payload validation, or the
checksum fails, the logical `Position` remains unchanged but the file cursor is
not restored. Calling `read_next` again can consequently begin at an arbitrary
point after some or all of the corrupt frame. This may happen to reach the next
frame, but it neither proves that the cursor is at a frame boundary nor records
the skipped data as lost.

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
gap. An incomplete newest tail remains a separate crash-recovery case: startup
should truncate it to the last valid boundary. A completely validated frame
whose record ID regresses should be reported and skipped as invalid or
duplicated data rather than terminating the reader.

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

### Medium: manual acknowledgements can arrive out of order

**Status: resolved.** `DiskBufferReceiver` now registers every emitted frame
with Vector's `OrderedFinalizer<AcknowledgementToken>`. Downstream work may
complete out of order, but the writer actor receives completed tokens only in
the original read order. There is no public manual acknowledgement operation,
token sequence number, or separate `AcknowledgementTracker`.

The token retains only the frame's ending position and logical byte count. The
actor synchronizes committed data, persists that position, releases those
bytes, and publishes the new reclaimable position for each ordered completion.
A regression test finalizes the second of two frames first and verifies that
the checkpoint does not advance until the first frame also completes.

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

## Scalability findings

### Recovery validates the entire retained log

`SegmentedLogReader::recover_tail` starts at the earliest retained segment and
reads and checksums every frame. Startup time is therefore proportional to all
retained bytes rather than the active segment's size.

This is reasonable for an initial implementation, but it will become expensive
with large segments and a long retained log. Once sealed segments have durable
metadata or a trusted terminal boundary, recovery should validate the segment
catalog and scan only the newest active segment.

### Every acknowledgement performs synchronous durability work

Each acknowledgement currently synchronizes committed data, synchronously
flushes the memory-mapped checkpoint, and releases logical capacity. Performing
that work once per frame serializes synchronization through the writer actor
and can substantially limit throughput.

After the correctness semantics are established, the actor should consider
batching the largest contiguous acknowledged prefix. Batching may be driven by
bytes, elapsed time, explicit flush, and graceful shutdown.

## Known production blockers

### Incomplete crash tails are not recovered

An incomplete newest frame currently makes tail recovery fail. The writer then
cannot reopen the buffer without manual repair.

Recovery should distinguish an incomplete tail in the newest segment from
corruption in an earlier sealed segment. It should truncate the newest segment
to the last validated frame boundary and synchronize the repaired file before
resuming writes. Corruption in a sealed segment should remain fatal.

### The data directory has no exclusive owner lock

The code assumes exclusive ownership of the buffer directory, but it does not
enforce that assumption. Two buffer instances can open the same active segment
for append, bypassing the protection provided by `create_new` during segment
creation.

Opening a `DiskBuffer` should acquire an exclusive lock for the lifetime of its
sender, receiver, and actor. A second open of the same directory should fail
clearly.

### Acknowledged sealed segments are never deleted

Logical capacity is released after a durable checkpoint, but the physical
segment files remain on disk. Producers can continue to receive logical
capacity while physical disk consumption grows without bound.

Segment reclamation should be driven by durable checkpoint advancement. After
the checkpoint is persisted, a reclamation component can delete sealed
segments that lie entirely before the reclaimable position and then
synchronize the directory. The active segment must never be deleted.

## Positive observations

The following parts of the implementation are well structured:

- The frame format is isolated from segment and queue concerns and validates
  lengths and checksums.
- Frames never span segments, keeping rotation and recovery boundaries simple.
- `Position` clearly separates segment record base offsets, segment-local byte
  offsets, and the next record ID.
- The single writer actor establishes a clear owner for mutation and preserves
  MPSC command order.
- Logical capacity reserves bytes before enqueueing and uses checked atomic
  updates to prevent overcommit and underflow.
- The writable and readable segment abstractions have focused unit tests,
  while the Disk Buffer harness exercises multi-segment round trips and actor
  behavior.

At the time of this review, all 58 Disk V3 tests and the repository Clippy check
passed. Passing tests do not cover the failure modes identified above, so the
listed regression tests should be added as the corresponding behavior is
fixed.
