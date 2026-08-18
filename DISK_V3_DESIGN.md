# Disk V3 MPSC Buffer Design

This document proposes a rewrite of Vector's disk buffer as a multiple-producer,
single-consumer (MPSC) buffer backed by a single-writer, single-reader,
append-only segmented log. Multiple topology producers submit records through a
bounded command queue. A dedicated writer actor exclusively owns the writable
file, record ordering, batching, checkpoints, and all in-flight I/O state.

The design makes record acceptance, publication, durability, and
acknowledgement explicit while removing shared file-handle ownership and
condition-variable-style coordination.

This is a draft design plan rather than an accepted RFC.

## Context

Disk v2 combines several distinct states under the idea that a record has been
"written":

- Vector may still hold the record in an aggregation buffer.
- Tokio may have accepted the bytes while its blocking filesystem write is
  still running.
- The kernel may expose the bytes through the page cache before Vector has
  published the corresponding ledger state.
- Vector may publish ledger state before the bytes become readable.
- The data may be readable without having been synchronized to durable storage.

The current reader and writer coordinate through mutable ledger fields and
notifications. Correctness therefore depends on the ordering among file I/O,
ledger publication, wake-ups, and cancellation of asynchronous operations.

Disk v3 replaces this coordination with a single-owner writer actor, an
exclusive reader, and monotonic watermarks. The watermarks are authoritative;
runtime notification channels are only an efficient way to learn that a
watermark may have advanced.

## Scope

### In scope

- Multiple concurrent producers and exactly one consumer per buffer.
- A bounded MPSC command queue with byte-aware capacity reservations.
- A dedicated writer actor that exclusively owns and awaits all write I/O.
- An exclusive reader stream.
- Append-only segment files with a versioned frame format.
- Explicit accepted, committed, data-synchronized, durable,
  observed-acknowledged, and reclaimable-acknowledged progress.
- Batched writes and group synchronization.
- Logical capacity recovered from the segment catalog and frame-boundary
  positions, then maintained through checked exact-byte transitions.
- Crash recovery from checkpoints and segment scanning.
- Asynchronous segment deletion.
- Deterministic fault-injection and model-based testing.
- A migration path that does not alter the disk-v2 format in place.

### Out of scope

- Multiple concurrent consumers.
- Concurrent ownership of the writable file or on-disk writer state.
- In-place mutation of disk-v2 files.
- Random access to buffered records.
- Exactly-once delivery across process or host failure.
- Removing all memory buffering.
- A `writev` optimization before profiling demonstrates that record copying is
  a material cost.

## Goals

Disk v3 has the following primary goals:

- A cancelled producer operation never causes an accepted record or partial
  batch to be written twice.
- A reader never consumes bytes beyond the writer's committed boundary.
- Logical publication never gets ahead of a failed filesystem write.
- Durability acknowledgement is tied to a completed synchronization boundary.
- A notification race cannot permanently stall the reader or writer.
- Logical occupancy cannot drift from independently maintained counters.
- A permanent I/O failure becomes an explicit buffer failure rather than an
  infinite wait.
- The common path retains the throughput benefits of batching sequential
  writes.

## Non-goals and delivery semantics

Disk v3 does not promise exactly-once delivery. A crash can occur after data is
durable but before its upstream acknowledgement is observed, or after sink
delivery but before the reader checkpoint is durable. Replaying that data can
produce duplicates.

Disk v3 does distinguish visibility from durability. Users and internal callers
must not interpret a committed record as one that is guaranteed to survive a
host or storage failure.

## Architecture

The topology-facing buffer is MPSC, while the on-disk log has one writer and
one reader:

```text
BufferSender clones
    |   |   |
    +---+---+----> bounded command queue
                          |
                          v
                    Writer actor
              owns files, ordering,
              batches, and checkpoints
                          |
              committed/durable/error state
                          v
                     DiskReader
                          |
                contiguous acknowledgement
                          +--------> Writer actor
```

Vector can connect multiple sources or transforms to the same component input,
and its `BufferSender` is cloneable. Disk v3 therefore cannot expose an
exclusive writer endpoint directly to topology producers. Each sender clone
submits commands to the writer actor instead.

The writer actor exclusively owns the writable file handle, record-ID
assignment, batch construction, segment rotation, checkpoint updates, capacity
accounting, and all in-flight write and synchronization state. `DiskReader`
exclusively owns the readable file handle and decoder state. No producer holds a
file-handle mutex, and producer cancellation cannot destroy the underlying I/O
future.

The endpoints exchange retained state through channels such as Tokio `watch`
channels and the writer command queue:

- Producers submit append and flush commands through a bounded MPSC channel.
- The writer publishes the latest committed and durable positions, together
  with terminal failure or graceful-close state.
- The reader submits the latest contiguous observed acknowledgement to the
  writer actor.
- The writer publishes capacity progress or releases byte reservations after
  acknowledgements become reclaimable.

Notifications are not progress themselves. A receiver always compares its
local position with the latest retained state. Missing or coalescing a
notification cannot lose progress.

### Producer ordering and acceptance

There is no global ordering guarantee among producers that submit concurrently.
The writer actor's receive order defines the log order and record-ID order.
Ordering within one producer is preserved when that producer awaits each send
in order.

An append becomes accepted when ownership of the record and its capacity
reservation has transferred irrevocably to the writer actor. Cancellation
before that linearization point leaves the record unaccepted. Cancellation
after it does not remove the command or stop its write; the actor continues even
if the producer drops its response future.

The producer prepares the record before queueing it so it can reserve exact
capacity without assigning a record ID. The command contains:

- Its encoded payload and codec metadata.
- Its event count and exact framed byte length.
- Its ingress durability finalizers.
- An owned capacity reservation.

Successful insertion into the bounded queue is the acceptance linearization
point: the reservation and prepared record transfer together. Cancelling the
queue send before it completes leaves the command unqueued and releases the
reservation. Record IDs are assigned only by the writer actor, in receive order.
If a full or overflow result must return the original record after encoding, the
producer adapter recovers it using the same encode/decode approach as disk v2
and reattaches its finalizers.

## High-level components and interfaces

The following Rust-like interfaces show ownership and communication boundaries.
They are design sketches rather than final public APIs.

The initial implementation has a single-owner `DiskBuffer` facade over the
low-level reader, writer, and logical-capacity manager:

```rust
struct DiskBuffer {
    writer: SegmentedLogWriter<FilesystemSegmentStorage>,
    reader: SegmentedLogReader,
    logical_capacity: LogicalCapacity,
    acknowledgements: AcknowledgementTracker,
}

struct AcknowledgementTracker {
    next_sequence: u64,
    pending: VecDeque<PendingAcknowledgement>,
    observed_position: Position,
    reclaimable_position: Position,
    uncheckpointed_bytes: u64,
}

struct AcknowledgementToken { /* opaque, single-use */ }

enum DiskBufferRead {
    Frame {
        frame: OwnedDecodedFrame,
        acknowledgement: AcknowledgementToken,
    },
    EndOfAvailableData,
    IncompleteTail,
}

impl DiskBuffer {
    async fn open(
        directory: PathBuf,
        reclaimable: Position,
        max_frame_len: usize,
        max_logical_bytes: u64,
        writer_config: SegmentedLogWriterConfig,
    ) -> Result<Self, DiskBufferError>;

    fn capacity(&self) -> LogicalCapacity;
    async fn append(
        &mut self,
        frame: PreparedFrame,
        reservation: LogicalCapacityReservation,
    ) -> Result<(), DiskBufferError>;
    async fn next(&mut self) -> Result<DiskBufferRead, DiskBufferError>;
    fn acknowledge(
        &mut self,
        acknowledgement: AcknowledgementToken,
    ) -> Result<Position, DiskBufferError>;
    async fn flush(&mut self) -> Result<Position, DiskBufferError>;
    async fn sync_all(&mut self) -> Result<Position, DiskBufferError>;
    fn logical_bytes(&self) -> u64;
}
```

`next` returns each frame with an opaque acknowledgement token. The token is
neither cloneable nor constructible by callers. The acknowledgement tracker
retains the corresponding ending position and encoded byte length, so callers
cannot forge either value. Vector has one reader owner per disk buffer, and that
owner also owns the buffer's finalizer, so tokens never cross buffer ownership
boundaries and need no separate runtime buffer identifier.

Vector's existing `OrderedFinalizer<AcknowledgementToken>` carries these tokens
through downstream finalization and emits them in original read order even when
later records finish first. The finalizer consumer passes each completed token
to `acknowledge`. This advances the in-memory observed acknowledgement only. A
future checkpoint operation will persist that position before moving the
reclaimable acknowledgement and releasing its encoded bytes from logical
capacity. The facade provides the correct initialization and ownership boundary
now; the MPSC producer adapter and writer actor can later wrap or split this
ownership without moving recovery logic back into the segmented writer.

```rust
struct DiskV3Sender<T> {
    command_tx: mpsc::Sender<WriterCommand<T>>,
    writer_state: watch::Receiver<PublishedWriterState>,
    capacity: CapacityHandle,
    preparer: RecordPreparer<T>,
}

impl<T> Clone for DiskV3Sender<T> { /* clones handles, not the writer */ }

impl<T: Bufferable> DiskV3Sender<T> {
    async fn send(&self, record: T) -> Result<(), BufferError>;
    async fn try_send(&self, record: T) -> Result<TryWriteOutcome<T>, BufferError>;
    async fn flush(&self) -> Result<(), BufferError>;
}

enum WriterCommand<T> {
    Append(AppendCommand<T>),
    Flush {
        completed: oneshot::Sender<Result<(), BufferError>>,
    },
}

struct AppendCommand<T> {
    record: PreparedRecord<T>,
    reservation: CapacityReservation,
}

struct PreparedRecord<T> {
    payload: Bytes,
    codec_metadata: u32,
    event_count: NonZeroUsize,
    framed_len: usize,
    ingress_finalizers: EventFinalizerGroups,
    _record_type: PhantomData<T>,
}

struct RecordPreparer<T> { /* payload codec and frame sizing */ }

impl<T: Bufferable> RecordPreparer<T> {
    fn prepare(&self, record: T) -> Result<PreparedRecord<T>, BufferError>;
    fn recover(&self, record: PreparedRecord<T>) -> Result<T, BufferError>;
}

struct CapacityHandle { /* shared byte permits */ }
struct CapacityReservation { bytes: usize /* owned permit */ }

impl CapacityHandle {
    async fn reserve(&self, bytes: usize)
        -> Result<CapacityReservation, BufferError>;
    fn try_reserve(&self, bytes: usize)
        -> Result<CapacityReservation, CapacityFull>;
}
```

`DiskV3Sender` is the cloneable MPSC producer interface used by topology
transforms. It does not contain a file handle. It prepares the payload, reserves
its exact framed length, and transfers both into the command queue atomically
from the design's perspective. `send` completes when that transfer succeeds.
`try_send` preserves the existing block, drop, and overflow behavior and
recovers the original record if it cannot transfer the prepared record.
`flush` queues a command behind that sender's preceding append and waits for the
actor to publish all append commands ahead of it. Graceful shutdown still
uses its dedicated control path rather than a producer operation.

`CapacityReservation` moves with the prepared record. The actor retains its
exact framed byte count in queued or batched accounting until publication moves
those bytes into committed positional accounting. It releases the corresponding
byte permits only when the reclaimable acknowledgement advances.

The current single-owner implementation calls these types `LogicalCapacity`
and `LogicalCapacityReservation`. A producer acquires a reservation before
calling `DiskBuffer::append`. `append` validates that the reservation belongs to
that buffer and matches the frame's exact encoded length. Dropping a reservation
before successful append returns its bytes, while accepted bytes remain charged
until a durable reader checkpoint releases them. Acquiring the reservation
outside `append(&mut self, ...)` is essential: waiting while exclusively
borrowing the buffer would prevent the same owner from reading, acknowledging,
and checkpointing the data needed to free capacity.

```rust
struct WriterActor<T, S> {
    command_rx: mpsc::Receiver<WriterCommand<T>>,
    acknowledgement_rx: watch::Receiver<ObservedAcknowledgement>,
    control_rx: mpsc::Receiver<WriterControl>,
    published_state: watch::Sender<PublishedWriterState>,
    log: SegmentedLogWriter<S>,
    checkpoints: CheckpointStore<S>,
    capacity: CapacityManager,
    durability: PendingDurability,
    reclaimer: SegmentReclaimer<S>,
}

impl<T: Bufferable, S: Storage> WriterActor<T, S> {
    async fn run(mut self);
}

enum WriterControl {
    DeletionCompleted(DeletionResult),
    Shutdown(oneshot::Sender<Result<(), BufferError>>),
}
```

`WriterActor` is the only writable-state owner. It receives records from every
producer, assigns their record IDs in receive order, batches frames, drives the
file state machine, persists both writer and reader checkpoint progress,
releases capacity, and schedules deletion. A separate retained
acknowledgement channel prevents capacity progress from being trapped behind a
full append queue. Shutdown uses a control path that cannot be triggered by
dropping one producer clone.

```rust
struct SegmentedLogWriter<S> {
    storage: S,
    active_segment: WritableSegment<S::File>,
    committed: Position,
    data_synced: Position,
    next_record_id: u64,
    batch: AggregationBuffer,
    last_sync: Instant,
    sync_interval: Duration,
}

impl<S: Storage> SegmentedLogWriter<S> {
    async fn append(&mut self, frame: PreparedFrame)
        -> Result<Option<SealedSegment>, BufferError>;
    async fn flush(&mut self) -> Result<Position, BufferError>;
    async fn sync_all(&mut self) -> Result<Position, BufferError>;
}
```

`SegmentedLogWriter` is the single-writer append-only log engine. It knows
nothing about cloned topology producers. Its actor awaits each log operation to
completion without selecting it against a branch that discards the future.
Producer cancellation cannot cancel log I/O because producers own only MPSC
commands, not writer futures.

```rust
struct PublishedWriterState {
    committed: Position,
    durable: Position,
    lifecycle: WriterLifecycle,
}

enum WriterLifecycle {
    Running,
    Closing,
    Closed,
    Failed(Arc<BufferError>),
}

struct DiskV3Reader<T, S> {
    storage: S,
    cursor: Position,
    writer_state: watch::Receiver<PublishedWriterState>,
    acknowledgements: OrderedAcknowledgements,
    acknowledgement_tx: watch::Sender<ObservedAcknowledgement>,
    max_frame_len: usize,
}

impl<T: Bufferable, S: Storage> Stream for DiskV3Reader<T, S> {
    type Item = Result<T, BufferError>;
}
```

`DiskV3Reader` reads only through `PublishedWriterState::committed`. It decodes
one frame, attaches a downstream finalizer for the frame boundary, and emits the
record. Ordered finalization advances `acknowledgement_tx` only across a
contiguous prefix. Writer failure is stream failure; graceful close becomes end
of stream after the final committed position is consumed.

```rust
#[repr(C)]
struct CheckpointRecord {
    magic: [u8; 8],
    version: u32,
    checksum: u32,
    segment_base_offset: u64,
    segment_byte_offset: u64,
    next_record_id: u64,
}

struct ReaderCheckpoint {
    map: MmapMut,
    position: Option<Position>,
}

impl ReaderCheckpoint {
    async fn open(directory: &Path) -> Result<Self, CheckpointError>;
    fn position(&self) -> Option<Position>;
    fn persist(&mut self, position: Position) -> Result<(), CheckpointError>;
}
```

`ReaderCheckpoint` validates and synchronously flushes the single mmap record.
It is contained by and called only from `DiskBuffer`.

```rust
trait Storage: Clone + Send + Sync + 'static {
    type File: AsyncRead + AsyncWrite + Send + Unpin;

    async fn lock_buffer(&self, path: &Path) -> Result<DirectoryLock, io::Error>;
    async fn open_segment(&self, start: u64) -> Result<Self::File, io::Error>;
    async fn create_segment(&self, start: u64) -> Result<Self::File, io::Error>;
    async fn sync_file(&self, file: &Self::File) -> Result<(), io::Error>;
    async fn sync_directory(&self) -> Result<(), io::Error>;
    async fn truncate(&self, start: u64, len: u64) -> Result<(), io::Error>;
    async fn remove_segment(&self, start: u64) -> Result<(), io::Error>;
}
```

`Storage` is implemented by the production filesystem and by a deterministic
fault-injection implementation. It exposes the operations whose completion and
ordering affect correctness instead of hiding them behind a generic file
wrapper.

### Interaction summary

1. A producer prepares a record, reserves its exact framed length, and sends the
   reservation and `AppendCommand` together.
2. Successful queue insertion accepts the record. The actor dequeues it,
   assigns its record ID, and adds it to the current batch.
3. The producer's following flush command makes the log writer publish the
   pending batch and the actor publishes the new committed position. A full
   aggregation buffer or segment rotation may publish it earlier.
4. The reader observes that position, reads and decodes the frame, attaches an
   acknowledgement finalizer, and emits the record.
5. The actor synchronizes committed data, advances the durable position, and
   resolves ingress durability finalizers.
6. Downstream finalizers can complete out of order; the reader publishes only
   the largest contiguous observed acknowledgement.
7. The actor persists that acknowledgement, advances the reclaimable position,
   releases capacity, and schedules fully reclaimable segments for deletion.
8. Any permanent write or metadata error moves the actor to `Failed`, fails
   queued commands, and wakes every sender and the reader.

## Positions and watermarks

A position identifies an exact boundary between complete frames:

```rust
struct Position {
    segment_base_offset: u64,
    segment_byte_offset: u64,
    next_record_id: u64,
}
```

`segment_base_offset` is the record ID of the segment's first frame and is
encoded in its `<base_offset>.log` filename. `segment_byte_offset` is the byte
boundary within that one file. These are different coordinate systems and must
never be added or compared to one another. `next_record_id` identifies the next
frame expected after the boundary.

`next_record_id` is the progress ordering key at valid frame boundaries. The
segment base offset and byte offset are restart coordinates and recovery
witnesses. Segment base offsets are unique and monotonically increasing; a
normal rotation sets the next segment's base offset to the writer's
`next_record_id` and resets its byte offset to zero. The implementation must not
derive `Ord` lexicographically over the entire structure. An empty segment
rotation does not by itself advance record progress.

Disk v3 tracks these principal watermarks:

- **Accepted**: ownership and an exact byte reservation have transferred from a
  producer to the writer actor, but the actor may not have assigned a record ID
  yet.
- **Committed**: the complete filesystem write has succeeded and the reader may
  consume the frame.
- **Data synchronized**: segment files and required directory metadata have
  been synchronized through this position.
- **Durable**: the data-synchronized position has been persisted in a
  synchronized checkpoint.
- **Observed acknowledged**: downstream finalization has completed for every
  frame before this position.
- **Reclaimable acknowledged**: the observed acknowledgement has been persisted
  in a synchronized checkpoint and may be used to release logical capacity and
  delete segments.

They maintain these partial-order constraints:

```text
durable writer <= data synchronized <= committed
reclaimable acknowledged <= observed acknowledged <= reader emitted <= committed
```

There is no required ordering between downstream acknowledgement and the
durable writer position. A fast sink can finalize committed data before the next
writer synchronization. The implementation must represent those watermarks
independently.

Queued commands and frames in the aggregation buffer are not visible to the
reader and do not have a published `Position`. They are tracked as exact byte
totals plus the writer's next record ID. Only after the complete batch write and
file flush succeed does their boundary become the committed position.

## Writer actor design

### Command loop

The writer actor is the sole consumer of producer commands. Its event loop
drives command admission, record encoding and ID assignment, batching, file I/O,
manual flush requests, checkpoint updates, acknowledgement persistence,
deletion completion, shutdown, and terminal failure publication.

The command channel is bounded independently from disk capacity so that a
stalled writer cannot cause unbounded memory growth. Disk capacity is bounded
in bytes and includes accepted commands, even while they are still queued in
memory. `when_full: block`, `drop_newest`, and `overflow` continue to apply at
the producer-facing adapter.

The actor owns the authoritative runtime positions. Retained channels expose
snapshots to the reader and producers, but no other task mutates those
positions.

### Actor-owned asynchronous I/O

The writer actor calls ordinary asynchronous log methods and awaits each one to
completion. `WritableSegment::write_all` advances its physical offset after
each completed partial write, and that future remains owned by the actor until
it succeeds or fails. The actor does not place a newly constructed write future
in a `select!` branch where another message can cancel and recreate it.

While disk I/O is pending, producer commands remain in the bounded MPSC queue,
the acknowledgement watch channel retains its latest value, and control
messages remain queued. The actor processes them after the current I/O boundary
completes. This provides bounded backpressure without recreating partially
completed filesystem operations.

Graceful shutdown first closes admission, then awaits publication, data
synchronization, and checkpoint synchronization before dropping the active file
handle. The active segment remains appendable and can be reopened during
restart recovery. A forced actor abort or process termination does not attempt
in-place resumption; restart recovery finds the last valid framed prefix and
truncates an incomplete tail.

### Batching

Disk v3 retains a contiguous aggregation buffer. Small frames are serialized
into that buffer until one of these conditions occurs:

- The configured batch byte limit is reached.
- The configured batch event limit is reached.
- A queued flush command is processed.
- Segment rotation or shutdown is processed.
- A single frame is too large for the normal batch.

The actor can drain several ready commands from the MPSC queue and serialize
their frames into one aggregation buffer. When a batch is ready, the writer
moves its allocation into the actor-owned `write_all` operation and installs a
spare pooled allocation for subsequent records. The batch owns:

- Serialized bytes.
- First and next record IDs.
- Event and byte counts.
- Durability finalizers or completion receipts.

Awaiting the operation to completion avoids resubmission, and aggregation avoids
requiring one allocation per record. Once moved, the batch is not restored to
the aggregation buffer after an I/O failure; that failure terminates the writer
and restart recovery determines the last complete committed prefix.

### Producer flush semantics

Vector's fanout currently calls `send` followed by `flush` for every
`EventArray`. Disk v3 deliberately uses that existing manual drive, matching
disk v2:

- **Admission completion**: the actor owns the record and its reservation. A
  normal producer send may return.
- **Flush completion**: the actor has written and published every append ahead
  of that flush command. Readers can consume through the returned committed
  position.
- **Periodic durability**: while processing a flush, the writer checks whether
  500 ms has elapsed since its last synchronization. If so, it synchronizes the
  data and the actor completes the checkpoint durability sequence.
- **Close**: the actor stops admission, drains accepted records, publishes the
  final batch, and completes the durability sequence.

The disk-v3 adapter forwards ordinary `BufferSender::flush` as a queued writer
command and awaits its response. It always publishes pending data but does not
imply `fsync` on every call. Concurrent producers can still accumulate records
ahead of one flush command, but low-volume traffic commonly produces a
one-record batch. This is intentional and preserves disk-v2 behavior. Once a
flush command has entered the queue, cancellation of its response does not
cancel the actor's write or synchronization work.

### Publication

The publication sequence for a batch is:

1. Await `write_all` for the complete batch.
2. Await the file's `flush` operation when a manual flush command, aggregation
   limit, rotation, or shutdown requires publication.
3. On success, update the committed position.
4. Publish the committed position to the reader.
5. On failure, terminate the writer actor without publishing the batch.

The reader therefore never needs to infer publication from EOF or a record ID.
The actor reports a permanent error to all producer clones and the reader, and
resolves ingress finalizers for queued or in-flight records according to the
failure policy. It never retries an ambiguously partial append in place.

### Synchronization and durability acknowledgement

Synchronization is performed for a group of committed batches rather than for
every record. A group synchronization follows this order:

1. Complete all data writes through the selected committed position.
2. Synchronize the data file.
3. Advance the runtime durable watermark.
4. Resolve durability finalizers through that watermark.

Writer state is not checkpointed. Recovery derives the writer tail by scanning
complete frames in the segment files.

The first implementation preserves disk v2's 500 ms default synchronization
interval for behavioral continuity. This is an opportunistic interval rather
than a background timer: each manual flush compares the elapsed time with the
interval. The first flush at or after the deadline synchronizes the active
segment when it is dirty and then advances the durable watermark. Segment
directory entries are already durable because the directory is synchronized
immediately after every segment creation. Synchronizations cannot overlap
because the actor awaits each flush command to completion.

Only the active segment can require synchronization. The condition is derived
from `committed > data_synced`; it does not need a separate dirty flag. Rotation
publishes its final batch and synchronizes the file before converting it to
`SealedSegment`. A failed file synchronization prevents rotation and leaves
`data_synced` behind `committed`. Consequently, sealed segments never require
later synchronization and the writer needs neither dirty-segment bookkeeping
nor a file-synchronized position.

If traffic stops after a flush that did not reach the synchronization deadline,
the final published data can remain unsynchronized until another flush or
graceful shutdown. This matches disk v2's current behavior; the 500 ms setting
is not a strict upper bound on the crash-loss window. Shutdown unconditionally
publishes and synchronizes the remaining data. Durability
acknowledgement is not a separate user-facing mode in the initial release:
ingress finalizers are resolved only after the durable sequence above.

## Reader design

`DiskReader` is a stateful stream with exclusive ownership of its readable file
handle. It tracks:

- Current segment and byte offset.
- Next expected record ID.
- Latest observed committed position.
- Decoder state for the current frame.
- Ordered consumer acknowledgements.

The reader never reads beyond the committed position. When its local position
equals the committed position, it waits for the writer's retained state to
change. On wake-up it reloads the latest state and continues only if that
position has advanced. A terminal writer failure is returned immediately; a
graceful close ends the stream only after the reader reaches the final committed
position.

Because the committed position ends on a complete frame boundary, the runtime
reader does not need to distinguish a temporary EOF from an unpublished partial
record. EOF before the committed boundary is an I/O or invariant violation, not
a normal synchronization condition.

## Consumer acknowledgements

Although there is one consumer stream, downstream sink requests may complete
out of order. Each emitted frame has an opaque `AcknowledgementToken`, and
Vector's existing `OrderedFinalizer<AcknowledgementToken>` emits completed
tokens in original read order. `AcknowledgementTracker` also verifies the
token's sequence before advancing the largest contiguous observed
acknowledgement.

Here, acknowledged means that downstream finalization has completed, not only
that the final status was `Delivered`. A permanently rejected or otherwise
terminally finalized record must not occupy the disk buffer forever. The buffer
reclaims it while recording its final status in the appropriate delivery and
drop/error telemetry. Sink retry policy remains responsible for deciding when
an error is terminal.

The buffer owner batches observed acknowledgements into checkpoint updates. It
synchronizes log data before persisting a reader checkpoint. Only after the
checkpoint mmap is synchronously flushed does it advance the reclaimable
acknowledgement, release logical capacity, and mark segments for deletion. A
crash before that point may replay already finalized records, but cannot make
the buffer delete or overwrite data whose acknowledgement was not durable.

## On-disk format

Disk v3 does not archive a Rust structure directly. It defines an explicit,
versioned binary format.

### Segment file name

A segment is named `<base_offset>.log`, where `base_offset` is the record ID of
the first frame in the segment, for example `0.log` or `1048576.log`. The name
is parsed numerically rather than sorted
lexicographically. Only the canonical decimal spelling is accepted: no sign,
leading zeroes except for `0.log`, alternate extension, or value outside
`u64`.

Segment files contain frames starting at byte zero and have no segment header.
The filename provides the segment's first record ID, each frame carries its own
format version and record ID, and the checkpoint carries the durable reader
boundary. New segments are opened with `create_new` so an existing
record range is never overwritten.

An empty trailing segment can exist if Vector crashes immediately after file
creation. It advances no logical position and recovery may remove it. An empty
file followed by a non-empty later segment is an invariant violation.

### Frame

Version 1 uses a 34-byte fixed header followed immediately by the payload. All
integer fields use big-endian byte order, offsets are measured from the start
of the frame, and no padding or trailing length is present:

| Offset | Width | Field | Version 1 value or meaning |
| ------ | ----: | ----- | -------------------------- |
| 0 | 4 | Magic | ASCII `VDB3` |
| 4 | 1 | Frame version | `1` |
| 5 | 1 | Flags | `0`; nonzero values are unsupported |
| 6 | 8 | Record ID | ID of the frame's first event |
| 14 | 4 | Event count | Nonzero number of events in the payload |
| 18 | 2 | Payload codec | `1` for Vector `Encodable` |
| 20 | 2 | Reserved | Must be zero |
| 22 | 4 | Codec metadata | `Encodable::Metadata` represented as `u32` |
| 26 | 4 | Checksum | CRC-32 of the complete frame with this field zeroed |
| 30 | 4 | Payload length | Number of bytes following the header |
| 34 | variable | Payload | Bytes produced by `Encodable::encode` |

The framing format and event payload codec are versioned separately. This
allows the event representation to evolve without redefining segment recovery
or lifecycle rules.

The payload length makes concatenated frames self-delimiting; the total frame
length is the fixed 34-byte header plus that value. The checksum and payload
length allow recovery to distinguish a complete corrupt frame from an
incomplete or torn tail. Version 1 does not support reverse tail inspection;
recovery scans forward from a trusted segment boundary.

CRC-32 uses the ISO-HDLC polynomial implemented by `crc32fast`. A decoder first
requires the complete fixed header, then validates magic, version, flags,
payload length, configured maximum frame length, nonzero event count, payload
codec, and reserved fields. It does not allocate or trust a payload offset
before those bounds checks. Once the complete declared frame is available, it
verifies the checksum before exposing the payload. Empty payloads are permitted
because validity is determined by the payload codec.

### Initial payload codec

The initial payload codec reuses Vector's existing `Encodable` representation.
For `EventArray`, this is the existing Protocol Buffers payload plus its `u32`
encoding metadata. The frame stores a stable codec identifier and the metadata
value; framing version and payload metadata remain independent. Ingress and
egress finalizers are runtime state and are never serialized into the payload.

## Checkpoint format

The reader checkpoint is one fixed-size memory-mapped `CheckpointRecord`. Its
`repr(C)` layout contains magic bytes, a format version, the three `Position`
fields, and a checksum. The file length is derived from the record type rather
than maintained as a separate wire-layout table. Integer fields use little
endian representation.

The checkpoint has one slot. The buffer owner replaces the record and
synchronously flushes the mmap. Startup validates its magic, version, and
checksum. If the update was torn, recovery ignores the snapshot and resumes at
the beginning of the earliest retained segment. This may replay acknowledged
records, which is acceptable under the buffer's at-least-once contract.

Decoding reports invalid magic, checksum mismatch, and unsupported version as
typed errors. `DiskBuffer` applies the recovery policy: invalid magic and
checksum errors produce a warning and fall back to the earliest retained
segment, while an unsupported version fails startup.

The backing file is sized and synchronized when first created, after which its
directory entry is synchronized. Writer state, acknowledgement sequence
numbers, logical size, and pending byte counts are not stored. Writer state is
recovered from segment contents; the remaining values are process-local or
derived during startup.

## Capacity management

Capacity is released only through the reclaimable acknowledgement, not through
an acknowledgement that exists only in memory. Because segment filenames carry
record IDs rather than byte offsets, byte occupancy cannot be calculated by
subtracting filenames or record positions. It is derived from the segment
catalog and the byte offsets in the two boundary positions:

```text
on-disk live bytes =
    sum of segment file lengths
    - bytes before the reclaimable acknowledged boundary
    - unpublished bytes after the committed boundary in the active segment

memory in-flight bytes =
    queued accepted bytes + aggregation batch bytes

logical occupancy = on-disk live bytes + memory in-flight bytes
```

Bytes before the acknowledged boundary include every complete earlier segment
plus `segment_byte_offset` bytes in the acknowledged segment. Bytes after the
committed boundary normally do not exist, but subtracting a partial or otherwise
unpublished active tail makes recovery accounting explicit. This calculation
is needed only during startup. Normal operation maintains occupancy through
reservation and reclaimable byte deltas.

### Logical-capacity accounting

The first implementation uses a shared `LogicalCapacity` backed by one
`AtomicU64` and a Tokio `Notify`. `DiskBuffer` retains the handle so durable
checkpoints can release bytes, while producer-facing senders receive clones
that can reserve bytes. All clones refer to the same occupancy counter; the
writer neither owns nor updates capacity accounting.

On startup, `DiskBuffer` recovers the writer and opens the reader at the durable
reclaimable position. One calculation totals bytes through the committed writer
position, and a second totals bytes through the actual reader position. Checked
subtraction produces the initial logical size. Bytes before the reader and a
physical tail after the writer are therefore excluded. `DiskBuffer` initializes
capacity occupancy with this value; no segment collection is retained. There
are no in-flight reservations to recover after process restart.

Normal operation uses exact byte deltas rather than positions:

- Acquiring a reservation adds the frame's exact encoded length.
- Dropping an unaccepted reservation subtracts that length automatically.
- Accepting or publishing the frame does not change occupancy; it only changes
  the frame's lifecycle state.
- After acknowledgement progress is durable, the checkpoint owner subtracts
  the encoded bytes that became reclaimable and wakes blocked producers.

Rotation, segment sealing, and segment deletion do not change the logical byte
count. Fully acknowledged segments already stopped contributing when their
bytes became reclaimable, regardless of when their files are deleted.

The producer-facing adapter reserves the exact framed length before reporting
acceptance. Capacity occupancy remains unchanged when a batch commits: the
bytes merely move from an in-flight lifecycle state to a committed on-disk
lifecycle state. Reservations prevent concurrent producers, flushes, and
acknowledgements from admitting more logical data than configured. A future
committed-byte metric, if needed, is observability state rather than a second
admission counter.

Using observed but non-durable acknowledgement for admission is unsafe. A
writer could reuse capacity released by an in-memory acknowledgement, crash
before checkpointing it, and recover both the old and replacement data above
the configured logical limit. Advancing the reclaimable watermark only after
checkpoint synchronization prevents that rollback.

Physical usage is tracked separately:

```text
physical usage =
    segment file lengths
    + checkpoint file length
```

There is no universal ordering between physical and logical usage. Buffered and
queued records can make logical occupancy larger, while acknowledged segments
pending deletion can make physical usage larger. Metrics and assertions must
not assume one is always an upper bound for the other.

For disk v3, `max_size` is the logical admission limit. It covers committed
non-reclaimable frame bytes plus exact reservations for queued and batched
frames. It does not reserve space for segment slack, rotation, the checkpoint
file, or acknowledged files pending deletion. A single frame larger than the
limit is rejected immediately; otherwise a full buffer blocks or follows the
configured overflow policy until a durable acknowledgement checkpoint releases
enough logical bytes.

Physical usage and pending-delete bytes remain separately observable. Segment
reclamation is still required to prevent stale files from accumulating, and an
actual filesystem exhaustion error remains fatal to the running buffer, but
physical file sizes do not alter logical admission.

## Segment lifecycle

Segments are append-only while active and immutable after rotation.

The writer rotates when the next complete frame would exceed the configured
segment size. A frame never spans segments. Creating an empty next segment does
not advance a logical watermark. The actor creates the canonical
`<next_record_id>.log` file with `create_new` and immediately synchronizes the
parent directory before activating the segment. A crash after this point may
leave an empty segment, which recovery can safely remove or reuse according to
the recovery policy. The directory synchronization cost is deliberately paid
on rotation; with the intended approximately 2 GB maximum segment size, it is
amortized over a large amount of data and keeps later data synchronization
simple.

Runtime committed publication requires completed writes followed by a file
flush that makes those bytes readable. Periodic group synchronization only
synchronizes the active segment when it is dirty; its directory entry is already
durable.

Rotation follows this sequence:

1. Publish the active segment's final aggregation batch.
2. Synchronize the active segment file.
3. Create the next segment file.
4. Synchronize the parent directory.
5. Replace the active handle and return durable `SealedSegment` metadata for the
   previous file.

The deletion sequence is:

1. Receive an advanced contiguous observed acknowledgement.
2. Persist and synchronize the checkpoint containing that acknowledgement.
3. Advance the reclaimable acknowledgement and release logical capacity.
4. Mark all fully reclaimable immutable segments for deletion.
5. Delete reclaimable segments asynchronously.
6. Update physical-usage metrics after successful deletion.
7. Retry failed deletions with bounded backoff without changing logical
   acknowledgement.

The writer actor owns deletion intent and physical accounting. A helper task may
perform the blocking filesystem operation, but it returns the exact result to
the actor and cannot independently mutate watermarks.

## Exclusive directory ownership

Before reading checkpoints or opening segments, disk v3 acquires an exclusive
lock for the buffer directory. This enforces the single-writer/single-reader
assumption across Vector processes as well as within one process. Startup fails
with an actionable error if another process owns the directory. The directory
path and checkpoint identify the buffer; files with non-canonical names are not
treated as segments.

## Recovery

Startup recovery performs these steps:

1. Acquire the exclusive directory lock.
2. Load and validate the reader checkpoint; if it is torn, use the earliest
   retained segment.
3. Enumerate canonical segment filenames and validate numeric ordering and
   gaps.
4. Reconcile checkpoint references with present, deleted, and orphan segments.
5. Start scanning at the earliest segment required by the durable checkpoint.
6. Validate bounded frame lengths, versions, record IDs, event counts, and
   checksums before allocating or decoding payloads.
7. Preserve every complete valid frame.
8. Stop at the first incomplete or torn frame, discard any logically later
   orphan segments, and truncate the invalid tail.
9. Fail closed on a complete corrupt frame.
10. Reconstruct committed, durable, observed-acknowledged,
    reclaimable-acknowledged, and physical positions.
11. Restore the next record ID from the recovered committed prefix, clear
    queued and batched byte accounting, open the active segment for append, and
    publish the recovered state.

Complete valid frames found beyond the last durable checkpoint may be retained.
Because their upstream durability acknowledgement might not have been observed,
this can produce duplicates after a retry. That behavior is preferable to
silently deleting valid data and is consistent with at-least-once delivery.

The initial corruption policy for a complete corrupt frame is to stop recovery
and fail the buffer with the segment, offset, and validation error. Automatic
skipping risks silent data loss and cannot safely infer record IDs or event
counts from an untrustworthy frame. A later administrative repair tool may copy
the valid prefix and quarantine the corrupt tail, but repair is never automatic
on normal startup.

## Error handling

Permanent writer errors transition the writer into `Failed`. The failure state
contains the operation, segment, offset, and underlying error. It is visible to
all producer clones, the reader, the topology supervisor, and internal
telemetry. The buffer adapter must propagate reader failure into topology error
handling rather than converting it to EOF or polling forever.

The error policy is operation-specific:

- An append, rotation, data synchronization, or checkpoint error that may have
  partially changed persistent state is terminal for the running actor. It is
  not retried in place; restart recovery determines the last valid prefix.
- Failure to encode an individual record is handled as an individual rejected
  or dropped record according to existing buffer semantics and does not corrupt
  the actor state.
- Failure to delete a reclaimable segment is retryable because the durable
  checkpoint already makes the operation idempotent. A deletion backlog is
  reported independently and does not change logical admission.
- Interrupted operations may be retried only when the platform API guarantees
  that doing so cannot duplicate an ambiguous partial effect.

When the actor fails, it stops accepting records, preserves the first causal
error, completes every queued response with that error, and resolves ingress
finalizers that did not reach the durable watermark as errored. Finalizers
through the durable watermark remain successfully resolved. Dropping a response
receiver does not stop the actor from completing the command.

The reader must not poll indefinitely when committed bytes cannot be read. EOF,
decode failure, or checksum failure before the committed position is reported
as a concrete invariant or corruption error.

Retryable operations retain their target state and use bounded backoff.
Retrying an operation must not republish a frame or advance a watermark twice.

## Observability

Disk v3 should expose at least:

- Accepted and batched byte counts plus committed, data-synchronized, durable,
  observed-acknowledged, and reclaimable-acknowledged record positions.
- Logical occupancy and physical disk usage.
- Reserved, queued, in-flight, and pending-delete bytes.
- Append queue depth, admission latency, and writer command age.
- In-flight batch age, bytes, events, and accepted offset.
- Current reader and writer segment base record offsets and file byte offsets.
- Time since last reader, writer, and synchronization progress.
- Write, synchronization, checkpoint, decode, corruption, and deletion errors.
- Segment creation, rotation, recovery, and deletion counts.
- Group synchronization batch size and latency.
- Out-of-order acknowledgement depth.

Metrics representing logical capacity and physical disk usage must use distinct
names and descriptions.

## Testing strategy

### Deterministic I/O tests

Use a controllable filesystem implementation that can pause, partially complete,
or fail each operation. Required cases include:

- Cancellation before a producer command is accepted.
- Cancellation after command acceptance but before record-ID assignment.
- Forced actor termination after a partial batch write, followed by recovery.
- Forced actor termination while waiting for the final Tokio write to complete.
- Cancellation of an admission or flush response while the actor continues.
- Dropping one producer clone while other producers and the actor continue.
- Verification that producer cancellation never drops the actor-owned I/O
  future.
- A write accepted by Tokio that later returns an I/O error.
- `ENOSPC` during a batch and during checkpoint synchronization.
- Synchronization success and failure.
- Segment creation and rotation failures.
- Complete corruption and torn-tail recovery.
- Checkpoint corruption and fallback to the earliest retained segment.
- Asynchronous deletion failure and retry.
- A growing deletion backlog without any change to logical admission.
- Large frames spanning multiple underlying write-buffer operations.
- A full append queue while acknowledgement progress still reaches the actor.

Every cancellation test must verify that record IDs remain strictly increasing
and that each accepted frame is written at most once.

MPSC tests must run several producers concurrently and verify that actor receive
order defines one gap-free record-ID sequence, each producer's awaited send
order is preserved, and cancellation cannot remove another producer's record.
They must also verify `block`, `drop_newest`, and `overflow` behavior without
losing or duplicating finalizers.

### Model testing

A reference model should track:

- Accepted commands and reservations.
- Pending records.
- Committed records.
- Durable records.
- Emitted, finalized, observed-acknowledged, and reclaimable records.
- Logical and physical positions.
- Existing and pending-delete segments.

Random operation sequences should include concurrent producer submissions,
producer cancellation, writes, manual flushes, reads, out-of-order
acknowledgements, rotations, I/O failures, deletion failures, shutdown, and
restarts. The implementation state is compared with the model after every
operation.

### Real crash tests

The deterministic filesystem validates state-machine behavior but cannot prove
real kernel and filesystem synchronization ordering. Subprocess tests should
kill a writer at each data-file, checkpoint-record, segment-creation, and
directory-synchronization boundary, then restart against the real filesystem.
Run the checkpoint and recovery suite on Linux, macOS, and Windows and keep
golden binary fixtures for every supported frame and checkpoint version.

### Antithesis

The Antithesis scenario should assert safety and liveness rather than durability
that disk buffers do not promise. It should cover:

- No duplicate record IDs within one recovered log history.
- No record-ID regression.
- No frame crossing a segment boundary.
- Logical occupancy never exceeds admitted capacity.
- Physical usage is reported independently from logical occupancy.
- A retryable deletion failure does not prevent acknowledgement progress and
  resumes deletion when the fault clears.
- A permanent failure is surfaced rather than represented as a stuck buffer.
- Restart recovery never panics on a valid prefix followed by a torn tail.

## Migration

Disk v3 uses a new directory and format identifier. It does not reinterpret or
modify disk-v2 files.

The initial release is selected explicitly with a new configuration value such
as `type: disk_v3`. The existing `type: disk` continues to mean disk v2. Users
must drain disk-v2 buffers, stop Vector, verify that the old buffer is empty, and
then switch the component to disk v3. If both version directories contain data,
startup fails with migration guidance rather than choosing one implicitly.
Automatic import can be considered later, but it is not required for the first
implementation.

Once disk v3 has sufficient production and fault-testing coverage, it can
become the default only through an explicit compatibility and deprecation plan.
Existing `type: disk` configurations and disk-v2 directories must not silently
change format. A future major-version alias may choose v3 for new directories,
but must refuse ambiguous or non-empty disk-v2 state unless the user explicitly
migrates it.

## Alternatives

### Continue patching disk v2

Incremental reader retries and publication gates improve particular races but
retain the underlying coupling among asynchronous file state, ledger state, and
notifications. This remains appropriate for targeted production fixes while
disk v3 is developed, but it does not provide the proposed simplified model.

### Producer-owned writer behind a mutex

Disk v2 places a writer behind shared mutable ownership. Retaining that shape
would fit the cloneable topology sender but would leave the file I/O future
owned by whichever producer currently holds the mutex. Producer cancellation,
hot-reload detachment, and per-record flushing would continue to interact with
partially completed writes. Disk v3 rejects this alternative in favor of the
dedicated actor.

### Refactor the topology to a single producer

The disk log could expose an exclusive writer directly if Vector first
serialized every upstream connection outside the buffer. That is a much larger
topology change and still creates an owning task or queue somewhere else. Disk
v3 keeps the existing MPSC topology contract and makes the serialization point
an explicit part of the buffer.

### Vectored writes

Vectored writes could replace the contiguous batch with a queue of independently
owned record allocations. Current Tokio file I/O still coalesces vectored slices
into its internal buffer, so this adds ownership and partial-write complexity
without clearly removing a copy. It should only be considered after profiling.

### Recalculate runtime occupancy from segment positions

Frame-boundary positions and the segment catalog remain the recovery authority,
but rescanning files for every admission decision would make a hot producer path
perform directory and metadata I/O. Disk v3 instead derives the initial value at
startup, then maintains checked atomic counters with exact frame-byte deltas.
RAII reservations cover cancellation before acceptance, and restart scanning
repairs any process-local accounting after a crash.

## Remaining implementation work

The segmented reader, writer, framing, logical-capacity accounting,
acknowledgements, and reader checkpoint provide the core storage path. The
remaining work, in priority order, is:

1. **Reclaim fully acknowledged segments.** After a reader checkpoint is
   durable, delete segments strictly older than its segment base offset. Keep
   the segment containing the checkpoint, synchronize the directory after
   deletion, and track physical bytes independently from logical bytes. A
   deletion failure must be retryable without rolling back the durable
   checkpoint. Until this exists, logical capacity can be released but physical
   segment files accumulate forever.
2. **Drive acknowledgements and checkpoints automatically.** The finalizer
   consumer must pass completed tokens to `acknowledge`, then batch calls to
   `checkpoint` according to manual flushes, an acknowledged-byte threshold,
   and graceful shutdown. Without this owner, acknowledged progress can remain
   in memory indefinitely and be replayed after restart.
3. **Harden crash recovery.** Recovery must preserve every complete valid
   frame, truncate an incomplete frame in the newest segment, remove logically
   later orphan segments, and reopen the last valid boundary for append.
   Complete frame corruption continues to fail closed. Startup must also obtain
   an exclusive directory lock and validate that a loaded reader checkpoint is
   within the recovered log.
4. **Complete capacity integration in the MPSC adapter.** `DiskBuffer` now
   initializes logical admission from recovered committed non-reclaimable bytes,
   requires an exact frame reservation on append, returns abandoned
   reservations through RAII, and releases accepted capacity only after a
   durable acknowledgement checkpoint. The topology adapter must acquire and
   transfer those reservations with queued frames and map full admission to its
   block, drop, and overflow policies.
5. **Add the Vector-facing adapter.** The adapter needs cloneable MPSC senders,
   one task owning `DiskBuffer`, the existing `BufferSender` and
   `BufferReceiver` interfaces, ordered-finalizer completion handling, flush
   and shutdown wiring, and the real `EventArray` codec. Disk v3 also needs an
   opt-in configuration path before it can replace disk v2.
6. **Complete production validation.** Add metrics and failure diagnostics,
   deterministic failure injection, subprocess crash tests, golden frame and
   checkpoint fixtures, an Antithesis scenario, migration guidance, and
   benchmarks against disk v2.

Segment reclamation should be implemented next, followed by incomplete-tail
recovery. Together they complete the physical storage lifecycle before topology
integration is added.

## Outstanding questions

- What synchronization byte threshold should complement the initial 500 ms
  interval?
- What exact topology API owns the writer actor's shutdown handle and waits for
  its drain result during reload and graceful shutdown?
- How should a full or overflow result recover an encoded record without adding
  an avoidable clone on the common path?
- How should users drain or inspect a failed disk-v3 buffer?
- Is an automatic disk-v2 importer worth supporting after the initial release?

## Decisions made in this draft

- The topology-facing buffer is MPSC.
- A dedicated actor is the sole writer, checkpoint owner, and capacity owner.
- The log itself has one writer and one reader.
- The existing `BufferSender::flush` call manually drives publication through
  the MPSC command queue, matching disk v2's topology integration.
- Manual flushes opportunistically synchronize dirty data when 500 ms has
  elapsed; this is not a strict background deadline.
- Shutdown forces final data and checkpoint synchronization.
- The initial payload codec is Vector's existing `Encodable` representation;
  `EventArray` uses its current Protocol Buffers encoding and metadata.
- Complete corrupt frames fail recovery closed in the initial release.
- Checkpoints use one fixed-size mmap record; a torn record falls back to the
  earliest retained segment.
- Observed acknowledgement does not release capacity until it is checkpointed
  and becomes reclaimable.
- `max_size` is a logical byte limit over committed non-reclaimable frames and
  accepted in-flight reservations; physical disk usage is separate.
- Ingress durability finalizers are resolved only after data synchronization;
  there is no separate durability mode initially.
- Disk v3 is opt-in through a versioned configuration and directory.

## Plan of attack

- [ ] Turn this draft into a numbered RFC and resolve the remaining API and
  sizing questions.
- [ ] Prototype the MPSC adapter, actor lifecycle, reload, shutdown, and fanout
  flush integration.
- [ ] Specify the segment, frame, payload-codec, and reader checkpoint binary
  formats with golden fixtures.
- [ ] Build a deterministic filesystem and failure-injection test framework.
- [ ] Implement the position types and a model-only MPSC admission and
  single-writer log state machine.
- [ ] Implement the writer actor and its actor-owned asynchronous I/O sequence.
- [ ] Implement committed-position publication and the bounded reader stream.
- [ ] Add group synchronization and durable acknowledgement tracking.
- [x] Implement ordered observed acknowledgements, reclaimable checkpoints, and
  logical capacity admission.
- [ ] Implement segment rotation, durable checkpoints, and recovery scanning.
- [ ] Implement asynchronous deletion and physical-usage accounting.
- [ ] Add internal metrics and actionable failure diagnostics.
- [ ] Run model, MPSC ordering, cancellation, real-crash, and large-record tests.
- [ ] Add an Antithesis scenario covering the safety and liveness invariants.
- [ ] Add an opt-in disk-v3 configuration and drain-based migration guidance.
- [ ] Benchmark throughput, latency, memory usage, and synchronization policies
  against disk v2.
- [ ] Roll out experimentally before considering disk v3 as the default.

## Future improvements

- Automatic import of drained or offline disk-v2 buffers.
- Vectored or platform-specific I/O after profiling.
- Optional direct I/O if alignment and cache behavior justify the complexity.
- Administrative inspection and repair tooling.
- Alternative MPSC admission implementations if profiling identifies the
  command queue or record recovery path as a bottleneck.
