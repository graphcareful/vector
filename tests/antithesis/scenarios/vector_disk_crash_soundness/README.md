# vector_disk_crash_soundness

Crashes a single `disk_v2`-buffered Vector node and checks that the buffer comes
back **sound**, not that it comes back **complete**.

## Why not conservation

`vector_to_vector_e2e_disk` already covers conservation: with end-to-end acks on,
an acked event must survive arbitrary faults. That property currently does not
hold for the disk buffer -- the sink's ack fires once an event is encoded into
the buffer's in-memory write buffer, not once it is fsync'd, so a crash between
those two points loses an event the client was told (200) was safe. That is a
real, tracked bug in its own right, not something this scenario should also
(re)assert.

This scenario turns acks off (see `vector.yaml`) and asks a different question:
**whatever the ack situation, does a crash ever corrupt data, invent data, wedge
the buffer, or leave its internal accounting broken?** Those properties must hold
independent of the ack bug and shouldn't wait on it to be fixed:

- **integrity** -- every record that does arrive at the oracle matches what was
  actually sent for its id. Checked online, on every delivery, by the shared
  oracle (`/ingest`); restated once more as a final quiescent measurement in
  `eventually_disk_crash_soundness`.
- **accounting soundness** -- the disk buffer's own record-count and byte-size
  bookkeeping never underflows. This is asserted in-process by the buffer code
  itself (`antithesis-disk-asserts`, e.g. `Ledger::get_total_records` and
  `decrement_total_buffer_size`), not by this scenario's test commands -- an
  underflow wraps the counter to a near-maximum value and wedges the writer, so
  catching it where it happens is more precise than inferring it from the
  outside.
- **liveness** -- once faults stop, the node recovers and a fresh write still
  makes it through end to end.
- **no software crash** -- node termination is an Antithesis-injected `SIGKILL`,
  not a Vector panic. The Antithesis instrumentation baked into the SUT binary
  flags a panic or segfault as its own fault class automatically; no test-command
  code is needed for that.

What this scenario explicitly does **not** assert: how much data survived a
crash, or whether an acked id came back. With acks disabled there is no
durability contract to check that against, so "some records are missing" is
expected and not a finding.

## How it works

One Vector node and one oracle container.

- **vector** takes an `http_server` source and delivers over `http` to the
  oracle through a `disk_v2` buffer with `when_full: block` and acks disabled.
  `VECTOR_DISK_V2_MAX_DATA_FILE_SIZE` shrinks the data file so it fills and rolls
  constantly, reaching the file-roll and crash-recovery paths a longer-running
  buffer would rarely hit.
- **oracle** injects unique event ids at the node and runs the HTTP endpoint the
  node's sink delivers back to, tracking issued/delivered ids and flagging any
  corrupted or spurious (never-issued) delivery as it happens.

The workload binaries (`oracle`, `parallel_driver_produce`,
`eventually_disk_crash_soundness`) are the shared, buffer-agnostic bins from
`tests/antithesis/harness`, pointed at this topology by the environment in
`docker-compose.yaml`.

## Run

Validate the config locally:

```bash
cd tests/antithesis
docker compose -f scenarios/vector_disk_crash_soundness/docker-compose.yaml build
snouty validate scenarios/vector_disk_crash_soundness
```

Submit a run through the shared launcher, which pins the fault profile (see
`tests/antithesis/AGENTS.md`):

```bash
cd tests/antithesis/scenarios
./launch.sh vector_disk_crash_soundness
```
