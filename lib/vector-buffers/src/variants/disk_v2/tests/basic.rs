use std::{io::Cursor, sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use tokio::{select, time::sleep};
use tokio_test::{assert_pending, task::spawn};
use tracing::Instrument;
use vector_common::finalization::{AddBatchNotifier, BatchNotifier, BatchStatus, Finalizable};

use super::{
    create_default_buffer_v2, create_default_buffer_v2_with_usage, read_next, read_next_some,
};
use crate::{
    EventCount, assert_buffer_is_empty, assert_buffer_records,
    buffer_usage_data::BufferUsageHandle,
    test::{MultiEventRecord, SizedRecord, acknowledge, install_tracing_helpers, with_temp_dir},
    variants::disk_v2::{
        Buffer, BufferWriter, DiskBufferConfigBuilder, Ledger, writer::RecordWriter,
    },
};

#[tokio::test]
async fn basic_read_write_loop() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, ledger) = create_default_buffer_v2(data_dir).await;
            assert_buffer_is_empty!(ledger);

            let expected_items = (512..768)
                .cycle()
                .take(10)
                .map(SizedRecord::new)
                .collect::<Vec<_>>();
            let input_items = expected_items.clone();

            // Now create a reader and writer task that will take a set of input messages, buffer
            // them, read them out, and then make sure nothing was missed.
            let write_task = tokio::spawn(async move {
                for item in input_items {
                    writer
                        .write_record(item)
                        .await
                        .expect("write should not fail");
                }
                writer.flush().await.expect("writer flush should not fail");
                writer.close();
            });

            let read_task = tokio::spawn(async move {
                let mut items = Vec::new();
                while let Some(mut record) = read_next(&mut reader).await {
                    acknowledge(record.take_finalizers()).await;
                    items.push(record);
                }
                items
            });

            // Wait for both tasks to complete.
            write_task.await.expect("write task should not panic");
            let actual_items = read_task.await.expect("read task should not panic");

            // All records should be consumed at this point.
            assert_buffer_is_empty!(ledger);

            // Make sure we got the right items.
            assert_eq!(actual_items, expected_items);
        }
    })
    .await;
}

#[ignore = "flaky. See https://github.com/vectordotdev/vector/issues/23456"]
#[tokio::test]
async fn reader_exits_cleanly_when_writer_done_and_in_flight_acks() {
    let assertion_registry = install_tracing_helpers();

    let fut = with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, ledger) = create_default_buffer_v2(data_dir).await;
            assert_buffer_is_empty!(ledger);

            // Now write a single value and close the writer.
            writer
                .write_record(SizedRecord::new(32))
                .await
                .expect("write should not fail");
            writer.flush().await.expect("writer flush should not fail");
            writer.close();
            assert_buffer_records!(ledger, 1);

            // And read that single value.
            let first_read = read_next_some(&mut reader).await;
            assert_eq!(first_read, SizedRecord::new(32));
            assert_buffer_records!(ledger, 1);

            // Now, we haven't acknowledged that read yet, so our next read should see the writer as
            // done but the total buffer size as >= 0, which means it has to wait for something,
            // which in this case is going to be the wakeup after we acknowledge the read.
            //
            // We have to poll until we hit the `wait_for_writer` call because there might be
            // wake-ups in between due to the actual file I/O calls we do in the `next` call waking
            // us up.
            //
            // Why do we need to make sure we enter `wait_for_writer` twice?  When the writer
            // closes, it always sends a wakeup in case there's a reader that's been (correctly)
            // waiting for new data.  When we do our first read here, there's data to be read, so
            // the reader never has to actually wait for the writer to make progress because it
            // doesn't yet think it's out of data.
            //
            // When we do the first poll of the second read, we will call `wait_for_writer` which
            // has a stored wakeup, which will let it proceed with another loop iteration, landing
            // it at the second call which is the one that causes it to actually block.
            let waiting_for_writer = assertion_registry
                .build()
                .with_name("wait_for_writer")
                .with_parent_name("reader_exits_cleanly_when_writer_done_and_in_flight_acks")
                .was_entered_at_least(2)
                .finalize();
            let mut blocked_read = spawn(reader.next());
            while !waiting_for_writer.try_assert() {
                assert_pending!(blocked_read.poll());
            }

            // Now acknowledge the first read, which should wake up our blocked read.
            acknowledge(first_read).await;

            // Our blocked read should be woken up, and when we poll it, it should be also be ready,
            // albeit with a return value of `None`... because the writer is closed, and we read all
            // the records, so nothing is left. :)
            assert!(blocked_read.is_woken());

            let second_read = select! {
                // if the reader task finishes in time, extract its output
                res = blocked_read => res,
                // otherwise panics after 1s
                () = sleep(Duration::from_secs(1)) => {
                    panic!("Reader not ready after 1s");
                }
            };
            assert_eq!(second_read.expect("read should not fail"), None);

            // All records should be consumed at this point.
            assert_buffer_is_empty!(ledger);
        }
    });

    let parent = trace_span!("reader_exits_cleanly_when_writer_done_and_in_flight_acks");
    fut.instrument(parent.or_current()).await;
}

#[tokio::test]
async fn initial_size_correct_with_multievents() {
    let _a = install_tracing_helpers();
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Build a write-only buffer without a finalizer task. Using
            // `from_config_inner` would spawn a background finalizer that holds
            // an Arc<Ledger> (and the lock file), causing a racy
            // LedgerLockAlreadyHeld when we reopen the buffer below.
            let config = DiskBufferConfigBuilder::from_path(&data_dir)
                .build()
                .expect("creating buffer config should not fail");
            let usage_handle = BufferUsageHandle::noop();
            let ledger = Ledger::load_or_create(config, usage_handle)
                .await
                .expect("ledger should not fail to load/create");
            let ledger = Arc::new(ledger);

            let mut writer = BufferWriter::new(Arc::clone(&ledger));
            writer
                .validate_last_write()
                .await
                .expect("validate_last_write should not fail");

            ledger.synchronize_buffer_usage();

            let input_items = (512..768)
                .cycle()
                .take(2000)
                .map(MultiEventRecord::new)
                .collect::<Vec<_>>();
            let expected_records = input_items.len();
            let expected_events = input_items
                .iter()
                .map(EventCount::event_count)
                .sum::<usize>();

            // We also directly create a record writer so we can simulate actually
            // encoding/archiving the record to get the true on-disk size, as that's what we report
            // to the buffer usage handle but not anything we have access to from the outside.
            //
            // Technically, we aggregate the bytes written value from each write, but we also want
            // to verify that is accurate, so we record each record by hand to make sure our totals
            // are identical:
            let expected_bytes = stream::iter(input_items.iter().cloned())
                .filter_map(|record| async move {
                    let mut record_writer =
                        RecordWriter::new(Cursor::new(Vec::new()), 0, 16_384, u64::MAX, usize::MAX);
                    let (bytes_written, flush_result) = record_writer
                        .write_record(0, record)
                        .await
                        .expect("record writing should not fail");
                    record_writer.flush().await.expect("flush should not fail");
                    let inner_buf_len = record_writer.get_ref().get_ref().len();

                    // The bytes that it reports writing should be identical to what the underlying
                    // write buffer has, since this is a fresh record writer.
                    assert_eq!(bytes_written, inner_buf_len);
                    assert_eq!(flush_result, None);

                    Some(inner_buf_len)
                })
                .fold(0, |acc, n| async move { acc + n })
                .await;

            // Write a bunch of records so the buffer has events when we reload it.
            let mut total_bytes_written = 0;
            for item in input_items {
                let bytes_written = writer
                    .write_record(item)
                    .await
                    .expect("write should not fail");
                total_bytes_written += bytes_written;
            }
            writer.flush().await.expect("writer flush should not fail");
            writer.close();

            // Drop the first buffer. No background finalizer task exists, so the
            // ledger lock is released immediately when the last Arc is dropped.
            drop(writer);
            drop(ledger);

            // Reopen the buffer with a full reader to verify the persisted data.
            let (writer, mut reader, ledger, usage) =
                create_default_buffer_v2_with_usage::<_, MultiEventRecord>(&data_dir).await;
            drop(writer);

            // Make sure our usage data agrees with our expected event count and byte size:
            let snapshot = usage.snapshot();
            assert_eq!(expected_events as u64, snapshot.received_event_count);
            assert_eq!(expected_bytes as u64, snapshot.received_byte_size);
            assert_eq!(expected_events as u64, ledger.get_total_records());
            assert_eq!(expected_bytes, total_bytes_written);

            // Make sure we can read all of the records we wrote, and recalculate some of these
            // values from the source:
            let mut total_records_read = 0;
            let mut total_record_events = 0;
            while let Some(record) = read_next(&mut reader).await {
                total_records_read += 1;
                let event_count = record.event_count();
                acknowledge(record).await;
                total_record_events += event_count;
            }

            assert_eq!(expected_events, total_record_events);
            assert_eq!(expected_records, total_records_read);
        }
    })
    .await;
}

/// Regression test for the e2e ack durability fix.
///
/// Before the fix, encoding a record consumed it (along with any upstream-attached
/// `EventFinalizers`), causing those finalizers to fire `Delivered` the moment the encoded bytes
/// reached the OS page cache — well before the periodic `sync_all()` (fsync, default 500 ms
/// interval) made the data durable. With e2e acks enabled, an upstream source (e.g. Filebeat)
/// would receive an acknowledgement, drop the records from its retry queue, and Vector could
/// then crash before fsync — losing the data permanently with no chance of retransmission.
///
/// After the fix, `try_write_record_inner` detaches the finalizers from the record before
/// encoding and parks them in `pending_finalizers`. They fire `Delivered` only after the next
/// successful `sync_all()` in `flush_inner`. On `close()` (or `Drop` without a final fsync) any
/// still-pending finalizers fire `Errored`, so the source retransmits on reconnect rather than
/// silently losing data.
#[tokio::test]
async fn no_premature_e2e_ack_before_fsync() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();
        async move {
            // Use a very long flush interval so the page-cache flush in `flush_inner` never
            // promotes to an fsync during the first phase of the test. We trigger the
            // post-fsync phase later by rebuilding the buffer with a 0-duration interval.
            let config = DiskBufferConfigBuilder::from_path(&data_dir)
                .flush_interval(Duration::from_mins(1))
                .build()
                .expect("creating buffer config should not fail");
            let usage_handle = BufferUsageHandle::noop();
            let (mut writer, reader, ledger) =
                Buffer::<SizedRecord>::from_config_inner(config, usage_handle)
                    .await
                    .expect("buffer should build");

            // Attach a BatchNotifier to the record — this is what an upstream source uses to
            // know when its data has been durably accepted by the buffer.
            let mut record = SizedRecord::new(64);
            let (batch, mut receiver) = BatchNotifier::new_with_receiver();
            record.add_batch_notifier(batch);

            // Write + flush. flush_inner drains the BufWriter to the OS page cache, but the
            // long flush_interval keeps should_flush() returning false, so sync_all() is
            // skipped and the data is not yet durable.
            writer
                .write_record(record)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");

            // The upstream source MUST NOT see an ack yet. Before the fix, the finalizer fired
            // as soon as the encoded record dropped — try_recv() here would return the
            // Delivered status immediately, which is the regression we're guarding against.
            assert_eq!(
                receiver.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty),
                "BatchNotifier fired before fsync — premature e2e ack regression",
            );

            // Drop the writer/reader/ledger from this phase before reopening the buffer below.
            // The reader is still parked on an empty file, so dropping it lets the background
            // finalizer task exit and release the advisory ledger lock.
            drop(writer);
            drop(reader);
            while Arc::strong_count(&ledger) > 1 {
                tokio::task::yield_now().await;
            }
            drop(ledger);

            // The dropped writer fired our pending finalizer as `Errored` (data was never
            // fsynced).  Drain that signal so the next phase can verify the Delivered path
            // cleanly.
            assert_eq!(
                receiver.await,
                BatchStatus::Errored,
                "closing the writer without fsync should fire Errored, not Delivered",
            );

            // Phase 2: write a fresh record with a 0-duration flush_interval so the next
            // flush() always triggers sync_all(), and verify the finalizer fires `Delivered`.
            let config = DiskBufferConfigBuilder::from_path(&data_dir)
                .flush_interval(Duration::ZERO)
                .build()
                .expect("creating buffer config should not fail");
            let usage_handle = BufferUsageHandle::noop();
            let (mut writer, reader, ledger) =
                Buffer::<SizedRecord>::from_config_inner(config, usage_handle)
                    .await
                    .expect("buffer should rebuild");

            let mut record = SizedRecord::new(64);
            let (batch, mut receiver) = BatchNotifier::new_with_receiver();
            record.add_batch_notifier(batch);

            writer
                .write_record(record)
                .await
                .expect("write should not fail");

            // The fix still applies pre-flush: finalizers were taken off the record at write
            // time and parked, so try_recv() must still be Empty until flush() actually
            // fsyncs.
            assert_eq!(
                receiver.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty),
                "finalizer fired before flush — should only fire after sync_all()",
            );

            // sleep so any non-zero elapsed comparison inside should_flush() definitely
            // returns true on the next check.
            sleep(Duration::from_millis(1)).await;
            writer.flush().await.expect("flush should not fail");

            // After fsync, the held finalizer should have been fired as `Delivered`.
            assert_eq!(
                receiver.try_recv(),
                Ok(BatchStatus::Delivered),
                "finalizer should fire Delivered after sync_all() completes",
            );

            // Clean shutdown.
            drop(writer);
            drop(reader);
            while Arc::strong_count(&ledger) > 1 {
                tokio::task::yield_now().await;
            }
            drop(ledger);
        }
    })
    .await;
}

/// Verifies the graceful-shutdown durable flush does not close the buffer.
///
/// On graceful shutdown, `flush_pending_finalizers` (driven through `BufferSender::flush_durable`)
/// fsyncs and fires the pending tail as `Delivered`. Crucially, it must NOT mark the writer as
/// done: a disk buffer's writer can be shared across multiple upstream components in a fan-in
/// topology, so signalling writer-done here would terminate the reader while other upstreams are
/// still writing. This test confirms that after a durable flush the buffer is still fully usable —
/// the just-flushed record is readable and further writes succeed and are readable too.
#[tokio::test]
async fn durable_flush_does_not_close_buffer() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();
        async move {
            let (mut writer, mut reader, ledger) = create_default_buffer_v2(data_dir).await;

            let write_task = tokio::spawn(async move {
                // Write a record carrying an upstream finalizer, then perform a graceful durable
                // flush mid-stream.
                let mut first = SizedRecord::new(64);
                let (batch, receiver) = BatchNotifier::new_with_receiver();
                first.add_batch_notifier(batch);
                writer
                    .write_record(first)
                    .await
                    .expect("write should not fail");

                writer
                    .flush_pending_finalizers()
                    .await
                    .expect("durable flush should not fail");

                // The durable flush fires the parked finalizer as Delivered...
                assert_eq!(
                    receiver.await,
                    BatchStatus::Delivered,
                    "durable flush should fire Delivered",
                );

                // ...but must NOT have marked the writer done. If it had, the reader would
                // terminate after the first record and never observe this second write.
                writer
                    .write_record(SizedRecord::new(128))
                    .await
                    .expect("write after durable flush should not fail");
                writer.flush().await.expect("flush should not fail");

                // Only now do we actually close the writer.
                writer.close();
            });

            let read_task = tokio::spawn(async move {
                let mut items = Vec::new();
                while let Some(mut record) = read_next(&mut reader).await {
                    acknowledge(record.take_finalizers()).await;
                    items.push(record.0);
                }
                items
            });

            write_task.await.expect("write task should not panic");
            let items = read_task.await.expect("read task should not panic");

            // Both records survived the mid-stream durable flush, proving it left the buffer open.
            assert_eq!(items, vec![64, 128]);
            assert_buffer_is_empty!(ledger);

            while Arc::strong_count(&ledger) > 1 {
                tokio::task::yield_now().await;
            }
            drop(ledger);
        }
    })
    .await;
}

/// Regression test for the idle-tail ack stall.
///
/// The e2e ack fix defers a record's `Delivered` finalizer until the next `sync_all()` in
/// `flush_inner`, which only fsyncs when `should_flush()` is true. Since flushes are write-driven,
/// the last batch written before traffic goes idle parks its finalizers indefinitely — the data is
/// durable on disk but the upstream source never receives its ack. `flush_pending_finalizers`,
/// driven by the periodic flush task, drains that idle tail: it forces an fsync and fires the
/// parked finalizers as `Delivered`, but only when finalizers are actually awaiting durability so
/// an idle buffer performs no extra I/O.
#[tokio::test]
async fn periodic_flush_drains_idle_tail() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();
        async move {
            // A long flush interval guarantees the per-write `flush()` never promotes to an fsync,
            // so the finalizer stays parked exactly as it would when traffic goes idle.
            let config = DiskBufferConfigBuilder::from_path(&data_dir)
                .flush_interval(Duration::from_mins(1))
                .build()
                .expect("creating buffer config should not fail");
            let usage_handle = BufferUsageHandle::noop();
            let (mut writer, reader, ledger) =
                Buffer::<SizedRecord>::from_config_inner(config, usage_handle)
                    .await
                    .expect("buffer should build");

            // With nothing written yet, the periodic flush must be a no-op (no pending finalizers).
            writer
                .flush_pending_finalizers()
                .await
                .expect("flush_pending_finalizers should not fail when idle");

            let mut record = SizedRecord::new(64);
            let (batch, mut receiver) = BatchNotifier::new_with_receiver();
            record.add_batch_notifier(batch);

            writer
                .write_record(record)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");

            // The long flush interval kept `should_flush()` false, so the finalizer is parked.
            assert_eq!(
                receiver.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty),
                "finalizer fired before the periodic flush — should still be parked",
            );

            // The periodic flush task forces an fsync and drains the parked finalizer, even though
            // no further writes arrived and `should_flush()` would still return false.
            writer
                .flush_pending_finalizers()
                .await
                .expect("flush_pending_finalizers should not fail");

            assert_eq!(
                receiver.try_recv(),
                Ok(BatchStatus::Delivered),
                "periodic flush should fire Delivered for the idle tail",
            );

            drop(writer);
            drop(reader);
            while Arc::strong_count(&ledger) > 1 {
                tokio::task::yield_now().await;
            }
            drop(ledger);
        }
    })
    .await;
}
