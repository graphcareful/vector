use std::{collections::BTreeSet, num::NonZeroU32, time::Duration};

use tokio::time::{sleep, timeout};
use vector_common::finalization::{
    BatchNotifier, BatchStatus, EventFinalizer, EventFinalizerGroups, EventFinalizers,
};

use crate::variants::disk_v3::{
    checkpoint::CHECKPOINT_FILE_NAME,
    disk_buffer::{
        DEFAULT_COMMAND_QUEUE_CAPACITY, DiskBuffer, DiskBufferConfig, DiskBufferError,
        DiskBufferRead, DiskBufferReceiver, DiskBufferSender, PreparedRecord, SendOutcome,
        TrySendOutcome, WriterStatus,
    },
    position::Position,
    segment_files::segment_file_name,
};

use super::harness::{DiskV3Harness, TestFrame, writer_config};

const FIRST_RECORD_ID: u64 = 100;
const MAX_FRAME_LEN: usize = 1024;
const MAX_LOGICAL_BYTES: u64 = 1024 * 1024;

fn config(
    harness: &DiskV3Harness,
    max_logical_bytes: u64,
    segment_size: u64,
    batch_size: usize,
) -> DiskBufferConfig {
    DiskBufferConfig {
        directory: harness.path().to_path_buf(),
        initial: Position::at_segment_start(FIRST_RECORD_ID),
        max_frame_len: MAX_FRAME_LEN,
        max_logical_bytes,
        command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
        writer: writer_config(segment_size, batch_size),
    }
}

fn record(frame: &TestFrame) -> PreparedRecord {
    PreparedRecord::for_test(
        frame.payload().to_vec(),
        frame.event_count(),
        frame.codec_metadata(),
    )
}

async fn append(sender: &DiskBufferSender, frame: &TestFrame) {
    assert_eq!(
        sender.send(record(frame)).await.unwrap(),
        SendOutcome::Accepted
    );
}

async fn read_and_finalize(
    receiver: &mut DiskBufferReceiver,
) -> crate::variants::disk_v3::readable_segment::OwnedDecodedFrame {
    let read = receiver.next().await.unwrap().expect("expected a frame");
    let position = receiver.read_position();
    finalize_and_wait(receiver, read, position).await
}

async fn finalize_and_wait(
    receiver: &DiskBufferReceiver,
    read: DiskBufferRead,
    position: Position,
) -> crate::variants::disk_v3::readable_segment::OwnedDecodedFrame {
    let DiskBufferRead {
        frame,
        batch_notifier,
    } = read;
    drop(batch_notifier);
    wait_for_reclaimable(receiver, position).await;
    frame
}

async fn wait_for_reclaimable(receiver: &DiskBufferReceiver, position: Position) {
    timeout(Duration::from_secs(1), async {
        loop {
            if receiver.state().reclaimable == position {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("finalized frame should be checkpointed");
}

async fn segment_exists(harness: &DiskV3Harness, segment_base_offset: u64) -> bool {
    let path = harness.path().join(segment_file_name(segment_base_offset));
    match tokio::fs::metadata(path).await {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => panic!("failed to inspect segment {segment_base_offset}.log: {error}"),
    }
}

async fn wait_for_segment_removal(harness: &DiskV3Harness, segment_base_offset: u64) {
    timeout(Duration::from_secs(1), async {
        while segment_exists(harness, segment_base_offset).await {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("reclaimable segment should be removed");
}

#[tokio::test]
async fn durable_checkpoint_reclaims_sealed_segments_and_retries_at_startup() {
    let harness = DiskV3Harness::new("vector-disk-v3-reclamation", MAX_FRAME_LEN);
    let frames = (0..3)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + index,
                NonZeroU32::MIN,
                0,
                format!("record-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let frame_len = frames[0].encoded_len();
    assert!(frames.iter().all(|frame| frame.encoded_len() == frame_len));
    let buffer_config = config(
        &harness,
        MAX_LOGICAL_BYTES,
        u64::try_from(frame_len).unwrap(),
        4096,
    );

    let (sender, mut receiver) = DiskBuffer::open(buffer_config.clone()).await.unwrap();
    for frame in &frames {
        append(&sender, frame).await;
    }
    sender.sync().await.unwrap();

    let first_segment_bytes = harness.segment_bytes(FIRST_RECORD_ID).await;
    for segment_base_offset in FIRST_RECORD_ID..FIRST_RECORD_ID + 3 {
        assert!(segment_exists(&harness, segment_base_offset).await);
    }

    let first = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");
    let first_position = receiver.read_position();
    finalize_and_wait(&receiver, first, first_position).await;
    sleep(Duration::from_millis(20)).await;
    assert!(
        segment_exists(&harness, FIRST_RECORD_ID).await,
        "the segment containing the durable checkpoint must be retained"
    );

    let second = receiver
        .next()
        .await
        .unwrap()
        .expect("expected second frame");
    let second_position = receiver.read_position();
    assert_eq!(second_position.segment_base_offset(), FIRST_RECORD_ID + 1);
    sleep(Duration::from_millis(20)).await;
    assert!(
        segment_exists(&harness, FIRST_RECORD_ID).await,
        "reading the next segment without acknowledging it must not reclaim data"
    );

    finalize_and_wait(&receiver, second, second_position).await;
    wait_for_segment_removal(&harness, FIRST_RECORD_ID).await;
    assert!(segment_exists(&harness, FIRST_RECORD_ID + 1).await);
    assert!(segment_exists(&harness, FIRST_RECORD_ID + 2).await);

    sender.shutdown().await.unwrap();
    drop(sender);
    drop(receiver);

    // Model a deletion that was not durable across a crash. The checkpoint is
    // already in the next segment, so the reclaimer's initial watch value must
    // remove the reappearing older segment without another acknowledgement.
    tokio::fs::write(
        harness.path().join(segment_file_name(FIRST_RECORD_ID)),
        first_segment_bytes,
    )
    .await
    .unwrap();
    let (resumed_sender, _resumed_receiver) = DiskBuffer::open(buffer_config).await.unwrap();
    wait_for_segment_removal(&harness, FIRST_RECORD_ID).await;
    resumed_sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn actor_backed_buffer_initializes_sends_reads_and_resumes() {
    let harness = DiskV3Harness::new("vector-disk-v3-buffer", MAX_FRAME_LEN);
    let frames = (0..3)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + index,
                NonZeroU32::MIN,
                0,
                format!("record-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let frame_len = frames[0].encoded_len();
    assert!(frames.iter().all(|frame| frame.encoded_len() == frame_len));
    let buffer_config = config(
        &harness,
        MAX_LOGICAL_BYTES,
        u64::try_from(frame_len).unwrap(),
        4096,
    );

    let (sender, mut receiver) = DiskBuffer::open(buffer_config.clone()).await.unwrap();
    assert_eq!(sender.logical_bytes(), 0);

    for frame in &frames {
        append(&sender, frame).await;
    }
    sender.sync().await.unwrap();
    assert_eq!(
        sender.logical_bytes(),
        u64::try_from(frame_len * frames.len()).unwrap()
    );

    let first = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");
    assert_eq!(first.frame.record_id(), frames[0].record_id());
    let restart_position = receiver.read_position();
    finalize_and_wait(&receiver, first, restart_position).await;
    assert_eq!(receiver.read_position(), restart_position);
    assert_eq!(sender.state().reclaimable, restart_position);
    assert_eq!(
        sender.logical_bytes(),
        u64::try_from(frame_len * (frames.len() - 1)).unwrap()
    );
    sender.shutdown().await.unwrap();
    drop(sender);
    drop(receiver);

    let (resumed_sender, mut resumed_receiver) = DiskBuffer::open(buffer_config).await.unwrap();
    assert_eq!(
        resumed_sender.logical_bytes(),
        u64::try_from(frame_len * (frames.len() - 1)).unwrap()
    );

    let appended = harness.frame(
        FIRST_RECORD_ID + u64::try_from(frames.len()).unwrap(),
        NonZeroU32::MIN,
        0,
        "record-3",
    );
    append(&resumed_sender, &appended).await;
    resumed_sender.flush().await.unwrap();

    for expected in frames.iter().skip(1).chain(std::iter::once(&appended)) {
        let actual = read_and_finalize(&mut resumed_receiver).await;
        assert_eq!(actual.record_id(), expected.record_id());
        assert_eq!(actual.payload().as_ref(), expected.payload());
    }
    resumed_sender.shutdown().await.unwrap();
    assert!(resumed_receiver.next().await.unwrap().is_none());
}

#[tokio::test]
async fn cloned_senders_are_serialized_into_contiguous_actor_assigned_record_ids() {
    let harness = DiskV3Harness::new("vector-disk-v3-mpsc", MAX_FRAME_LEN);
    let buffer_config = config(&harness, MAX_LOGICAL_BYTES, 4096, 4096);
    let (sender, mut receiver) = DiskBuffer::open(buffer_config).await.unwrap();

    let mut tasks = Vec::new();
    for producer in 0..16_u64 {
        let sender = sender.clone();
        tasks.push(tokio::spawn(async move {
            let record =
                PreparedRecord::for_test(format!("producer-{producer}"), NonZeroU32::MIN, 7);
            sender.send(record).await.unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), SendOutcome::Accepted);
    }
    sender.flush().await.unwrap();

    let mut payloads = BTreeSet::new();
    for index in 0..16_u64 {
        let frame = read_and_finalize(&mut receiver).await;
        assert_eq!(frame.record_id(), FIRST_RECORD_ID + index);
        assert_eq!(frame.codec_metadata(), 7);
        payloads.insert(String::from_utf8(frame.payload().to_vec()).unwrap());
    }
    assert_eq!(payloads.len(), 16);
    assert!(payloads.contains("producer-0"));
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn drop_newest_consumes_the_record_without_reconstructing_it() {
    let harness = DiskV3Harness::new("vector-disk-v3-drop-newest", MAX_FRAME_LEN);
    let first = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "same-sized-record");
    let second = harness.frame(FIRST_RECORD_ID + 1, NonZeroU32::MIN, 0, "same-sized-record");
    let frame_len = u64::try_from(first.encoded_len()).unwrap();
    let (sender, mut receiver) = DiskBuffer::open(config(
        &harness,
        frame_len,
        frame_len * 2,
        usize::try_from(frame_len * 2).unwrap(),
    ))
    .await
    .unwrap();

    append(&sender, &first).await;
    let (batch, status) = BatchNotifier::new_with_receiver();
    let finalizers =
        EventFinalizerGroups::from_flat(EventFinalizers::new(EventFinalizer::new(batch)));
    let second = PreparedRecord::new(
        second.payload().to_vec(),
        second.event_count(),
        second.codec_metadata(),
        finalizers,
    );
    assert_eq!(
        sender.try_send(second).await.unwrap(),
        TrySendOutcome::DroppedNewest
    );
    assert_eq!(status.await, BatchStatus::Delivered);
    assert_eq!(sender.logical_bytes(), frame_len);

    sender.flush().await.unwrap();
    let actual = read_and_finalize(&mut receiver).await;
    assert_eq!(actual.record_id(), FIRST_RECORD_ID);
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn soft_capacity_admits_one_complete_frame_across_the_limit() {
    let harness = DiskV3Harness::new("vector-disk-v3-soft-capacity", MAX_FRAME_LEN);
    let frames = (0..3)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + index,
                NonZeroU32::MIN,
                0,
                "same-sized-record",
            )
        })
        .collect::<Vec<_>>();
    let frame_len = u64::try_from(frames[0].encoded_len()).unwrap();
    let logical_high_water_mark = frame_len + 1;
    let (sender, mut receiver) = DiskBuffer::open(config(
        &harness,
        logical_high_water_mark,
        frame_len * 3,
        usize::try_from(frame_len * 3).unwrap(),
    ))
    .await
    .unwrap();

    append(&sender, &frames[0]).await;
    assert_eq!(
        sender.try_send(record(&frames[1])).await.unwrap(),
        TrySendOutcome::Accepted
    );
    assert_eq!(sender.logical_bytes(), frame_len * 2);
    assert!(sender.logical_bytes() > logical_high_water_mark);

    assert_eq!(
        sender.try_send(record(&frames[2])).await.unwrap(),
        TrySendOutcome::DroppedNewest
    );

    sender.flush().await.unwrap();
    for expected in &frames[..2] {
        let actual = read_and_finalize(&mut receiver).await;
        assert_eq!(actual.record_id(), expected.record_id());
    }
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_batch_is_invisible_until_flush_command_reaches_the_actor() {
    let harness = DiskV3Harness::new("vector-disk-v3-visibility", MAX_FRAME_LEN);
    let frame = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "record");
    let (sender, mut receiver) = DiskBuffer::open(config(&harness, MAX_LOGICAL_BYTES, 4096, 4096))
        .await
        .unwrap();
    append(&sender, &frame).await;

    let read = {
        let next = receiver.next();
        tokio::pin!(next);
        assert!(timeout(Duration::from_millis(20), &mut next).await.is_err());
        sender.flush().await.unwrap();
        timeout(Duration::from_secs(1), &mut next)
            .await
            .unwrap()
            .unwrap()
            .expect("flush should make the frame visible")
    };
    let position = receiver.read_position();
    finalize_and_wait(&receiver, read, position).await;
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn ingress_finalizers_wait_for_their_frame_to_be_durable() {
    let harness = DiskV3Harness::new("vector-disk-v3-ingress-durability", MAX_FRAME_LEN);
    let frame = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "record");
    let mut buffer_config = config(&harness, MAX_LOGICAL_BYTES, 4096, 4096);
    buffer_config.writer.sync_interval = Duration::from_mins(1);
    let (sender, mut receiver) = DiskBuffer::open(buffer_config).await.unwrap();

    let (batch, status) = BatchNotifier::new_with_receiver();
    let finalizers =
        EventFinalizerGroups::from_flat(EventFinalizers::new(EventFinalizer::new(batch)));
    let record = PreparedRecord::new(
        frame.payload().to_vec(),
        frame.event_count(),
        frame.codec_metadata(),
        finalizers,
    );
    assert_eq!(sender.send(record).await.unwrap(), SendOutcome::Accepted);

    // Flush is an actor ordering barrier and makes the frame readable, but the
    // long sync interval ensures it does not make the frame durable.
    sender.flush().await.unwrap();
    tokio::pin!(status);
    assert!(
        timeout(Duration::from_millis(20), &mut status)
            .await
            .is_err()
    );

    sender.sync().await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut status).await.unwrap(),
        BatchStatus::Delivered
    );

    let read = receiver.next().await.unwrap().expect("expected a frame");
    let position = receiver.read_position();
    finalize_and_wait(&receiver, read, position).await;
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn acknowledgement_does_not_publish_an_unrelated_partial_batch() {
    let harness = DiskV3Harness::new("vector-disk-v3-ack-visibility", MAX_FRAME_LEN);
    let first = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "record-0");
    let second = harness.frame(FIRST_RECORD_ID + 1, NonZeroU32::MIN, 0, "record-1");
    let mut buffer_config = config(&harness, MAX_LOGICAL_BYTES, 4096, 4096);
    buffer_config.writer.sync_interval = Duration::from_mins(1);
    let (sender, mut receiver) = DiskBuffer::open(buffer_config).await.unwrap();

    append(&sender, &first).await;
    sender.flush().await.unwrap();
    let first = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");

    // This append reaches the actor before the acknowledgement, but remains
    // below the publication threshold.
    append(&sender, &second).await;
    let position = receiver.read_position();
    finalize_and_wait(&receiver, first, position).await;
    assert!(
        timeout(Duration::from_millis(20), receiver.next())
            .await
            .is_err(),
        "acknowledging a committed frame must not publish a partial batch"
    );

    sender.flush().await.unwrap();
    let second = read_and_finalize(&mut receiver).await;
    assert_eq!(second.record_id(), FIRST_RECORD_ID + 1);
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn blocking_send_wakes_only_after_acknowledgement_is_checkpointed() {
    let harness = DiskV3Harness::new("vector-disk-v3-capacity", MAX_FRAME_LEN);
    let first = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "same-length-record-0");
    let second = harness.frame(
        FIRST_RECORD_ID + 1,
        NonZeroU32::MIN,
        0,
        "same-length-record-1",
    );
    assert_eq!(first.encoded_len(), second.encoded_len());
    let frame_len = u64::try_from(first.encoded_len()).unwrap();
    let (sender, mut receiver) = DiskBuffer::open(config(
        &harness,
        frame_len,
        frame_len * 2,
        usize::try_from(frame_len * 2).unwrap(),
    ))
    .await
    .unwrap();

    append(&sender, &first).await;
    sender.flush().await.unwrap();
    let waiting_sender = sender.clone();
    let waiting = tokio::spawn(async move { waiting_sender.send(record(&second)).await });
    sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "a full logical buffer must block");

    let read = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");
    sleep(Duration::from_millis(20)).await;
    assert!(
        !waiting.is_finished(),
        "reading without acknowledgement must not release capacity"
    );
    drop(read.batch_notifier);
    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        SendOutcome::Accepted
    );
    sender.flush().await.unwrap();
    let second = read_and_finalize(&mut receiver).await;
    assert_eq!(second.record_id(), FIRST_RECORD_ID + 1);
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordered_finalizer_waits_for_earlier_frames() {
    let harness = DiskV3Harness::new("vector-disk-v3-ack-order", MAX_FRAME_LEN);
    let frames = (0..2)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + index,
                NonZeroU32::MIN,
                0,
                format!("record-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let (sender, mut receiver) = DiskBuffer::open(config(&harness, MAX_LOGICAL_BYTES, 4096, 4096))
        .await
        .unwrap();
    for frame in &frames {
        append(&sender, frame).await;
    }
    sender.flush().await.unwrap();

    let first = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");
    let first_position = receiver.read_position();
    let second = receiver
        .next()
        .await
        .unwrap()
        .expect("expected second frame");
    let second_position = receiver.read_position();

    drop(second.batch_notifier);
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        sender.state().reclaimable,
        Position::at_segment_start(FIRST_RECORD_ID),
        "a later completion must not advance the checkpoint"
    );

    drop(first.batch_notifier);
    wait_for_reclaimable(&receiver, second_position).await;
    assert_eq!(sender.state().consumer_acknowledged, second_position);
    assert_ne!(sender.state().consumer_acknowledged, first_position);
    assert_eq!(sender.state().status, WriterStatus::Running);
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn periodic_sync_advances_durability_without_publishing_partial_batches() {
    let harness = DiskV3Harness::new("vector-disk-v3-periodic-sync", MAX_FRAME_LEN);
    let frame = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "record");
    let mut buffer_config = config(&harness, MAX_LOGICAL_BYTES, 4096, frame.encoded_len());
    buffer_config.writer.sync_interval = Duration::from_millis(20);
    let (sender, mut receiver) = DiskBuffer::open(buffer_config).await.unwrap();

    append(&sender, &frame).await;
    sender.flush().await.unwrap();
    let committed = sender.state().committed;
    timeout(Duration::from_secs(1), async {
        loop {
            if sender.state().data_synced == committed {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    let frame = read_and_finalize(&mut receiver).await;
    assert_eq!(frame.record_id(), FIRST_RECORD_ID);
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_wakes_a_producer_blocked_on_logical_capacity() {
    let harness = DiskV3Harness::new("vector-disk-v3-shutdown-wakeup", MAX_FRAME_LEN);
    let first = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "same-length-record-0");
    let second = harness.frame(
        FIRST_RECORD_ID + 1,
        NonZeroU32::MIN,
        0,
        "same-length-record-1",
    );
    let frame_len = u64::try_from(first.encoded_len()).unwrap();
    let (sender, receiver) = DiskBuffer::open(config(
        &harness,
        frame_len,
        frame_len * 2,
        usize::try_from(frame_len * 2).unwrap(),
    ))
    .await
    .unwrap();
    append(&sender, &first).await;

    let waiting_sender = sender.clone();
    let waiting = tokio::spawn(async move { waiting_sender.send(record(&second)).await });
    sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());

    sender.shutdown().await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap(),
        Err(DiskBufferError::ActorClosed)
    ));
    assert_eq!(receiver.state().status, WriterStatus::Closed);
}

#[tokio::test]
async fn blocking_send_rejects_an_already_closed_actor() {
    let harness = DiskV3Harness::new("vector-disk-v3-send-after-shutdown", MAX_FRAME_LEN);
    let frame = harness.frame(FIRST_RECORD_ID, NonZeroU32::MIN, 0, "record");
    let (sender, _receiver) = DiskBuffer::open(config(&harness, MAX_LOGICAL_BYTES, 4096, 4096))
        .await
        .unwrap();

    sender.shutdown().await.unwrap();

    assert!(matches!(
        sender.send(record(&frame)).await,
        Err(DiskBufferError::ActorClosed)
    ));
    assert_eq!(sender.logical_bytes(), 0);
}

#[tokio::test]
async fn torn_checkpoint_replays_from_the_earliest_retained_segment() {
    let harness = DiskV3Harness::new("vector-disk-v3-torn-checkpoint", MAX_FRAME_LEN);
    let frames = (0..2)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + index,
                NonZeroU32::MIN,
                0,
                format!("record-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let buffer_config = config(&harness, MAX_LOGICAL_BYTES, 4096, 4096);
    let (sender, mut receiver) = DiskBuffer::open(buffer_config.clone()).await.unwrap();
    for frame in &frames {
        append(&sender, frame).await;
    }
    sender.sync().await.unwrap();
    let first = receiver
        .next()
        .await
        .unwrap()
        .expect("expected first frame");
    let position = receiver.read_position();
    finalize_and_wait(&receiver, first, position).await;
    sender.shutdown().await.unwrap();
    drop(sender);
    drop(receiver);

    let checkpoint_path = harness.path().join(CHECKPOINT_FILE_NAME);
    let mut checkpoint_bytes = tokio::fs::read(&checkpoint_path).await.unwrap();
    checkpoint_bytes[0] ^= 0xff;
    tokio::fs::write(checkpoint_path, checkpoint_bytes)
        .await
        .unwrap();

    let (resumed_sender, mut resumed_receiver) = DiskBuffer::open(buffer_config).await.unwrap();
    assert_eq!(
        resumed_sender.state().reclaimable,
        Position::at_segment_start(FIRST_RECORD_ID)
    );
    let first = resumed_receiver
        .next()
        .await
        .unwrap()
        .expect("expected replayed frame");
    assert_eq!(first.frame.record_id(), FIRST_RECORD_ID);
    let position = resumed_receiver.read_position();
    finalize_and_wait(&resumed_receiver, first, position).await;
    resumed_sender.shutdown().await.unwrap();
}
