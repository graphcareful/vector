use std::num::NonZeroU32;

use futures::StreamExt;
use tokio::time::{Duration, timeout};
use vector_common::{finalization::BatchNotifier, finalizer::OrderedFinalizer};

use crate::variants::disk_v3::{
    acknowledgement::AcknowledgementError,
    disk_buffer::{DiskBuffer, DiskBufferError, DiskBufferRead},
    position::Position,
};

use super::harness::{DiskV3Harness, TestFrame, writer_config};

const FIRST_RECORD_ID: u64 = 100;
const MAX_FRAME_LEN: usize = 1024;
const MAX_LOGICAL_BYTES: u64 = 1024 * 1024;

async fn append(buffer: &mut DiskBuffer, frame: &TestFrame) {
    let reservation = buffer
        .capacity()
        .reserve(frame.encoded_len())
        .await
        .unwrap();
    buffer
        .append(frame.prepared().clone(), reservation)
        .await
        .unwrap();
}

#[tokio::test]
async fn high_level_buffer_initializes_appends_reads_and_resumes() {
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
    let config = writer_config(u64::try_from(frame_len).unwrap(), 4096);

    let mut buffer = DiskBuffer::open(
        harness.path(),
        Position::at_segment_start(FIRST_RECORD_ID),
        MAX_FRAME_LEN,
        MAX_LOGICAL_BYTES,
        config,
    )
    .await
    .unwrap();
    assert_eq!(buffer.logical_bytes(), 0);

    for frame in &frames {
        append(&mut buffer, frame).await;
    }
    buffer.sync_all().await.unwrap();
    assert_eq!(
        buffer.logical_bytes(),
        u64::try_from(frame_len * frames.len()).unwrap()
    );

    let DiskBufferRead::Frame {
        frame,
        acknowledgement,
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the first frame");
    };
    assert_eq!(frame.record_id(), frames[0].record_id());
    let restart_position = buffer.acknowledge(acknowledgement).unwrap();
    assert_eq!(buffer.read_position(), restart_position);
    assert_eq!(buffer.observed_acknowledged_position(), restart_position);
    assert_eq!(
        buffer.reclaimable_position(),
        Position::at_segment_start(FIRST_RECORD_ID)
    );
    assert_eq!(
        buffer.logical_bytes(),
        u64::try_from(frame_len * frames.len()).unwrap()
    );
    drop(buffer);

    let mut resumed = DiskBuffer::open(
        harness.path(),
        restart_position,
        MAX_FRAME_LEN,
        MAX_LOGICAL_BYTES,
        config,
    )
    .await
    .unwrap();
    assert_eq!(
        resumed.logical_bytes(),
        u64::try_from(frame_len * (frames.len() - 1)).unwrap()
    );

    let appended = harness.frame(
        FIRST_RECORD_ID + u64::try_from(frames.len()).unwrap(),
        NonZeroU32::MIN,
        0,
        "record-3",
    );
    assert_eq!(appended.encoded_len(), frame_len);
    append(&mut resumed, &appended).await;
    resumed.flush().await.unwrap();
    assert_eq!(
        resumed.logical_bytes(),
        u64::try_from(frame_len * frames.len()).unwrap()
    );

    for expected in frames.iter().skip(1).chain(std::iter::once(&appended)) {
        let DiskBufferRead::Frame { frame, .. } = resumed.next().await.unwrap() else {
            panic!("expected record {}", expected.record_id());
        };
        assert_eq!(frame.record_id(), expected.record_id());
        assert_eq!(frame.payload().as_ref(), expected.payload());
    }
    assert_eq!(
        resumed.next().await.unwrap(),
        DiskBufferRead::EndOfAvailableData
    );
}

#[tokio::test]
async fn ordered_finalizer_turns_out_of_order_completion_into_ordered_acknowledgements() {
    let harness = DiskV3Harness::new("vector-disk-v3-acknowledgements", MAX_FRAME_LEN);
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
    let frame_len = frames[0].encoded_len();
    let config = writer_config(u64::try_from(frame_len * frames.len()).unwrap(), 4096);
    let initial = Position::at_segment_start(FIRST_RECORD_ID);
    let mut buffer = DiskBuffer::open(
        harness.path(),
        initial,
        MAX_FRAME_LEN,
        MAX_LOGICAL_BYTES,
        config,
    )
    .await
    .unwrap();
    for frame in &frames {
        append(&mut buffer, frame).await;
    }
    buffer.sync_all().await.unwrap();

    let DiskBufferRead::Frame {
        acknowledgement: first_acknowledgement,
        ..
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the first frame");
    };
    let first_position = buffer.read_position();
    let DiskBufferRead::Frame {
        acknowledgement: second_acknowledgement,
        ..
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the second frame");
    };
    let second_position = buffer.read_position();

    let (finalizer, mut completed) = OrderedFinalizer::new(None);
    let (first_batch, first_receiver) = BatchNotifier::new_with_receiver();
    let (second_batch, second_receiver) = BatchNotifier::new_with_receiver();
    finalizer.add(first_acknowledgement, first_receiver);
    finalizer.add(second_acknowledgement, second_receiver);

    drop(second_batch);
    assert!(
        timeout(Duration::from_millis(20), completed.next())
            .await
            .is_err(),
        "the second acknowledgement must wait for the first"
    );

    drop(first_batch);
    let (_status, first_acknowledgement) = completed.next().await.unwrap();
    assert_eq!(
        buffer.acknowledge(first_acknowledgement).unwrap(),
        first_position
    );
    let (_status, second_acknowledgement) = completed.next().await.unwrap();
    assert_eq!(
        buffer.acknowledge(second_acknowledgement).unwrap(),
        second_position
    );

    assert_eq!(buffer.observed_acknowledged_position(), second_position);
    assert_eq!(buffer.reclaimable_position(), initial);
    assert_eq!(
        buffer.logical_bytes(),
        u64::try_from(frame_len * frames.len()).unwrap()
    );
}

#[tokio::test]
async fn disk_buffer_rejects_acknowledgements_outside_read_order() {
    let harness = DiskV3Harness::new("vector-disk-v3-acknowledgement-order", MAX_FRAME_LEN);
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
    let config = writer_config(1024, 4096);
    let initial = Position::at_segment_start(FIRST_RECORD_ID);
    let mut buffer = DiskBuffer::open(
        harness.path(),
        initial,
        MAX_FRAME_LEN,
        MAX_LOGICAL_BYTES,
        config,
    )
    .await
    .unwrap();
    for frame in &frames {
        append(&mut buffer, frame).await;
    }
    buffer.sync_all().await.unwrap();

    let DiskBufferRead::Frame {
        acknowledgement: _first_acknowledgement,
        ..
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the first frame");
    };
    let DiskBufferRead::Frame {
        acknowledgement: second_acknowledgement,
        ..
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the second frame");
    };

    assert!(matches!(
        buffer.acknowledge(second_acknowledgement),
        Err(DiskBufferError::Acknowledgement {
            source: AcknowledgementError::OutOfOrder {
                expected: 0,
                actual: 1,
            },
        })
    ));
    assert_eq!(buffer.observed_acknowledged_position(), initial);
    assert_eq!(buffer.reclaimable_position(), initial);
}
