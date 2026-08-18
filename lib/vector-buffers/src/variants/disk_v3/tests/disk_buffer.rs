use std::num::NonZeroU32;

use crate::variants::disk_v3::{
    disk_buffer::DiskBuffer, position::Position, segmented_log_reader::SegmentedRead,
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

    let SegmentedRead::Frame {
        frame,
        position: restart_position,
    } = buffer.next().await.unwrap()
    else {
        panic!("expected the first frame");
    };
    assert_eq!(frame.record_id(), frames[0].record_id());
    assert_eq!(buffer.read_position(), restart_position);
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
        let SegmentedRead::Frame { frame, .. } = resumed.next().await.unwrap() else {
            panic!("expected record {}", expected.record_id());
        };
        assert_eq!(frame.record_id(), expected.record_id());
        assert_eq!(frame.payload().as_ref(), expected.payload());
    }
    assert_eq!(
        resumed.next().await.unwrap(),
        SegmentedRead::EndOfAvailableData
    );
}
