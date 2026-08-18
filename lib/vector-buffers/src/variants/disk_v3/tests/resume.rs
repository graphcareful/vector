use std::num::NonZeroU32;

use crate::variants::disk_v3::{position::Position, segmented_log_reader::SegmentedRead};

use super::harness::{DiskV3Harness, writer_config};

const FIRST_RECORD_ID: u64 = 100;
const RECORD_COUNT: usize = 6;
const MAX_FRAME_LEN: usize = 1024;

#[tokio::test]
async fn writer_open_or_create_resumes_the_existing_active_segment() {
    let harness = DiskV3Harness::new("vector-disk-v3-resume", MAX_FRAME_LEN);
    let frames = (0..RECORD_COUNT)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + u64::try_from(index).unwrap(),
                NonZeroU32::MIN,
                0,
                format!("record-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let frame_len = frames[0].encoded_len();
    let segment_size = u64::try_from(frame_len * 2).unwrap();
    let config = writer_config(segment_size, 4096);

    let mut first_writer = harness.open_writer(FIRST_RECORD_ID, config).await;
    for frame in &frames[..3] {
        first_writer.append(frame.prepared().clone()).await.unwrap();
    }
    let first_end = first_writer.sync_all().await.unwrap();
    drop(first_writer);

    assert_eq!(first_end.segment_base_offset(), FIRST_RECORD_ID + 2);
    assert_eq!(
        first_end.segment_byte_offset(),
        u64::try_from(frame_len).unwrap()
    );
    assert_eq!(first_end.next_record_id(), FIRST_RECORD_ID + 3);

    let mut resumed = harness.open_writer(0, config).await;
    assert_eq!(resumed.committed(), first_end);
    assert_eq!(
        resumed.data_synced(),
        Position::at_segment_start(first_end.segment_base_offset())
    );
    for frame in &frames[3..] {
        resumed.append(frame.prepared().clone()).await.unwrap();
    }
    let final_end = resumed.sync_all().await.unwrap();
    drop(resumed);

    assert_eq!(final_end.segment_base_offset(), FIRST_RECORD_ID + 4);
    assert_eq!(final_end.segment_byte_offset(), segment_size);
    assert_eq!(
        final_end.next_record_id(),
        FIRST_RECORD_ID + u64::try_from(RECORD_COUNT).unwrap()
    );

    let mut reader = harness
        .open_reader(Position::at_segment_start(FIRST_RECORD_ID))
        .await;
    for expected in &frames {
        let SegmentedRead::Frame { frame, .. } = reader.read_next().await.unwrap() else {
            panic!("expected record {}", expected.record_id());
        };
        assert_eq!(frame.record_id(), expected.record_id());
        assert_eq!(frame.payload().as_ref(), expected.payload());
    }
    assert_eq!(
        reader.read_next().await.unwrap(),
        SegmentedRead::EndOfAvailableData
    );
}
