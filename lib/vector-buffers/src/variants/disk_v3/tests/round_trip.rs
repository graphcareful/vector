use std::{collections::BTreeSet, ffi::OsString, num::NonZeroU32};

use crate::variants::disk_v3::{
    position::Position, segment_files::segment_file_name, segmented_log_reader::SegmentedRead,
};

use super::harness::{DiskV3Harness, TestFrame, writer_config};

const FIRST_RECORD_ID: u64 = 100;
const RECORD_COUNT: usize = 8;
const FRAMES_PER_SEGMENT: usize = 2;
const MAX_FRAME_LEN: usize = 1024;

#[tokio::test]
async fn writes_rolls_and_reads_frames_with_restartable_positions() {
    let harness = DiskV3Harness::new("vector-disk-v3-round-trip", MAX_FRAME_LEN);
    let frames = test_frames(&harness);
    let frame_len = frames[0].encoded_len();
    assert!(frames.iter().all(|frame| frame.encoded_len() == frame_len));
    let segment_size = u64::try_from(frame_len * FRAMES_PER_SEGMENT).unwrap();

    write_and_assert_rolls(&harness, &frames, segment_size).await;
    assert_segment_files(&harness, &frames, segment_size).await;
    let positions = read_and_assert_frames(&harness, &frames, frame_len).await;
    assert_positions_are_restartable(&harness, &frames, &positions, frame_len).await;
}

fn test_frames(harness: &DiskV3Harness) -> Vec<TestFrame> {
    (0..RECORD_COUNT)
        .map(|index| {
            harness.frame(
                FIRST_RECORD_ID + u64::try_from(index).unwrap(),
                NonZeroU32::MIN,
                u32::try_from(index).unwrap(),
                format!("record-{index}").into_bytes(),
            )
        })
        .collect()
}

async fn write_and_assert_rolls(harness: &DiskV3Harness, frames: &[TestFrame], segment_size: u64) {
    let mut writer = harness
        .create_writer(FIRST_RECORD_ID, writer_config(segment_size, 4096))
        .await;

    for (index, frame) in frames.iter().enumerate() {
        let sealed = writer.append(frame.prepared().clone()).await.unwrap();
        let should_rotate = index > 0 && index % FRAMES_PER_SEGMENT == 0;
        assert_eq!(sealed.is_some(), should_rotate);
    }

    let written_end = writer.sync_all().await.unwrap();
    let expected_last_segment_base_offset =
        FIRST_RECORD_ID + u64::try_from(RECORD_COUNT - FRAMES_PER_SEGMENT).unwrap();
    assert_eq!(
        written_end.segment_base_offset(),
        expected_last_segment_base_offset
    );
    assert_eq!(written_end.segment_byte_offset(), segment_size);
    assert_eq!(
        written_end.next_record_id(),
        FIRST_RECORD_ID + u64::try_from(RECORD_COUNT).unwrap()
    );
    assert_eq!(writer.committed(), writer.data_synced());
}

async fn assert_segment_files(harness: &DiskV3Harness, frames: &[TestFrame], segment_size: u64) {
    let expected_segments = expected_segment_contents(frames);
    let expected_names = expected_segments
        .iter()
        .map(|(segment_base_offset, _)| OsString::from(segment_file_name(*segment_base_offset)))
        .collect::<BTreeSet<_>>();
    assert_eq!(harness.entry_names().await, expected_names);

    for (segment_base_offset, expected_bytes) in expected_segments {
        let actual_bytes = harness.segment_bytes(segment_base_offset).await;
        assert_eq!(actual_bytes, expected_bytes);
        assert_eq!(u64::try_from(actual_bytes.len()).unwrap(), segment_size);
    }
}

async fn read_and_assert_frames(
    harness: &DiskV3Harness,
    frames: &[TestFrame],
    frame_len: usize,
) -> Vec<Position> {
    let mut reader = harness
        .open_reader(Position::at_segment_start(FIRST_RECORD_ID))
        .await;
    let mut positions = Vec::with_capacity(frames.len());

    for (index, expected_frame) in frames.iter().enumerate() {
        let SegmentedRead::Frame { frame, position } = reader.read_next().await.unwrap() else {
            panic!("expected frame {index}");
        };
        let expected_position = expected_position_after(index, frame_len);

        assert_eq!(frame.record_id(), expected_frame.record_id());
        assert_eq!(frame.event_count(), expected_frame.event_count());
        assert_eq!(frame.codec_metadata(), expected_frame.codec_metadata());
        assert_eq!(frame.payload().as_ref(), expected_frame.payload());
        assert_eq!(frame.frame_len(), expected_frame.encoded_len());
        assert_eq!(position, expected_position);
        assert_eq!(reader.read_position(), position);
        positions.push(position);
    }

    assert_eq!(
        reader.read_next().await.unwrap(),
        SegmentedRead::EndOfAvailableData
    );
    assert_eq!(reader.read_position(), *positions.last().unwrap());
    positions
}

async fn assert_positions_are_restartable(
    harness: &DiskV3Harness,
    frames: &[TestFrame],
    positions: &[Position],
    frame_len: usize,
) {
    // Include checkpoints at exact segment ends in the restart coverage.
    for (index, position) in positions.iter().copied().enumerate() {
        let mut restarted = harness.open_reader(position).await;

        if index + 1 == frames.len() {
            assert_eq!(
                restarted.read_next().await.unwrap(),
                SegmentedRead::EndOfAvailableData
            );
        } else {
            let SegmentedRead::Frame {
                frame,
                position: next_position,
            } = restarted.read_next().await.unwrap()
            else {
                panic!("expected frame after checkpoint {index}");
            };
            assert_eq!(frame.record_id(), frames[index + 1].record_id());
            assert_eq!(next_position, expected_position_after(index + 1, frame_len));
        }
    }
}

fn expected_segment_contents(frames: &[TestFrame]) -> Vec<(u64, Vec<u8>)> {
    frames
        .chunks(FRAMES_PER_SEGMENT)
        .map(|frames| {
            let mut bytes = Vec::new();
            for frame in frames {
                bytes.extend_from_slice(frame.encoded_bytes());
            }
            (frames[0].record_id(), bytes)
        })
        .collect()
}

fn expected_position_after(index: usize, frame_len: usize) -> Position {
    let segment_index = index / FRAMES_PER_SEGMENT;
    let frames_read_in_segment = index % FRAMES_PER_SEGMENT + 1;
    Position::new(
        FIRST_RECORD_ID + u64::try_from(segment_index * FRAMES_PER_SEGMENT).unwrap(),
        u64::try_from(frames_read_in_segment * frame_len).unwrap(),
        FIRST_RECORD_ID + u64::try_from(index + 1).unwrap(),
    )
}
