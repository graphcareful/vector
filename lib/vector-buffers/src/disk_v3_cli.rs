use std::{
    error::Error,
    fmt,
    io::{self, Write},
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use crate::variants::disk_v3::{
    frame::encode_frame,
    position::Position,
    segmented_log_reader::{SegmentedLogReader, SegmentedRead},
    segmented_log_writer::{
        FilesystemSegmentStorage, SegmentedLogWriter, SegmentedLogWriterConfig,
    },
};
use clap::{Args, Parser, Subcommand};
use rand::{
    Rng, SeedableRng,
    distr::{Alphanumeric, SampleString},
    rngs::SmallRng,
};
use serde_json::{Value, json};

type ToolResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const DEFAULT_SEGMENT_SIZE: &str = "1048576";
const DEFAULT_BATCH_SIZE: &str = "65536";
const DEFAULT_MAX_FRAME_SIZE: &str = "1048576";
const DEFAULT_RECORD_COUNT: &str = "100";
const DEFAULT_FLUSH_EVERY: &str = "100";
const DEFAULT_MESSAGE_BYTES: usize = 32;
const DEFAULT_SYNC_INTERVAL_MS: u64 = 500;

#[derive(Debug, Parser)]
#[command(
    name = "disk-v3",
    about = "Generate JSON events in a disk-v3 data directory and read them back"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Append randomly generated JSON events, creating the log when necessary.
    Write(WriteArgs),
    /// Read JSON events from a segmented log, optionally from a saved position.
    Read(ReadArgs),
}

#[derive(Debug, Args)]
struct WriteArgs {
    /// Data directory containing, or receiving, record-base-named segment files.
    data_dir: PathBuf,

    /// Number of single-event frames to generate.
    #[arg(long, alias = "count", default_value = DEFAULT_RECORD_COUNT)]
    records: NonZeroU64,

    /// Target maximum segment size in bytes.
    #[arg(long, default_value = DEFAULT_SEGMENT_SIZE)]
    segment_size: NonZeroU64,

    /// Aggregated write threshold in bytes.
    #[arg(long, default_value = DEFAULT_BATCH_SIZE)]
    batch_size: NonZeroUsize,

    /// Maximum encoded frame size accepted by the codec, in bytes.
    #[arg(long, default_value = DEFAULT_MAX_FRAME_SIZE)]
    max_frame_size: NonZeroUsize,

    /// Number of random alphanumeric bytes in each event's message field.
    #[arg(long, default_value_t = DEFAULT_MESSAGE_BYTES)]
    message_bytes: usize,

    /// Flush any partial write batch after this many generated events.
    #[arg(long, default_value = DEFAULT_FLUSH_EVERY)]
    flush_every: NonZeroU64,

    /// Minimum interval between opportunistic fsync operations triggered by flush.
    #[arg(long, default_value_t = DEFAULT_SYNC_INTERVAL_MS)]
    sync_interval_ms: u64,

    /// First record ID for a new log. Existing logs recover their next ID.
    #[arg(long, default_value_t = 0)]
    start_record_id: u64,

    /// Codec metadata stored in every generated frame.
    #[arg(long, default_value_t = 0)]
    codec_metadata: u32,

    /// Deterministic random seed. A random seed is selected and printed when omitted.
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Debug, Args)]
struct ReadArgs {
    /// Data directory containing record-base-named segment files.
    data_dir: PathBuf,

    /// Record ID encoded in the selected segment's file name.
    #[arg(long, alias = "segment-start", default_value_t = 0)]
    segment_base_offset: u64,

    /// Byte offset within the selected segment from a saved read position.
    #[arg(long, alias = "segment-offset", default_value_t = 0)]
    segment_byte_offset: u64,

    /// Record ID expected in the next unread frame.
    #[arg(long, default_value_t = 0)]
    next_record_id: u64,

    /// Maximum encoded frame size accepted by the decoder, in bytes.
    #[arg(long, default_value = DEFAULT_MAX_FRAME_SIZE)]
    max_frame_size: NonZeroUsize,

    /// Stop after reading this many frames instead of reading to the current end.
    #[arg(long)]
    limit: Option<NonZeroU64>,

    /// Wrap each event with its frame metadata and next restart position.
    #[arg(long)]
    with_positions: bool,

    /// Pretty-print JSON instead of emitting one compact object per line.
    #[arg(long)]
    pretty: bool,
}

/// Parses command-line arguments and runs the selected disk-v3 operation.
///
/// # Errors
///
/// Returns an error when the data directory cannot be accessed, a frame cannot
/// be encoded or decoded, or a segmented-log operation fails.
pub async fn run() -> ToolResult<()> {
    match Cli::parse().command {
        Command::Write(args) => write(args).await,
        Command::Read(args) => read(args).await,
    }
}

async fn write(args: WriteArgs) -> ToolResult<()> {
    let config = SegmentedLogWriterConfig {
        segment_size: args.segment_size.get(),
        batch_size: args.batch_size.get(),
        sync_interval: Duration::from_millis(args.sync_interval_ms),
    };
    let storage = FilesystemSegmentStorage::new(&args.data_dir);
    let mut writer = SegmentedLogWriter::open_or_create(
        storage,
        args.start_record_id,
        args.max_frame_size.get(),
        config,
    )
    .await?;
    let first_record_id = writer.committed().next_record_id();
    write_status(format_args!(
        "appending to {}.log at byte {} with record ID {first_record_id}",
        writer.committed().segment_base_offset(),
        writer.committed().segment_byte_offset()
    ))?;

    let seed = args.seed.unwrap_or_else(|| rand::rng().random());
    let mut rng = SmallRng::seed_from_u64(seed);
    write_status(format_args!("random seed: {seed}"))?;

    for index in 0..args.records.get() {
        let record_id = first_record_id
            .checked_add(index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "record ID overflowed"))?;
        let payload = generate_json_event(record_id, args.message_bytes, &mut rng)?;
        let frame = encode_frame(
            record_id,
            NonZeroU32::MIN,
            args.codec_metadata,
            &payload,
            args.max_frame_size.get(),
        )?;
        writer.append(frame).await?;

        if (index + 1) % args.flush_every.get() == 0 {
            writer.flush().await?;
        }
    }

    let position = writer.sync_all().await?;
    write_status(format_args!(
        "wrote {} events to {} (seed {seed})",
        args.records,
        args.data_dir.display()
    ))?;
    write_status(format_args!(
        "next position: --segment-base-offset {} --segment-byte-offset {} --next-record-id {}",
        position.segment_base_offset(),
        position.segment_byte_offset(),
        position.next_record_id()
    ))?;
    Ok(())
}

async fn read(args: ReadArgs) -> ToolResult<()> {
    let initial = Position::new(
        args.segment_base_offset,
        args.segment_byte_offset,
        args.next_record_id,
    );
    let committed = SegmentedLogReader::recover_tail(&args.data_dir, args.max_frame_size.get())
        .await?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the data directory contains no disk-v3 segments",
            )
        })?
        .position();
    let mut reader =
        SegmentedLogReader::open(&args.data_dir, initial, args.max_frame_size.get()).await?;
    let mut frames_read = 0_u64;

    loop {
        if args.limit.is_some_and(|limit| frames_read >= limit.get()) {
            break;
        }

        match reader.read_next(committed).await? {
            SegmentedRead::Frame { frame, position } => {
                let event: Value = serde_json::from_slice(frame.payload())?;
                let output = if args.with_positions {
                    json!({
                        "record_id": frame.record_id(),
                        "event_count": frame.event_count().get(),
                        "codec_metadata": frame.codec_metadata(),
                        "frame": {
                            "segment_base_offset": position.segment_base_offset(),
                            "segment_byte_offset": frame.segment_byte_offset(),
                            "frame_len": frame.frame_len(),
                        },
                        "next_position": position_json(position),
                        "event": event,
                    })
                } else {
                    event
                };
                write_json(&output, args.pretty)?;
                frames_read += 1;
            }
            SegmentedRead::CaughtUp => break,
        }
    }

    let position = reader.read_position();
    write_status(format_args!(
        "read {frames_read} frames from {}",
        args.data_dir.display()
    ))?;
    write_status(format_args!(
        "next position: --segment-base-offset {} --segment-byte-offset {} --next-record-id {}",
        position.segment_base_offset(),
        position.segment_byte_offset(),
        position.next_record_id()
    ))?;
    Ok(())
}

fn generate_json_event(
    record_id: u64,
    message_bytes: usize,
    rng: &mut SmallRng,
) -> serde_json::Result<Vec<u8>> {
    const LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];
    let message = Alphanumeric.sample_string(rng, message_bytes);
    serde_json::to_vec(&json!({
        "id": record_id,
        "level": LEVELS[rng.random_range(0..LEVELS.len())],
        "message": message,
        "value": rng.random_range(0..1_000_000_u64),
        "active": rng.random_bool(0.9),
    }))
}

fn position_json(position: Position) -> Value {
    json!({
        "segment_base_offset": position.segment_base_offset(),
        "segment_byte_offset": position.segment_byte_offset(),
        "next_record_id": position.next_record_id(),
    })
}

fn write_json(value: &Value, pretty: bool) -> ToolResult<()> {
    let mut stdout = io::stdout().lock();
    if pretty {
        serde_json::to_writer_pretty(&mut stdout, value)?;
    } else {
        serde_json::to_writer(&mut stdout, value)?;
    }
    stdout.write_all(b"\n")?;
    Ok(())
}

fn write_status(arguments: fmt::Arguments<'_>) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    stderr.write_fmt(arguments)?;
    stderr.write_all(b"\n")
}
