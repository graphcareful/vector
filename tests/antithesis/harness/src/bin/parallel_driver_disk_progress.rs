//! Load generator for the disk-progress scenario: drive one small, well-formed
//! record into the pipeline under fault injection.
//!
//! Its job is twofold. First, Antithesis only schedules an `eventually_` command
//! after a driver has started, so this driver is what lets
//! `eventually_disk_progress_check` run at all. Second, it keeps normal traffic
//! flowing through the disk buffer while faults (partitions, node crash/hang,
//! clock jitter) are active, so the buffer's fill/retry/recover paths are
//! exercised before the terminal check judges oversized-record handling.
//!
//! It deliberately sends only the *small* payload class. The check asserts that
//! exactly `OVERSIZED_RECORD_COUNT` records are reported as unprocessable, read off
//! the `buffer_discarded_events_total` counter; if this driver ever sent an
//! oversized record it would be dropped as unprocessable too and inflate that
//! count. Small records always fit, so they never touch the discard path — the only
//! oversized records in the whole run are the ones the check submits itself.

#![allow(clippy::disallowed_types)] // antithesis assert macros expand to once_cell::Lazy

#[cfg(target_os = "linux")]
extern crate antithesis_instrumentation;

use antithesis_harness::{claim_matching_payload, is_small_payload, post_event, report_acked};
use antithesis_sdk::{antithesis_init, assert_reachable, assert_unreachable};
use clap::Parser;
use serde_json::json;
use tokio::time;

const MAX_ATTEMPTS: u32 = 5;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "VECTOR_SOURCE_URL", default_value = "http://head:8080/")]
    source_url: String,
    #[arg(long, env = "ORACLE_URL", default_value = "http://127.0.0.1:8686")]
    oracle_url: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();
    let args = Args::parse();
    let client = reqwest::Client::new();

    // Claim only a small-class id so this driver never emits an oversized record
    // (see the module comment). A non-matching claim is simply dropped.
    let Some(id) = claim_matching_payload(&client, &args.oracle_url, is_small_payload).await else {
        return; // oracle unreachable or no small id this invocation; nothing to do
    };
    for _ in 0..MAX_ATTEMPTS {
        // Tight timeout. A wedged source blocks forever, so we stop waiting and
        // retry the same id.
        if post_event(&client, &args.source_url, id, time::Duration::from_secs(5)).await {
            // The pipeline took responsibility, so the oracle must record the
            // obligation or a later loss of this id goes uncounted. /acked is a
            // loopback call to the oracle, which is never killed, frozen, or
            // network-faulted, so a failure here is anomalous: fail loudly rather
            // than leave an acked id the oracle never expects.
            if report_acked(&client, &args.oracle_url, id).await {
                assert_reachable!("disk-progress driver got an ack", &json!({ "id": id }));
            } else {
                assert_unreachable!(
                    "the pipeline acked an id but the oracle did not record the obligation",
                    &json!({ "id": id })
                );
            }
            return;
        }
        time::sleep(time::Duration::from_millis(100)).await;
    }
    // Gave up before any 2xx: id is claimed but never acked, so it is no obligation.
}
