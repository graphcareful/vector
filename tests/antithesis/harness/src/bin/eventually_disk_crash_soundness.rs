//! Asserts disk_v2 soundness properties that must hold no matter what the crash
//! schedule did, deliberately NOT conservation:
//!
//! * **integrity** — every record the oracle receives matches what was issued for
//!   its id (checked online by the oracle on every `/ingest` call; restated here
//!   as a final quiescent measurement).
//! * **accounting soundness** — the disk buffer's own internal invariants (record
//!   count and byte size never underflow) are asserted in-process by the SUT
//!   itself, gated on `antithesis-disk-asserts`. This binary only has to prove the
//!   SUT is alive long enough to have reported them, not re-derive them
//!   externally.
//! * **liveness** — once faults stop, the buffer recovers and a fresh write still
//!   makes it through.
//!
//! What this does **not** check: whether an event the pipeline acked survives a
//! crash. The node runs with acknowledgements disabled (see `vector.yaml`), so
//! there is no delivery guarantee to judge here — data loss across a fault is
//! expected and unbounded. The separate question of whether an *end-to-end-acked*
//! disk-buffered event survives a crash is `vector_to_vector_e2e_disk`'s
//! conservation property, not this one.

#![allow(clippy::disallowed_types)] // antithesis assert macros expand to once_cell::Lazy

#[cfg(target_os = "linux")]
extern crate antithesis_instrumentation;

use antithesis_harness::{all_healthy, claim, delivered_contains, post_event};
use antithesis_sdk::{
    antithesis_init, assert_always, assert_always_less_than_or_equal_to, assert_unreachable,
};
use clap::Parser;
use serde_json::{json, Value};
use tokio::time;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "VECTOR_SOURCE_URL", default_value = "http://vector:8080/")]
    source_url: String,
    #[arg(long, env = "ORACLE_URL", default_value = "http://127.0.0.1:8686")]
    oracle_url: String,
    #[arg(
        long,
        env = "VECTOR_METRICS_URLS",
        value_delimiter = ',',
        default_value = "http://vector:9598/metrics"
    )]
    metrics_urls: Vec<String>,
}

/// Only the fields this check judges. `acked`/`missing` are deliberately absent:
/// this scenario runs with acks disabled, so the oracle's acked set carries no
/// durability obligation and reading it here would silently reintroduce a
/// conservation assertion this scenario does not make.
struct Report {
    delivered: u64,
    delivered_total: u64,
    spurious_count: u64,
    corrupted_count: u64,
}

async fn fetch_report(client: &reqwest::Client, oracle_url: &str) -> Option<Report> {
    let body = client
        .get(format!("{oracle_url}/report"))
        .timeout(time::Duration::from_secs(5))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let v: Value = serde_json::from_str(&body).ok()?;
    Some(Report {
        delivered: v["delivered"].as_u64()?,
        delivered_total: v["delivered_total"].as_u64()?,
        spurious_count: v["spurious_count"].as_u64()?,
        corrupted_count: v["corrupted_count"].as_u64()?,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();
    let args = Args::parse();
    let client = reqwest::Client::new();

    let source_url = args.source_url;
    let oracle_url = args.oracle_url;
    let metrics_urls = args.metrics_urls;

    // This is an Antithesis `eventually_` command: Antithesis stops all fault
    // injection and kills the drivers before scheduling it, so everything below
    // runs against a load-free, fault-free (but freshly recovered) cluster.

    // Faults stop instantly but recovery is not; wait for the node to serve again.
    let recovery_deadline = time::Instant::now() + time::Duration::from_secs(180);
    while time::Instant::now() < recovery_deadline && !all_healthy(&client, &metrics_urls).await {
        time::sleep(time::Duration::from_secs(3)).await;
    }

    // Drain: give the buffer a chance to deliver whatever it still can. There is
    // no target count to wait for — conservation is not judged here — so this
    // just waits for delivery to plateau (or a cap) before reading a final
    // snapshot, the same way a healthy-but-quiet buffer would settle on its own.
    let drain_deadline = time::Instant::now() + time::Duration::from_secs(60);
    let mut last_delivered_total = u64::MAX;
    let mut plateau = 0u32;
    while time::Instant::now() < drain_deadline {
        time::sleep(time::Duration::from_secs(3)).await;
        let Some(r) = fetch_report(&client, &oracle_url).await else {
            continue;
        };
        if r.delivered_total == last_delivered_total {
            plateau += 1;
            if plateau >= 5 {
                break;
            }
        } else {
            plateau = 0;
        }
        last_delivered_total = r.delivered_total;
    }

    let Some(report) = fetch_report(&client, &oracle_url).await else {
        // On a healthy run the oracle is up. Reaching this arm is itself the failure.
        assert_unreachable!(
            "oracle unreachable while building the soundness report",
            &json!({ "oracle_url": oracle_url })
        );
        return;
    };

    // Integrity holds no matter how much was lost: whatever the oracle did
    // receive must be a real, unmangled record. This restates the oracle's own
    // online per-record asserts (from `/ingest`) as one final quiescent
    // measurement, so a run that only ever violated integrity mid-fault (and
    // never triggered the online assert for some reason) still fails here too.
    assert_always_less_than_or_equal_to!(
        report.spurious_count,
        0,
        "every delivered id was actually issued (no invented or corrupted ids)",
        &json!({ "spurious_count": report.spurious_count, "delivered": report.delivered })
    );
    assert_always_less_than_or_equal_to!(
        report.corrupted_count,
        0,
        "every delivered record's payload matches what was issued for its id",
        &json!({ "corrupted_count": report.corrupted_count, "delivered": report.delivered })
    );

    // Liveness: a fresh write still round-trips once the node has recovered.
    // Claim and post retry until one sticks, since a node can briefly refuse
    // writes while still recovering; a permanently wedged buffer never delivers
    // it and fails here.
    let deadline = time::Instant::now() + time::Duration::from_secs(180);
    let mut probe = None;
    let mut progressed = false;
    while !progressed && time::Instant::now() < deadline {
        if probe.is_none() {
            if let Some(id) = claim(&client, &oracle_url).await {
                if post_event(&client, &source_url, id, time::Duration::from_secs(10)).await {
                    probe = Some(id);
                }
            }
        }
        if let Some(id) = probe {
            progressed = delivered_contains(&client, &oracle_url, id).await;
        }
        if !progressed {
            time::sleep(time::Duration::from_secs(2)).await;
        }
    }
    assert_always!(
        progressed,
        "post-recovery write makes progress",
        &json!({ "delivered": report.delivered, "delivered_total": report.delivered_total })
    );
}
