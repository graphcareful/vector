//! Asserts two properties after faults stop:
//!
//! * **throughput** — the pipeline delivered at least one event during the run.
//! * **liveness** — a fresh event round-trips after all faults clear.
//!
//! No conservation assertion. Data loss under faults is acceptable. The check verifies
//! that Vector makes progress through its disk-buffered pipeline and does not crash
//! permanently — every node must recover and accept new work.

#![allow(clippy::disallowed_types)] // antithesis assert macros expand to once_cell::Lazy

#[cfg(target_os = "linux")]
extern crate antithesis_instrumentation;

use antithesis_harness::payload_field;
use antithesis_sdk::{
    antithesis_init, assert_always, assert_reachable, assert_sometimes_greater_than,
    assert_unreachable,
};
use clap::Parser;
use serde_json::{json, Value};
use tokio::time;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "VECTOR_SOURCE_URL", default_value = "http://head:8080/")]
    source_url: String,
    #[arg(long, env = "ORACLE_URL", default_value = "http://127.0.0.1:8686")]
    oracle_url: String,
    #[arg(
        long,
        env = "VECTOR_METRICS_URLS",
        value_delimiter = ',',
        default_value = "http://head:9598/metrics,http://tail:9598/metrics"
    )]
    metrics_urls: Vec<String>,
}

struct Report {
    issued: u64,
    delivered_total: u64,
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
        issued: v["issued"].as_u64()?,
        delivered_total: v["delivered_total"].as_u64()?,
    })
}

async fn delivered_contains(client: &reqwest::Client, oracle_url: &str, id: u64) -> bool {
    let Ok(resp) = client
        .get(format!("{oracle_url}/delivered?id={id}"))
        .timeout(time::Duration::from_secs(5))
        .send()
        .await
    else {
        return false;
    };
    resp.text().await.map(|s| s.trim() == "1").unwrap_or(false)
}

async fn node_healthy(client: &reqwest::Client, metrics_url: &str) -> bool {
    matches!(
        client
            .get(metrics_url)
            .timeout(time::Duration::from_secs(3))
            .send()
            .await,
        Ok(resp) if resp.status().is_success()
    )
}

async fn all_healthy(client: &reqwest::Client, metrics_urls: &[String]) -> bool {
    for u in metrics_urls {
        if !node_healthy(client, u).await {
            return false;
        }
    }
    true
}

async fn claim(client: &reqwest::Client, oracle_url: &str) -> Option<u64> {
    let resp = client
        .post(format!("{oracle_url}/claim"))
        .timeout(time::Duration::from_secs(10))
        .send()
        .await
        .ok()?;
    resp.text().await.ok()?.trim().parse().ok()
}

async fn post_probe(client: &reqwest::Client, source_url: &str, id: u64) -> bool {
    let event = json!([{ "id": id, "data": payload_field(id) }]);
    matches!(
        client
            .post(source_url)
            .timeout(time::Duration::from_secs(10))
            .json(&event)
            .send()
            .await,
        Ok(resp) if resp.status().is_success()
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();
    let args = Args::parse();
    let client = reqwest::Client::new();

    let source_url = args.source_url;
    let oracle_url = args.oracle_url;
    let metrics_urls = args.metrics_urls;

    // Faults stop instantly, but nodes need time to restart and bind their listeners.
    // Poll until every node's metrics endpoint answers.
    let recovery_deadline = time::Instant::now() + time::Duration::from_secs(180);
    while time::Instant::now() < recovery_deadline && !all_healthy(&client, &metrics_urls).await {
        time::sleep(time::Duration::from_secs(3)).await;
    }

    let Some(report) = fetch_report(&client, &oracle_url).await else {
        assert_unreachable!(
            "oracle unreachable while building progress report",
            &json!({ "oracle_url": oracle_url })
        );
        return;
    };

    // Over the course of the run the pipeline must have processed some events.
    assert_sometimes_greater_than!(
        report.issued,
        0,
        "at least one event was issued to the pipeline during the run",
        &json!({ "issued": report.issued })
    );
    assert_sometimes_greater_than!(
        report.delivered_total,
        0,
        "at least one event was delivered end-to-end during the run",
        &json!({ "delivered_total": report.delivered_total })
    );

    // Liveness: a fresh write must round-trip after faults clear. Faults are stopped so
    // a failure here is a real wedge, not a transient fault artifact. Retry the post until
    // one sticks (a just-restarted node can briefly refuse connections while its source
    // listener comes up), then poll delivery with the same 180-second budget as recovery.
    let deadline = time::Instant::now() + time::Duration::from_secs(180);
    let mut probe = None;
    let mut progressed = false;
    while !progressed && time::Instant::now() < deadline {
        if probe.is_none() {
            if let Some(id) = claim(&client, &oracle_url).await {
                if post_probe(&client, &source_url, id).await {
                    probe = Some(id);
                    assert_reachable!(
                        "liveness probe accepted by source after recovery",
                        &json!({ "id": id })
                    );
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
        "post-recovery write makes progress through the disk-buffered pipeline",
        &json!({
            "issued_before_check": report.issued,
            "delivered_total_before_check": report.delivered_total,
        })
    );
}
