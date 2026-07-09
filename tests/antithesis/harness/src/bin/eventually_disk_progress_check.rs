//! Asserts oversized-record handling properties:
//!
//! * **drop accounting** — oversized records are reported as discarded and do
//!   not retain buffer occupancy.
//! * **delivery** — every later small record arrives at the oracle.
//!
//! This is an `eventually_` command: it runs in the terminal window after Antithesis
//! has stopped all fault injection and killed the drivers, so the cluster is quiet
//! and fault-free while it runs. The scenario pairs it with a `parallel_driver_`
//! load generator — that both satisfies Antithesis's rule that a driver must start
//! before an `eventually_` command is scheduled, and exercises the disk buffer under
//! faults beforehand. Faults stop instantly but recovery is not, so the command
//! waits for node health before judging. An oversized record is expected to be
//! dropped, but it must not crash Vector or permanently block its disk buffer.

#![allow(clippy::disallowed_types)] // antithesis assert macros expand to once_cell::Lazy

#[cfg(target_os = "linux")]
extern crate antithesis_instrumentation;

use antithesis_harness::{
    all_healthy, claim_matching_payload, is_guaranteed_oversized_payload, is_small_payload,
    post_event,
};
use antithesis_sdk::{antithesis_init, assert_always, assert_reachable, assert_unreachable};
use clap::Parser;
use serde_json::{json, Value};
use tokio::time;

const OVERSIZED_RECORD_COUNT: usize = 4;
const NORMAL_RECORD_COUNT: usize = 4;

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
        default_value = "http://head:9598/metrics"
    )]
    metrics_urls: Vec<String>,
}

struct BufferMetrics {
    discarded_events: f64,
    occupancy_events: f64,
}

struct OracleReport {
    expected_delivered_count: u64,
    expected_delivered_missing_count: u64,
    expected_dropped_count: u64,
    expected_dropped_delivered_count: u64,
}

/// Sum a Prometheus sample across every matching line. Vector's
/// prometheus_exporter renders each series as `name{labels} value <timestamp>`,
/// so read the value as the second whitespace token (the trailing timestamp is
/// ignored) and match on the first token (name+labels). `# HELP`/`# TYPE` comment
/// lines fall out naturally: their first token is `#`, which matches no metric.
fn metric_sum(body: &str, metric_name: &str, required_label: Option<&str>) -> f64 {
    body.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let sample = fields.next()?;
            let value = fields.next()?;
            if sample.starts_with(metric_name)
                && required_label.is_none_or(|label| sample.contains(label))
            {
                value.parse::<f64>().ok()
            } else {
                None
            }
        })
        .sum()
}

async fn fetch_head_buffer_metrics(
    client: &reqwest::Client,
    metrics_url: &str,
) -> Option<BufferMetrics> {
    let body = client
        .get(metrics_url)
        .timeout(time::Duration::from_secs(5))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    Some(BufferMetrics {
        // The prometheus_exporter emits internal metrics under the `vector_`
        // namespace, so the exported names carry that prefix.
        discarded_events: metric_sum(
            &body,
            "vector_buffer_discarded_events_total",
            // Vector labels this counter `intentional="false"` for unprocessable
            // (oversized) drops; there is no `reason` label on the metric — the
            // reason string is only in the log line. With `when_full: block` there
            // are no intentional drops, so this isolates the oversized-drop path.
            Some("intentional=\"false\""),
        ),
        // Restrict to the disk buffer so the exporter's own memory input buffer
        // (`buffer_type="memory"`) does not add phantom occupancy.
        occupancy_events: metric_sum(
            &body,
            "vector_buffer_size_events",
            Some("buffer_type=\"disk\""),
        ),
    })
}

async fn fetch_oracle_report(client: &reqwest::Client, oracle_url: &str) -> Option<OracleReport> {
    let Ok(resp) = client
        .get(format!("{oracle_url}/report"))
        .timeout(time::Duration::from_secs(5))
        .send()
        .await
    else {
        return None;
    };
    let report: Value = resp.json().await.ok()?;
    Some(OracleReport {
        expected_delivered_count: report.get("expected_delivered_count")?.as_u64()?,
        expected_delivered_missing_count: report
            .get("expected_delivered_missing_count")?
            .as_u64()?,
        expected_dropped_count: report.get("expected_dropped_count")?.as_u64()?,
        expected_dropped_delivered_count: report
            .get("expected_dropped_delivered_count")?
            .as_u64()?,
    })
}

async fn register_expectation(
    client: &reqwest::Client,
    oracle_url: &str,
    outcome: &str,
    id: u64,
) -> bool {
    matches!(
        client
            .post(format!("{oracle_url}/expect/{outcome}"))
            .timeout(time::Duration::from_secs(10))
            .body(id.to_string())
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

    // Faults have stopped; wait for the node's metrics endpoint to answer before
    // judging the buffer. This scenario injects only network and clock faults — no
    // node kills — so the Vector process never restarts: once metrics answer the
    // source data path is up too, and buffer_discarded_events_total reflects only
    // this check's own oversized drops (no crash-recovery torn-file skips to
    // conflate). Best-effort — on timeout, proceed and let the submission asserts
    // below surface a genuinely wedged pipeline.
    let ready_deadline = time::Instant::now() + time::Duration::from_secs(60);
    while time::Instant::now() < ready_deadline && !all_healthy(&client, &metrics_urls).await {
        time::sleep(time::Duration::from_secs(3)).await;
    }

    let Some(head_metrics_url) = metrics_urls.first() else {
        assert_unreachable!("missing head metrics URL", &json!({}));
        return;
    };

    let mut oversized_ids = Vec::with_capacity(OVERSIZED_RECORD_COUNT);
    for _ in 0..OVERSIZED_RECORD_COUNT {
        let Some(id) =
            claim_matching_payload(&client, &oracle_url, is_guaranteed_oversized_payload).await
        else {
            assert_unreachable!("could not claim an oversized payload id", &json!({}));
            return;
        };
        let registered = register_expectation(&client, &oracle_url, "drop", id).await;
        assert_always!(
            registered,
            "oracle registers an oversized record as expected to be dropped",
            &json!({ "id": id })
        );
        if !registered {
            return;
        }
        let accepted = post_event(&client, &source_url, id, time::Duration::from_secs(10)).await;
        assert_always!(
            accepted,
            "source accepts an oversized record without crashing the pipeline",
            &json!({ "id": id })
        );
        if !accepted {
            return;
        }
        oversized_ids.push(id);
    }
    assert_reachable!(
        "oversized records were submitted during the fault-free check",
        &json!({ "ids": oversized_ids })
    );

    // Send normal records only after the oversized writes so their delivery also
    // proves that drops do not prevent later writes from making progress.
    let mut normal_ids = Vec::with_capacity(NORMAL_RECORD_COUNT);
    for _ in 0..NORMAL_RECORD_COUNT {
        let Some(id) = claim_matching_payload(&client, &oracle_url, is_small_payload).await else {
            assert_unreachable!("could not claim a small payload id", &json!({}));
            return;
        };
        let registered = register_expectation(&client, &oracle_url, "delivery", id).await;
        assert_always!(
            registered,
            "oracle registers a normal record as expected to be delivered",
            &json!({ "id": id })
        );
        if !registered {
            return;
        }
        let accepted = post_event(&client, &source_url, id, time::Duration::from_secs(10)).await;
        assert_always!(
            accepted,
            "source accepts a normal record after oversized drops",
            &json!({ "id": id })
        );
        if !accepted {
            return;
        }
        normal_ids.push(id);
    }

    // The constrained head buffer must report exactly the submitted oversized
    // records as unprocessable, drain to zero occupancy, and deliver every
    // normal record to the oracle.
    let deadline = time::Instant::now() + time::Duration::from_secs(120);
    let mut buffer_metrics = None;
    let mut oracle_report = None;
    while time::Instant::now() < deadline {
        buffer_metrics = fetch_head_buffer_metrics(&client, head_metrics_url).await;
        oracle_report = fetch_oracle_report(&client, &oracle_url).await;
        if let (Some(metrics), Some(report)) = (buffer_metrics.as_ref(), oracle_report.as_ref()) {
            if metrics.discarded_events == OVERSIZED_RECORD_COUNT as f64
                && metrics.occupancy_events == 0.0
                && report.expected_delivered_count == NORMAL_RECORD_COUNT as u64
                && report.expected_delivered_missing_count == 0
                && report.expected_dropped_count == OVERSIZED_RECORD_COUNT as u64
                && report.expected_dropped_delivered_count == 0
            {
                break;
            }
        }
        time::sleep(time::Duration::from_secs(2)).await;
    }
    let discarded_events = buffer_metrics
        .as_ref()
        .map_or(0.0, |metrics| metrics.discarded_events);
    let occupancy_events = buffer_metrics
        .as_ref()
        .map_or(-1.0, |metrics| metrics.occupancy_events);
    let expected_delivered_count = oracle_report
        .as_ref()
        .map_or(0, |report| report.expected_delivered_count);
    let expected_delivered_missing_count = oracle_report
        .as_ref()
        .map_or(u64::MAX, |report| report.expected_delivered_missing_count);
    let expected_dropped_count = oracle_report
        .as_ref()
        .map_or(0, |report| report.expected_dropped_count);
    let expected_dropped_delivered_count = oracle_report
        .as_ref()
        .map_or(u64::MAX, |report| report.expected_dropped_delivered_count);
    assert_always!(
        discarded_events == OVERSIZED_RECORD_COUNT as f64,
        "all oversized records are reported as unprocessable by the disk buffer",
        &json!({ "discarded_events": discarded_events, "oversized_ids": oversized_ids })
    );
    assert_always!(
        occupancy_events == 0.0,
        "oversized record drops do not retain disk buffer event occupancy",
        &json!({ "occupancy_events": occupancy_events })
    );

    assert_always!(
        expected_delivered_count == NORMAL_RECORD_COUNT as u64,
        "oracle tracked every normal record submitted after oversized drops",
        &json!({
            "expected_delivered_count": expected_delivered_count,
            "normal_ids": normal_ids,
        })
    );
    assert_always!(
        expected_delivered_missing_count == 0,
        "every normal record submitted after oversized drops arrives at the oracle",
        &json!({ "expected_delivered_missing_count": expected_delivered_missing_count })
    );
    assert_always!(
        expected_dropped_count == OVERSIZED_RECORD_COUNT as u64,
        "oracle tracked every oversized record as expected to be dropped",
        &json!({ "expected_dropped_count": expected_dropped_count })
    );
    assert_always!(
        expected_dropped_delivered_count == 0,
        "no oversized record expected to be dropped arrives at the oracle",
        &json!({
            "expected_dropped_delivered_count": expected_dropped_delivered_count,
            "oversized_ids": oversized_ids,
        })
    );
}
