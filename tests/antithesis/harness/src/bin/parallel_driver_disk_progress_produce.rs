//! Drive one event into the pipeline. No ack obligation — data loss is acceptable.
//! Claims an id from the oracle so delivered events can be counted, but does not
//! register an acked obligation: a 200 from the source means the event entered the
//! pipeline, not that it was delivered end-to-end.

#![allow(clippy::disallowed_types)] // antithesis assert macros expand to once_cell::Lazy

#[cfg(target_os = "linux")]
extern crate antithesis_instrumentation;

use antithesis_harness::payload_field;
use antithesis_sdk::{antithesis_init, assert_reachable};
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

async fn post_event(
    client: &reqwest::Client,
    source_url: &str,
    id: u64,
    timeout: time::Duration,
) -> bool {
    let event = json!([{ "id": id, "data": payload_field(id) }]);
    matches!(
        client.post(source_url).timeout(timeout).json(&event).send().await,
        Ok(resp) if resp.status().is_success()
    )
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();
    let args = Args::parse();
    let client = reqwest::Client::new();

    let Some(id) = claim(&client, &args.oracle_url).await else {
        return; // oracle unreachable; nothing to do this invocation
    };
    for _ in 0..MAX_ATTEMPTS {
        if post_event(&client, &args.source_url, id, time::Duration::from_secs(5)).await {
            // The source accepted the event. We do not register an acked obligation
            // (data loss is acceptable), but we mark the path reachable so Antithesis
            // confirms the produce → source handoff was exercised.
            assert_reachable!(
                "produce driver delivered event to source",
                &json!({ "id": id })
            );
            return;
        }
        time::sleep(time::Duration::from_millis(100)).await;
    }
    // Gave up after MAX_ATTEMPTS — acceptable; no obligation was recorded.
}
