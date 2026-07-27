//! Common code shared across Antithesis scenarios. Each scenario crate (e.g.
//! `scenarios/vector_to_vector_e2e_disk`) owns its own test-command bins. When two
//! scenarios need the same HTTP or oracle helpers, factor them into modules here.

use vector_buffers::WRITE_BUFFER_SIZE_V2;

/// Payload lengths in bytes, one per id class. Sized around the disk_v2 write
/// buffer so the produced records straddle the boundary at which the buffer is
/// flushed to the data file: empty, a single byte, fractions of, just under, at,
/// just over, and a record several times larger than the buffer.
const PAYLOAD_LENGTHS: [usize; 8] = [
    0,
    1,
    WRITE_BUFFER_SIZE_V2 / 4,
    WRITE_BUFFER_SIZE_V2 / 2,
    WRITE_BUFFER_SIZE_V2 - 1,
    WRITE_BUFFER_SIZE_V2,
    WRITE_BUFFER_SIZE_V2 + 1,
    768 * 1024,
];

fn payload_length(id: u64) -> usize {
    PAYLOAD_LENGTHS[(id % PAYLOAD_LENGTHS.len() as u64) as usize]
}

/// True for the payload class that always exceeds the 64KiB record cap used by
/// the oversized-record scenario, even before record framing is added.
pub fn is_guaranteed_oversized_payload(id: u64) -> bool {
    payload_length(id) == 768 * 1024
}

/// True for the small probe class used to prove later writes still make progress.
pub fn is_small_payload(id: u64) -> bool {
    payload_length(id) == 1
}

/// One splitmix64 step. A full-avalanche mixer, so flipping any input bit
/// scrambles the whole output. Seeding the stream with this keyed by id means a
/// length-preserving corruption still changes the bytes the oracle expects.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The exact payload bytes issued for `id`. Deterministic in `id` alone, so the
/// producer regenerates the same record on every retry and the oracle regenerates
/// the same expected bytes with no per-id state to carry. Length comes from the
/// id's class; content is a splitmix64 stream seeded by id.
pub fn payload_for(id: u64) -> Vec<u8> {
    let len = payload_length(id);
    let mut out = Vec::with_capacity(len);
    let mut state = id;
    while out.len() < len {
        let chunk = splitmix64(&mut state).to_le_bytes();
        let take = (len - out.len()).min(chunk.len());
        out.extend_from_slice(&chunk[..take]);
    }
    out
}

/// Hex-encoding of `payload_for(id)`. Hex survives JSON and Vector transport
/// without escaping concerns, and a corruption of the bytes shows up as a hex
/// mismatch.
pub fn payload_field(id: u64) -> String {
    let bytes = payload_for(id);
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Decode the hex produced by [`payload_field`] back to bytes. Returns `None` on
/// any non-hex or odd-length input so the oracle can tell a mangled field from a
/// content mismatch.
pub fn decode_payload_field(field: &str) -> Option<Vec<u8>> {
    if !field.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(field.len() / 2);
    let mut bytes = field.bytes();
    while let (Some(hi), Some(lo)) = (bytes.next(), bytes.next()) {
        let hi = (hi as char).to_digit(16)?;
        let lo = (lo as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Shared oracle / pipeline HTTP helpers.
//
// The oracle protocol (/claim, /acked, /delivered) and the source data path are
// identical across scenarios, so the drivers and eventually-phase checks share
// these instead of each carrying a private copy.
// ---------------------------------------------------------------------------

use std::time::Duration;

use serde_json::json;

/// Claim one fresh id from the oracle. `None` if the oracle is unreachable.
pub async fn claim(client: &reqwest::Client, oracle_url: &str) -> Option<u64> {
    let resp = client
        .post(format!("{oracle_url}/claim"))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .ok()?;
    resp.text().await.ok()?.trim().parse().ok()
}

/// Claim ids until one satisfies `matches`, up to 8 attempts. A claimed id that
/// does not match is simply not used — it is issued-but-never-acked, so it is no
/// obligation and no false loss.
pub async fn claim_matching_payload(
    client: &reqwest::Client,
    oracle_url: &str,
    matches: fn(u64) -> bool,
) -> Option<u64> {
    for _ in 0..8 {
        let id = claim(client, oracle_url).await?;
        if matches(id) {
            return Some(id);
        }
    }
    None
}

/// Tell the oracle the pipeline acked `id`, so it must come back. Returns whether
/// the oracle recorded the obligation.
pub async fn report_acked(client: &reqwest::Client, oracle_url: &str, id: u64) -> bool {
    matches!(
        client
            .post(format!("{oracle_url}/acked"))
            .timeout(Duration::from_secs(10))
            .body(id.to_string())
            .send()
            .await,
        Ok(resp) if resp.status().is_success()
    )
}

/// POST one event (its id and the deterministic payload for that id) to the
/// source. `true` on a 2xx, meaning the pipeline took responsibility for the
/// event. The payload is a pure function of the id, so every retry re-sends the
/// exact same bytes and the oracle can recompute what to expect.
pub async fn post_event(
    client: &reqwest::Client,
    source_url: &str,
    id: u64,
    timeout: Duration,
) -> bool {
    let event = json!([{ "id": id, "data": payload_field(id) }]);
    matches!(
        client.post(source_url).timeout(timeout).json(&event).send().await,
        Ok(resp) if resp.status().is_success()
    )
}

/// True once the oracle has recorded delivery of the round-tripped record `id`.
pub async fn delivered_contains(client: &reqwest::Client, oracle_url: &str, id: u64) -> bool {
    let Ok(resp) = client
        .get(format!("{oracle_url}/delivered?id={id}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    else {
        return false;
    };
    resp.text().await.map(|s| s.trim() == "1").unwrap_or(false)
}

/// Whether a single node's metrics endpoint answers 2xx.
pub async fn node_healthy(client: &reqwest::Client, metrics_url: &str) -> bool {
    matches!(
        client.get(metrics_url).timeout(Duration::from_secs(3)).send().await,
        Ok(resp) if resp.status().is_success()
    )
}

/// Whether every node's metrics endpoint answers 2xx.
pub async fn all_healthy(client: &reqwest::Client, metrics_urls: &[String]) -> bool {
    for u in metrics_urls {
        if !node_healthy(client, u).await {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_deterministic_in_id() {
        for id in 0..32u64 {
            assert_eq!(payload_for(id), payload_for(id));
        }
    }

    #[test]
    fn payload_length_follows_class() {
        for id in 0..PAYLOAD_LENGTHS.len() as u64 {
            assert_eq!(payload_for(id).len(), PAYLOAD_LENGTHS[id as usize]);
        }
    }

    #[test]
    fn distinct_ids_differ_in_content_at_equal_length() {
        // Ids in the same nonzero-length class but different ids must not produce
        // the same bytes, or a swapped-id corruption would slip past the oracle.
        // Ids 2 and 10 share class 2 (a buffer-quarter of bytes).
        let a = payload_for(2);
        let b = payload_for(10);
        assert_eq!(a.len(), b.len());
        assert!(!a.is_empty());
        assert_ne!(a, b);
    }

    #[test]
    fn hex_round_trips() {
        for id in 0..32u64 {
            let field = payload_field(id);
            assert_eq!(decode_payload_field(&field).unwrap(), payload_for(id));
        }
    }
}
