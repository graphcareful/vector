use std::io::Read;

use base64::prelude::{Engine as _, BASE64_STANDARD};
use bytes::Bytes;
use chrono::Utc;
use flate2::read::MultiGzDecoder;
use futures::StreamExt;
use itertools::Itertools;
use smallvec::{smallvec, SmallVec};
use snafu::{ResultExt, Snafu};
use tokio_util::codec::FramedRead;
use vector_common::constants::GZIP_MAGIC;
use vector_lib::codecs::StreamDecodingError;
use vector_lib::lookup::{metadata_path, path, PathPrefix};
use vector_lib::{
    config::{LegacyKey, LogNamespace},
    event::BatchNotifier,
    EstimatedJsonEncodedSizeOf,
};
use vector_lib::{
    finalization::AddBatchNotifier,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Registered,
    },
};
use vrl::compiler::SecretTarget;
use warp::reject;

use super::{
    errors::{ParseLogEventsSnafu, ParseRecordsSnafu, RequestError},
    models::{EncodedFirehoseRecord, FirehoseRequest, FirehoseResponse},
    Compression,
};
use crate::{
    codecs::Decoder,
    config::log_schema,
    event::{BatchStatus, Event},
    internal_events::{
        AwsKinesisFirehoseAutomaticRecordDecodeError, EventsReceived, StreamClosedError,
    },
    sources::aws_kinesis_firehose::AwsKinesisFirehoseConfig,
    SourceSender,
};

#[derive(Clone)]
pub(super) struct Context {
    pub(super) compression: Compression,
    pub(super) store_access_key: bool,
    pub(super) decoder: Decoder,
    pub(super) acknowledgements: bool,
    pub(super) bytes_received: Registered<BytesReceived>,
    pub(super) out: SourceSender,
    pub(super) log_namespace: LogNamespace,
    pub(super) expand_cloudwatch_event_batch: bool,
}

/// Publishes decoded events from the FirehoseRequest to the pipeline
pub(super) async fn firehose(
    request_id: String,
    source_arn: String,
    request: FirehoseRequest,
    mut context: Context,
) -> Result<impl warp::Reply, reject::Rejection> {
    let log_namespace = context.log_namespace;
    let events_received = register!(EventsReceived);

    for record in request.records {
        let bytes = decode_record(&record, context.compression)
            .with_context(|_| ParseRecordsSnafu {
                request_id: request_id.clone(),
            })
            .map_err(reject::custom)?;
        context.bytes_received.emit(ByteSize(bytes.len()));

        let mut stream = FramedRead::new(bytes.as_ref(), context.decoder.clone());
        loop {
            match stream.next().await {
                Some(Ok((mut events, _byte_size))) => {
                    events_received.emit(CountByteSize(
                        events.len(),
                        events.estimated_json_encoded_size_of(),
                    ));

                    if context.expand_cloudwatch_event_batch {
                        events = expand_events(events, context.log_namespace)
                            .with_context(|_| ParseLogEventsSnafu {
                                request_id: request_id.clone(),
                            })
                            .map_err(reject::custom)?;
                    }

                    let (batch, receiver) = context
                        .acknowledgements
                        .then(|| {
                            let (batch, receiver) = BatchNotifier::new_with_receiver();
                            (Some(batch), Some(receiver))
                        })
                        .unwrap_or((None, None));

                    let now = Utc::now();
                    for event in &mut events {
                        if let Some(batch) = &batch {
                            event.add_batch_notifier(batch.clone());
                        }
                        if let Event::Log(ref mut log) = event {
                            log_namespace.insert_vector_metadata(
                                log,
                                log_schema().source_type_key(),
                                path!("source_type"),
                                Bytes::from_static(AwsKinesisFirehoseConfig::NAME.as_bytes()),
                            );
                            // This handles the transition from the original timestamp logic. Originally the
                            // `timestamp_key` was always populated by the `request.timestamp` time.
                            match log_namespace {
                                LogNamespace::Vector => {
                                    log.insert(metadata_path!("vector", "ingest_timestamp"), now);
                                    log.insert(
                                        metadata_path!(AwsKinesisFirehoseConfig::NAME, "timestamp"),
                                        request.timestamp,
                                    );
                                }
                                LogNamespace::Legacy => {
                                    if let Some(timestamp_key) = log_schema().timestamp_key() {
                                        log.try_insert(
                                            (PathPrefix::Event, timestamp_key),
                                            request.timestamp,
                                        );
                                    }
                                }
                            };

                            log_namespace.insert_source_metadata(
                                AwsKinesisFirehoseConfig::NAME,
                                log,
                                Some(LegacyKey::InsertIfEmpty(path!("request_id"))),
                                path!("request_id"),
                                request_id.to_owned(),
                            );
                            log_namespace.insert_source_metadata(
                                AwsKinesisFirehoseConfig::NAME,
                                log,
                                Some(LegacyKey::InsertIfEmpty(path!("source_arn"))),
                                path!("source_arn"),
                                source_arn.to_owned(),
                            );

                            if context.store_access_key {
                                if let Some(access_key) = &request.access_key {
                                    log.metadata_mut().secrets_mut().insert_secret(
                                        "aws_kinesis_firehose_access_key",
                                        access_key,
                                    );
                                }
                            }
                        }
                    }

                    let count = events.len();
                    if let Err(error) = context.out.send_batch(events).await {
                        emit!(StreamClosedError { count });
                        let error = RequestError::ShuttingDown {
                            request_id: request_id.clone(),
                            source: error,
                        };
                        warp::reject::custom(error);
                    }

                    drop(batch);
                    if let Some(receiver) = receiver {
                        match receiver.await {
                            BatchStatus::Delivered => Ok(()),
                            BatchStatus::Rejected => {
                                Err(warp::reject::custom(RequestError::DeliveryFailed {
                                    request_id: request_id.clone(),
                                }))
                            }
                            BatchStatus::Errored => {
                                Err(warp::reject::custom(RequestError::DeliveryErrored {
                                    request_id: request_id.clone(),
                                }))
                            }
                        }?;
                    }
                }
                Some(Err(error)) => {
                    // Error is logged by `crate::codecs::Decoder`, no further
                    // handling is needed here.
                    if !error.can_continue() {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    Ok(warp::reply::json(&FirehoseResponse {
        request_id: request_id.clone(),
        timestamp: Utc::now(),
        error_message: None,
    }))
}

#[derive(Debug, Snafu)]
pub enum ExpansionError {
    #[snafu(display("Event is not a log"))]
    NotLog,
    #[snafu(display("Missing field"))]
    MissingField,
    #[snafu(display("Payload is malformed JSON"))]
    BadPayload,
    #[snafu(display("Firehose element is not an object"))]
    NotObject,
    #[snafu(display("Firehose logEvent entry is not an object"))]
    LogEventItemNotObject,
}

/// Returns a list of individual events expanded from a list of batches of events
fn expand_events(
    events: SmallVec<[Event; 1]>,
    log_namespace: LogNamespace,
) -> Result<SmallVec<[Event; 1]>, ExpansionError> {
    events
        .into_iter()
        .map(|e| expand_message(e, log_namespace))
        .flatten_ok()
        .collect::<Result<SmallVec<[Event; 1]>, ExpansionError>>()
}

/// Returns a list of events per one 'message', under the key 'logEvents'
fn expand_message(
    event: Event,
    log_namespace: LogNamespace,
) -> Result<SmallVec<[Event; 1]>, ExpansionError> {
    let log_event: serde_json::Value = event.try_into().map_err(|_| ExpansionError::NotLog)?;
    let serde_json::Value::Object(mut obj_map) = log_event else {
        return Err(ExpansionError::NotObject);
    };
    let Some(message) = obj_map.remove("message") else {
        return Err(ExpansionError::MissingField);
    };
    let as_json: serde_json::Value =
        serde_json::from_str(message.as_str().unwrap()).map_err(|_| ExpansionError::BadPayload)?;
    // Return unmodified original event if shape does not match, i.e. it should be an object
    // with a key named 'logEvents'
    let serde_json::Value::Object(mut root) = as_json else {
        return Ok(smallvec![
            Event::from_json_value(as_json, log_namespace).unwrap()
        ]);
    };
    let Some(serde_json::Value::Array(log_events)) = root.remove("logEvents") else {
        return Ok(smallvec![Event::from_json_value(
            serde_json::Value::Object(root),
            log_namespace
        )
        .unwrap()]);
    };
    log_events
        .into_iter()
        .map(|inner| {
            if let serde_json::Value::Object(inner_obj) = inner {
                let mut new_event = obj_map.clone();
                let mut new_msg = root.clone();
                new_msg.extend(inner_obj);
                new_event.insert("message".to_string(), serde_json::Value::Object(new_msg));
                Event::from_json_value(serde_json::Value::Object(new_event), log_namespace)
                    .map_err(|_| ExpansionError::BadPayload)
            } else {
                Err(ExpansionError::LogEventItemNotObject)
            }
        })
        .collect::<Result<SmallVec<[Event; 1]>, ExpansionError>>()
}

#[derive(Debug, Snafu)]
pub enum RecordDecodeError {
    #[snafu(display("Could not base64 decode request data: {}", source))]
    Base64 { source: base64::DecodeError },
    #[snafu(display("Could not decompress request data as {}: {}", compression, source))]
    Decompression {
        source: std::io::Error,
        compression: Compression,
    },
}

/// Decodes a Firehose record.
fn decode_record(
    record: &EncodedFirehoseRecord,
    compression: Compression,
) -> Result<Bytes, RecordDecodeError> {
    let buf = BASE64_STANDARD
        .decode(record.data.as_bytes())
        .context(Base64Snafu {})?;

    if buf.is_empty() {
        return Ok(Bytes::default());
    }

    match compression {
        Compression::None => Ok(Bytes::from(buf)),
        Compression::Gzip => decode_gzip(&buf[..]).with_context(|_| DecompressionSnafu {
            compression: compression.to_owned(),
        }),
        Compression::Auto => {
            if is_gzip(&buf) {
                decode_gzip(&buf[..]).or_else(|error| {
                    emit!(AwsKinesisFirehoseAutomaticRecordDecodeError {
                        compression: Compression::Gzip,
                        error
                    });
                    Ok(Bytes::from(buf))
                })
            } else {
                // only support gzip for now
                Ok(Bytes::from(buf))
            }
        }
    }
}

fn is_gzip(data: &[u8]) -> bool {
    // The header length of a GZIP file is 10 bytes. The first two bytes of the constant comes from
    // the GZIP file format specification, which is the fixed member header identification bytes.
    // The third byte is the compression method, of which only one is defined which is 8 for the
    // deflate algorithm.
    //
    // Reference: https://datatracker.ietf.org/doc/html/rfc1952 Section 2.3
    data.starts_with(GZIP_MAGIC)
}

fn decode_gzip(data: &[u8]) -> std::io::Result<Bytes> {
    let mut decoded = Vec::new();

    let mut gz = MultiGzDecoder::new(data);
    gz.read_to_end(&mut decoded)?;

    Ok(Bytes::from(decoded))
}

#[cfg(test)]
mod tests {
    use flate2::{write::GzEncoder, Compression};
    use serde_json::{json, Value};
    use std::io::Write as _;
    use vector_lib::{config::LogNamespace, event::Event};

    use super::*;

    const CONTENT: &[u8] = b"Example";

    // Inner contents of cloudwatch events are serialized as strings of JSON content
    fn build_sample_firehose_event(num_events: usize) -> Value {
        let log_events = (0..num_events)
            .map(|i| {
                json!({
                    "id": &format!("id_value-{}", i),
                    "timestamp": "123456789",
                    "message": "message_value",
                })
            })
            .collect::<Vec<Value>>();
        let inner = json!({
            "messageType": "message_type_value",
            "owner": "owner_value",
            "logGroup": "log_group_value",
            "logStream": "log_stream_value",
            "subscriptionFilters": "subscription_filters_value",
            "logEvents": log_events,
        });
        json!({
            "message": serde_json::to_string(&inner).unwrap(),
            "request_id": "test-request-id",
            "source_arn": "arn:aws:firehost:us-east-1:1234678",
            "source_type": "aws_kinesis_firehose",
        })
    }

    #[test]
    fn correctly_detects_gzipped_content() {
        assert!(!is_gzip(CONTENT));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(CONTENT).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(is_gzip(&compressed));
    }

    fn expected_expanded_cw_event(sample_seq_id: usize) -> Value {
        json!({
            "message": {
                "id": &format!("id_value-{}", sample_seq_id),
                "logGroup": "log_group_value",
                "logStream": "log_stream_value",
                "message": "message_value",
                "messageType": "message_type_value",
                "owner": "owner_value",
                "subscriptionFilters": "subscription_filters_value",
                "timestamp": "123456789"
            },
            "request_id":"test-request-id",
            "source_arn": "arn:aws:firehost:us-east-1:1234678",
            "source_type": "aws_kinesis_firehose",
        })
    }

    #[test]
    fn expands_valid_cw_event() {
        let log_namespace = LogNamespace::default();
        let event = build_sample_firehose_event(10);
        let result = expand_message(
            Event::from_json_value(event, log_namespace).unwrap(),
            log_namespace,
        )
        .unwrap();
        assert_eq!(result.len(), 10);
        for (i, element) in result.into_iter().enumerate() {
            let observed: serde_json::Value = element.try_into().unwrap();
            assert_eq!(expected_expanded_cw_event(i), observed);
        }
    }

    #[test]
    fn expands_valid_cw_events() {
        let log_namespace = LogNamespace::default();
        let payload = (0..100)
            .map(|_| build_sample_firehose_event(10))
            .map(|v| Event::from_json_value(v, log_namespace).unwrap())
            .collect::<Vec<Event>>();
        assert_eq!(payload.len(), 100);
        let results = expand_events(payload.into(), log_namespace).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results.len(), 1000);
        for (i, element) in results.into_iter().enumerate() {
            let observed: serde_json::Value = element.try_into().unwrap();
            assert_eq!(expected_expanded_cw_event(i % 10), observed);
        }
    }
}
