use super::{MAX_STREAM_BATCH_BYTES, wire};
use crate::store::{Problem, Record};
use anyhow::{Context, Result};
use prost::Message;
use serde_json::{Map, Value};

fn object(fields: Map<String, Value>) -> wire::ReplayObject {
    wire::ReplayObject {
        fields: fields
            .into_iter()
            .map(|(key, v)| wire::ReplayField {
                key,
                value: Some(value(v)),
            })
            .collect(),
    }
}

fn value(v: Value) -> wire::ReplayValue {
    use wire::replay_value::Kind;
    let kind = match v {
        Value::Null => Kind::NullValue(true),
        Value::Bool(v) => Kind::Boolean(v),
        Value::String(v) => Kind::Text(v),
        Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Kind::Integer(v)
            } else if let Some(v) = n.as_u64() {
                Kind::Positive(v)
            } else {
                Kind::Real(n.as_f64().expect("JSON floating-point number"))
            }
        }
        Value::Object(v) => Kind::Object(object(v)),
        Value::Array(v) => Kind::Array(wire::ReplayArray {
            items: v.into_iter().map(value).collect(),
        }),
    };
    wire::ReplayValue { kind: Some(kind) }
}

// Keep recursive messages below Protobuf decoder nesting limits and bound temporary allocations.
fn fits_tree(v: &Value, depth: usize, remaining: &mut usize) -> bool {
    if depth > 16 || *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    match v {
        Value::Array(a) => a.iter().all(|v| fits_tree(v, depth + 1, remaining)),
        Value::Object(o) => o.values().all(|v| fits_tree(v, depth + 1, remaining)),
        _ => true,
    }
}

fn take_id(fields: &mut Map<String, Value>, key: &str) -> Result<String> {
    match fields.remove(key) {
        Some(Value::String(id)) => Ok(id),
        _ => anyhow::bail!("invalid journal event identifier {key}"),
    }
}

pub(super) fn events(after: i64, records: Vec<Record>) -> Result<wire::ReplayPage> {
    use wire::replay_event::Attributes;
    let mut page = wire::ReplayPage {
        after,
        records: vec![],
    };
    let mut size = page.encoded_len();
    for r in records {
        let Value::Object(mut fields) = r.event else {
            anyhow::bail!("invalid journal event")
        };
        let task_id = take_id(&mut fields, "task_id")?;
        let event_id = take_id(&mut fields, "event_id")?;
        let payload = if fields.get("payload").is_some_and(Value::is_string) {
            Some(take_id(&mut fields, "payload")?)
        } else {
            None
        };
        let mut budget = 8192;
        let attributes = if fields.is_empty() {
            None
        } else if fields.values().all(|v| fits_tree(v, 1, &mut budget)) {
            Some(Attributes::ObjectProto(object(fields).encode_to_vec()))
        } else {
            Some(Attributes::ObjectJson(serde_json::to_vec(&fields)?))
        };
        let record = wire::ReplayEvent {
            offset: r.offset,
            partition: r.partition,
            task_id,
            event_id,
            payload,
            attributes,
        };
        let n = record.encoded_len();
        size += 1 + prost::length_delimiter_len(n) + n;
        if size > MAX_STREAM_BATCH_BYTES {
            anyhow::ensure!(
                !page.records.is_empty(),
                "encoded record exceeds replay byte limit"
            );
            break;
        }
        page.records.push(record);
    }
    Ok(page)
}

fn exact_double(v: &Value) -> Option<f64> {
    let f = v.as_f64()?;
    // Compare in a wider integer type so saturating float casts cannot admit an inexact endpoint.
    if let Some(i) = v.as_i64() {
        return (f as i128 == i as i128).then_some(f);
    }
    if let Some(u) = v.as_u64() {
        return (f as u128 == u as u128).then_some(f);
    }
    Some(f)
}

pub(super) fn numbers(
    after: i64,
    records: Vec<Record>,
    array: &str,
    field: &str,
) -> Result<wire::NumericPage> {
    let mut page = wire::NumericPage {
        after,
        rows: vec![],
        values_f64_le: vec![],
    };
    for r in records {
        let start = page.values_f64_le.len();
        let samples = r.event.get(array).and_then(Value::as_array);
        let Some(samples) = samples else {
            if !page.rows.is_empty() {
                break;
            }
            return Err(Problem::Invalid(format!(
                "expected array {array:?} at offset {}",
                r.offset
            ))
            .into());
        };
        let row = wire::NumericRow {
            offset: r.offset,
            partition: r.partition,
            task_id: r.event["task_id"]
                .as_str()
                .context("invalid task ID")?
                .into(),
            event_id: r.event["event_id"]
                .as_str()
                .context("invalid event ID")?
                .into(),
            count: samples.len().try_into()?,
        };
        page.rows.push(row);
        let end = start + samples.len() * 8;
        // encoded_len includes the previous byte field with its previous length prefix.
        let bytes_size = |n: usize| {
            if n == 0 {
                0
            } else {
                1 + prost::length_delimiter_len(n) + n
            }
        };
        if page.encoded_len() - bytes_size(start) + bytes_size(end) > MAX_STREAM_BATCH_BYTES {
            page.rows.pop();
            if page.rows.is_empty() {
                return Err(Problem::Invalid(format!(
                    "numeric projection exceeds page limit at offset {}",
                    r.offset
                ))
                .into());
            }
            break;
        }
        for sample in samples {
            let Some(number) = sample.get(field).and_then(exact_double) else {
                page.rows.pop();
                page.values_f64_le.truncate(start);
                if !page.rows.is_empty() {
                    return Ok(page);
                }
                return Err(Problem::Invalid(format!(
                    "expected exact float64 {field:?} at offset {}",
                    r.offset
                ))
                .into());
            };
            page.values_f64_le.extend_from_slice(&number.to_le_bytes());
        }
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(offset: i64, event: Value) -> Record {
        Record {
            offset,
            partition: 2,
            event,
        }
    }

    #[test]
    fn projected_pages_stop_before_the_first_record_that_wont_fit() -> Result<()> {
        // Each input fits in the journal. The rate column expands short JSON numbers.
        let samples = vec![json!({"n": 1}); 90_000];
        let event = json!({"event_id":"e","task_id":"t","rows": samples});
        let rows = (1..=3).map(|i| record(i, event.clone())).collect();
        let page = numbers(0, rows, "rows", "n")?;
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.values_f64_le.len(), 180_000 * 8);
        assert!(page.encoded_len() <= MAX_STREAM_BATCH_BYTES);
        Ok(())
    }

    #[test]
    fn encoded_events_respect_the_wire_limit() -> Result<()> {
        let text = "x".repeat(710_000);
        let rows = (1..=3)
            .map(|i| record(i, json!({"event_id":"e","task_id":"t","payload":text})))
            .collect();
        let page = events(0, rows)?;
        assert_eq!(
            page.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(page.encoded_len() <= MAX_STREAM_BATCH_BYTES);
        Ok(())
    }
}
