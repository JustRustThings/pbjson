use crate::Timestamp;
use serde::de::Visitor;
use serde::Serialize;
use time::format_description::well_known::Rfc3339;

impl From<time::OffsetDateTime> for Timestamp {
    fn from(dt: time::OffsetDateTime) -> Self {
        // The UNIX timestamp is relative to UTC by definition, the time crate respects this.
        let seconds = dt.unix_timestamp();
        // `.nanoseconds()` guarantees a return value in 0 .. 1_000_000_000 and so will
        // always fit in an `i32`.
        let nanos = dt.nanosecond() as i32;
        Self { seconds, nanos }
    }
}

impl From<Timestamp> for time::OffsetDateTime {
    fn from(ts: Timestamp) -> Self {
        let ts = ts.seconds as i128 * 1_000_000_000 + ts.nanos as i128;
        // This cannot fail since the passed value is supposed to be a
        // valid UTC timestamp itself.
        Self::from_unix_timestamp_nanos(ts).unwrap()
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t: time::OffsetDateTime = self.clone().try_into().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&t.format(&Rfc3339).map_err(serde::ser::Error::custom)?)
    }
}

struct TimestampVisitor;

impl<'de> Visitor<'de> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a date string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let d = time::OffsetDateTime::parse(s, &Rfc3339).map_err(serde::de::Error::custom)?;
        Ok(d.into())
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TimestampVisitor)
    }
}

#[allow(clippy::derived_hash_with_manual_eq)] // Derived logic is correct: comparing the 2 fields for equality
impl std::hash::Hash for Timestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.seconds.hash(state);
        self.nanos.hash(state);
    }
}

/// Implements the unstable/naive version of `Eq`: a basic equality check on the internal fields of the `Timestamp`.
/// This implies that `normalized_ts != non_normalized_ts` even if `normalized_ts == non_normalized_ts.normalized()`.
impl Eq for Timestamp {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};
    use serde::de::value::{BorrowedStrDeserializer, Error};
    use serde::Deserialize;

    #[test]
    fn test_date() {
        let deserializer = BorrowedStrDeserializer::<'_, Error>::new(&encoded);
        let a: Timestamp = Timestamp::deserialize(deserializer).unwrap();
        assert_eq!(a.seconds, utc.timestamp());
        assert_eq!(a.nanos, utc.timestamp_subsec_nanos() as i32);

        let encoded = serde_json::to_string(&a).unwrap();
        assert_eq!(encoded, format!("\"{}\"", utc_encoded));
    }
}
