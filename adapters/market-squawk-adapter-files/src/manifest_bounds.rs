//! Allocation-bounded lexical and structural manifest admission.

use std::fmt;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::{ExtractionLimits, FileAdapterError, ParserLimit};

const RETAINED_BYTES_PER_OBJECT: usize = 2_048;
const RETAINED_BYTES_PER_MAPPING: usize = 512;

#[derive(Clone, Copy)]
enum Location {
    Root,
    Objects,
    Object,
    RowPolicy,
    Mappings,
    Other,
}

struct Audit {
    limits: ExtractionLimits,
    objects: usize,
    mappings: usize,
}

struct AuditSeed<'a> {
    audit: &'a mut Audit,
    location: Location,
}

struct AuditVisitor<'a> {
    audit: &'a mut Audit,
    location: Location,
}

pub(crate) fn admit(bytes: &[u8], limits: ExtractionLimits) -> Result<(), FileAdapterError> {
    let manifest_bytes = u64::try_from(bytes.len())
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::ManifestBytes))?;
    if manifest_bytes > limits.input.max_manifest_bytes {
        return Err(FileAdapterError::LimitExceeded(ParserLimit::ManifestBytes));
    }
    lexical_admission(bytes, limits)?;
    let mut audit = Audit {
        limits,
        objects: 0,
        mappings: 0,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    AuditSeed {
        audit: &mut audit,
        location: Location::Root,
    }
    .deserialize(&mut deserializer)
    .map_err(classify_audit_error)?;
    deserializer
        .end()
        .map_err(|_| FileAdapterError::InvalidManifest)?;
    let retained = bytes
        .len()
        .checked_mul(2)
        .and_then(|bytes| {
            audit
                .objects
                .checked_mul(RETAINED_BYTES_PER_OBJECT)
                .and_then(|objects| bytes.checked_add(objects))
        })
        .and_then(|bytes| {
            audit
                .mappings
                .checked_mul(RETAINED_BYTES_PER_MAPPING)
                .and_then(|mappings| bytes.checked_add(mappings))
        })
        .ok_or(FileAdapterError::LimitExceeded(
            ParserLimit::ManifestRetainedBytes,
        ))?;
    if match u64::try_from(retained) {
        Ok(retained) => retained > limits.input.max_manifest_retained_bytes,
        Err(_) => true,
    } {
        return Err(FileAdapterError::LimitExceeded(
            ParserLimit::ManifestRetainedBytes,
        ));
    }
    Ok(())
}

fn lexical_admission(bytes: &[u8], limits: ExtractionLimits) -> Result<(), FileAdapterError> {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut string_bytes = 0_usize;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
                string_bytes =
                    string_bytes
                        .checked_add(1)
                        .ok_or(FileAdapterError::LimitExceeded(
                            ParserLimit::ManifestStringBytes,
                        ))?;
            } else if *byte == b'\\' {
                escaped = true;
                string_bytes =
                    string_bytes
                        .checked_add(1)
                        .ok_or(FileAdapterError::LimitExceeded(
                            ParserLimit::ManifestStringBytes,
                        ))?;
            } else if *byte == b'"' {
                in_string = false;
            } else {
                string_bytes =
                    string_bytes
                        .checked_add(1)
                        .ok_or(FileAdapterError::LimitExceeded(
                            ParserLimit::ManifestStringBytes,
                        ))?;
            }
            if string_bytes > limits.input.max_manifest_string_bytes {
                return Err(FileAdapterError::LimitExceeded(
                    ParserLimit::ManifestStringBytes,
                ));
            }
            continue;
        }
        match *byte {
            b'"' => {
                in_string = true;
                string_bytes = 0;
            }
            b'{' | b'[' => {
                depth = depth.checked_add(1).ok_or(FileAdapterError::LimitExceeded(
                    ParserLimit::ManifestNestingDepth,
                ))?;
                if depth > limits.input.max_manifest_nesting_depth {
                    return Err(FileAdapterError::LimitExceeded(
                        ParserLimit::ManifestNestingDepth,
                    ));
                }
            }
            b'}' | b']' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(FileAdapterError::InvalidManifest)?;
            }
            _ => {}
        }
    }
    if in_string || escaped || depth != 0 {
        return Err(FileAdapterError::InvalidManifest);
    }
    Ok(())
}

impl<'de> DeserializeSeed<'de> for AuditSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(AuditVisitor {
            audit: self.audit,
            location: self.location,
        })
    }
}

impl<'de> Visitor<'de> for AuditVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded file-source manifest")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        loop {
            let location = match self.location {
                Location::Objects => Location::Object,
                Location::Mappings => Location::Other,
                _ => Location::Other,
            };
            let Some(()) = sequence.next_element_seed(AuditSeed {
                audit: self.audit,
                location,
            })?
            else {
                return Ok(());
            };
            match self.location {
                Location::Objects => {
                    self.audit.objects = self.audit.objects.checked_add(1).ok_or_else(|| {
                        serde::de::Error::custom("manifest object limit exceeded")
                    })?;
                    if self.audit.objects > self.audit.limits.input.max_manifest_objects {
                        return Err(serde::de::Error::custom("manifest object limit exceeded"));
                    }
                }
                Location::Mappings => {
                    self.audit.mappings = self.audit.mappings.checked_add(1).ok_or_else(|| {
                        serde::de::Error::custom("manifest mapping limit exceeded")
                    })?;
                    if self.audit.mappings > self.audit.limits.input.max_manifest_mappings {
                        return Err(serde::de::Error::custom("manifest mapping limit exceeded"));
                    }
                }
                _ => {}
            }
        }
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            let location = match (self.location, key.as_str()) {
                (Location::Root, "objects") => Location::Objects,
                (Location::Object, "row_policy") => Location::RowPolicy,
                (Location::RowPolicy, "fields") => Location::Mappings,
                _ => Location::Other,
            };
            map.next_value_seed(AuditSeed {
                audit: self.audit,
                location,
            })?;
        }
        Ok(())
    }
}

fn classify_audit_error(error: serde_json::Error) -> FileAdapterError {
    let message = error.to_string();
    if message.contains("manifest object limit exceeded") {
        FileAdapterError::LimitExceeded(ParserLimit::ManifestObjects)
    } else if message.contains("manifest mapping limit exceeded") {
        FileAdapterError::LimitExceeded(ParserLimit::ManifestMappings)
    } else {
        FileAdapterError::InvalidManifest
    }
}
