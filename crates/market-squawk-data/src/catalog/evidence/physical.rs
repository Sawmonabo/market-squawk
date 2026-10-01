//! Ordered physical traversal and indexed retry membership on the retained catalog.
use super::*;

const PHYSICAL_ROWS: &str = "
 SELECT 1 AS kind,a.relative_reference,a.content_digest,a.size_bytes,
        (SELECT MIN(o.row_count) FROM analytical_generation_objects o WHERE o.artifact_id=a.artifact_id) AS expected_rows,
        NULL AS schema_name,NULL AS schema_version,NULL AS schema_fingerprint
 FROM artifacts a
 UNION ALL
 SELECT 2,r.relative_reference,r.content_digest,r.size_bytes,
        (SELECT MIN(o.row_count) FROM analytical_generation_objects o WHERE o.artifact_id=r.artifact_id),NULL,NULL,NULL
 FROM query_artifact_reservations q JOIN query_artifact_results r USING(reservation_id)
 WHERE q.state='published' AND q.expires_at_ns>?1
 UNION ALL
 SELECT 3,relative_reference,content_digest,size_bytes,row_count,schema_name,schema_version,schema_fingerprint
 FROM market_event_archive_objects";

impl Catalog {
    pub(crate) fn visit_physical_evidence(
        connection: &Connection,
        snapshot: &CatalogEvidenceSnapshot,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(PhysicalArtifactEvidence) -> Result<(), EvidenceError>,
    ) -> Result<(), EvidenceError> {
        snapshot.check_cancellation(cancellation)?;
        let mut statement = connection
            .prepare(&format!(
                "SELECT * FROM ({PHYSICAL_ROWS}) ORDER BY relative_reference"
            ))
            .map_err(|_| {
                if cancellation.is_cancelled() {
                    EvidenceError::Cancelled
                } else {
                    EvidenceError::InvalidCatalogEvidence
                }
            })?;
        let mut rows = statement
            .query([snapshot.request().cutoff().unix_nanos()])
            .map_err(|_| {
                if cancellation.is_cancelled() {
                    EvidenceError::Cancelled
                } else {
                    EvidenceError::InvalidCatalogEvidence
                }
            })?;
        let mut count = 0_u64;
        let mut bytes = 0_u64;
        let mut previous: Option<Box<str>> = None;
        while let Some(row) = rows.next().map_err(|_| {
            if cancellation.is_cancelled() {
                EvidenceError::Cancelled
            } else {
                EvidenceError::InvalidCatalogEvidence
            }
        })? {
            snapshot.check_cancellation(cancellation)?;
            let artifact = decode(row).map_err(|_| {
                if cancellation.is_cancelled() {
                    EvidenceError::Cancelled
                } else {
                    EvidenceError::InvalidCatalogEvidence
                }
            })?;
            if previous
                .as_deref()
                .is_some_and(|value| value >= artifact.relative_reference())
            {
                return Err(EvidenceError::InvalidCatalogEvidence);
            }
            count = count
                .checked_add(1)
                .ok_or(EvidenceError::ResourceLimitExceeded)?;
            bytes = bytes
                .checked_add(artifact.size_bytes())
                .ok_or(EvidenceError::ResourceLimitExceeded)?;
            if count > snapshot.physical_artifact_count()
                || bytes > snapshot.physical_artifact_bytes()
            {
                return Err(EvidenceError::InvalidCatalogEvidence);
            }
            previous = Some(artifact.relative_reference().into());
            consume(artifact)?;
        }
        if count != snapshot.physical_artifact_count()
            || bytes != snapshot.physical_artifact_bytes()
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        snapshot.check_cancellation(cancellation)
    }

    pub(crate) fn physical_evidence_in_shard(
        connection: &Connection,
        snapshot: &CatalogEvidenceSnapshot,
        shard: &str,
    ) -> Result<bool, EvidenceError> {
        if shard.len() != 2
            || !shard
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(EvidenceError::DestinationConflict);
        }
        let prefix = format!("objects/sha256/{shard}/");
        let end = format!("{prefix}~");
        connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM ({PHYSICAL_ROWS}) WHERE relative_reference>=?2 AND relative_reference<?3)"),
            (snapshot.request().cutoff().unix_nanos(),prefix,end),|row|row.get(0)).map_err(|_|EvidenceError::InvalidCatalogEvidence)
    }

    pub(crate) fn physical_evidence_by_reference(
        connection: &Connection,
        snapshot: &CatalogEvidenceSnapshot,
        reference: &str,
    ) -> Result<Option<PhysicalArtifactEvidence>, EvidenceError> {
        // Equality pushes into each UNION arm and uses the durable relative-reference indexes.
        let mut statement = connection
            .prepare(&format!(
                "SELECT * FROM ({PHYSICAL_ROWS}) WHERE relative_reference=?2"
            ))
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?;
        let mut rows = statement
            .query((snapshot.request().cutoff().unix_nanos(), reference))
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?;
        let result = rows
            .next()
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?
            .map(decode)
            .transpose()
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?;
        if rows
            .next()
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?
            .is_some()
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(result)
    }
}
fn decode(row: &rusqlite::Row<'_>) -> Result<PhysicalArtifactEvidence, CatalogError> {
    let kind: i64 = row.get(0)?;
    let relative_reference = row.get::<_, String>(1)?.into_boxed_str();
    let content_hash = parse_sha256(1, row.get(2)?)?;
    let size_bytes = parse_positive_u64(row.get(3)?)?;
    let expected_row_count = row
        .get::<_, Option<i64>>(4)?
        .map(parse_positive_u64)
        .transpose()?;
    match kind {
        1 => Ok(PhysicalArtifactEvidence::Artifact {
            relative_reference,
            content_hash,
            size_bytes,
            expected_row_count,
        }),
        2 => Ok(PhysicalArtifactEvidence::QueryArtifact {
            relative_reference,
            content_hash,
            size_bytes,
            expected_row_count,
        }),
        3 => {
            let schema = generations::schema(row, 5, 6, 7)?;
            Ok(PhysicalArtifactEvidence::MarketEventArchive {
                relative_reference,
                content_hash,
                size_bytes,
                expected_row_count: expected_row_count.ok_or(CatalogError::CorruptCatalog)?,
                schema,
            })
        }
        _ => Err(CatalogError::CorruptCatalog),
    }
}
