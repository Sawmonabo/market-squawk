use super::*;

pub(super) fn xlsx_package(sheet: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    xlsx_package_with_relationship(sheet, "worksheets/sheet1.xml")
}

pub(super) fn with_eocd_entry_count(
    mut archive: Vec<u8>,
    entry_count: u16,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let signature = [0x50, 0x4b, 0x05, 0x06];
    let offset = archive
        .windows(signature.len())
        .rposition(|window| window == signature)
        .ok_or("ZIP EOCD is absent")?;
    archive
        .get_mut(offset + 8..offset + 12)
        .ok_or("ZIP EOCD entry fields are truncated")?
        .chunks_exact_mut(2)
        .for_each(|field| field.copy_from_slice(&entry_count.to_le_bytes()));
    Ok(archive)
}

pub(super) fn xlsx_package_with_relationship(
    sheet: &[u8],
    relationship_target: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let files: [(&str, &[u8]); 8] = [
        (
            "[Content_Types].xml",
            br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/></Types>"#,
        ),
        (
            "_rels/.rels",
            br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>"#,
        ),
        (
            "xl/workbook.xml",
            br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>"#,
        ),
        ("xl/worksheets/sheet1.xml", sheet),
        ("docProps/core.xml", br#"<coreProperties/>"#),
        ("docProps/app.xml", br#"<Properties/>"#),
        ("xl/styles.xml", br#"<styleSheet/>"#),
        ("xl/theme/theme1.xml", br#"<theme/>"#),
    ];
    for (name, bytes) in files {
        archive.start_file(name, SimpleFileOptions::default())?;
        archive.write_all(bytes)?;
    }
    let workbook_relationships = format!(
        r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="{relationship_target}"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="theme/theme1.xml"/></Relationships>"#
    );
    archive.start_file("xl/_rels/workbook.xml.rels", SimpleFileOptions::default())?;
    archive.write_all(workbook_relationships.as_bytes())?;
    Ok(archive.finish()?.into_inner())
}

pub(super) fn xlsx_custom_package(
    sheet: &[u8],
    content_types: &[u8],
    root_relationships: Option<&[u8]>,
    workbook: &[u8],
    workbook_relationships: &[u8],
    shared_strings: Option<&[u8]>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    let mut files = vec![
        ("[Content_Types].xml", content_types),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_relationships),
        ("xl/worksheets/sheet1.xml", sheet),
    ];
    if let Some(root_relationships) = root_relationships {
        files.push(("_rels/.rels", root_relationships));
    }
    if let Some(shared_strings) = shared_strings {
        files.push(("xl/sharedStrings.xml", shared_strings));
    }
    for (name, bytes) in files {
        archive.start_file(name, SimpleFileOptions::default())?;
        archive.write_all(bytes)?;
    }
    Ok(archive.finish()?.into_inner())
}

pub(super) async fn extract_fixture(
    directory: &tempfile::TempDir,
    name: &str,
    format: &str,
) -> Result<Result<market_squawk_sources::ExtractionBatch, FileAdapterError>, Box<dyn Error>> {
    extract_fixture_with_limits(directory, name, format, ExtractionLimitsInput::standard()).await
}

pub(super) async fn extract_fixture_with_limits(
    directory: &tempfile::TempDir,
    name: &str,
    format: &str,
    limits: ExtractionLimitsInput,
) -> Result<Result<market_squawk_sources::ExtractionBatch, FileAdapterError>, Box<dyn Error>> {
    let representation_state = tempfile::tempdir()?;
    let request_max_records = NonZeroU32::new(u32::try_from(limits.max_records)?)
        .ok_or("nonzero extraction record limit")?;
    let manifest = manifest(name, format);
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root(&representation_state, &manifest),
        manifest_input,
        ExtractionLimits::try_new(limits)?,
        fixed_clock(),
    )?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let discovered = source
        .discover_files(&discovery, &CancellationToken::new())
        .await?;
    let request = ExtractionRequest::try_new(
        discovered.objects()[0].clone(),
        request_max_records,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    Ok(source
        .extract_file(&request, &CancellationToken::new())
        .await)
}

pub(super) fn representation_state_root(
    state_directory: &tempfile::TempDir,
    manifest: &[u8],
) -> std::path::PathBuf {
    representation_state_root_for(state_directory, manifest, "default")
}

pub(super) fn representation_state_root_for(
    state_directory: &tempfile::TempDir,
    manifest: &[u8],
    namespace: &str,
) -> std::path::PathBuf {
    state_directory.path().join(format!(
        "representation-authority-{:x}-{namespace}",
        Sha256::digest(manifest)
    ))
}

pub(super) fn manifest(name: &str, format: &str) -> Vec<u8> {
    manifest_with_superseded(name, format, None)
}

pub(super) fn manifest_with_superseded(
    name: &str,
    format: &str,
    superseded_at: Option<i64>,
) -> Vec<u8> {
    let format_policy = match format {
        "csv" => serde_json::json!({ "kind": "csv", "delimiter": 44 }),
        "xml" => serde_json::json!({ "kind": "xml", "record_element": "row" }),
        "excel" => serde_json::json!({
            "kind": "excel",
            "formula_policy": "reject"
        }),
        "sqlite" => serde_json::json!({
            "kind": "sqlite",
            "table": "prices",
            "columns": ["id", "value"],
            "order_by": ["id"]
        }),
        "ofx" | "qfx" => serde_json::json!({
            "kind": format,
            "account_id": "acct-1",
            "currency": "USD"
        }),
        _ => serde_json::json!({ "kind": format }),
    };
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "objects": [{
            "dataset": "alternative-prices",
            "object_id": format!("{format}-fixture"),
            "path": name,
            "format": format_policy,
            "effective_at": 100,
            "published_at": 150,
            "revision": "revision-1",
            "revision_number": 1,
            "superseded_at": superseded_at,
            "row_policy": {
                "identity_field": "id",
                "fields": [{
                    "source": "value",
                    "field": "price",
                    "decimal_scale": 2,
                    "unit": "USD"
                }]
            }
        }]
    }))
    .unwrap_or_default()
}

pub(super) fn local_metadata(manifest: &[u8]) -> Result<SourceMetadata, Box<dyn Error>> {
    let digest = market_squawk_domain::EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(manifest).into(),
    );
    let evidence = ExactPayloadEvidence::from_content_digest(digest);
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("local-files-fixture")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("manifest-revision-1")?),
            evidence.clone(),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("user-owned-local-files")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            evidence.clone(),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            evidence,
            effective,
            CoverageDomain::AlternativeData,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}
