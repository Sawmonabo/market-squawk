use super::*;

#[tokio::test]
async fn manifest_and_text_parsers_reject_ambiguous_or_expansive_inputs()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let representation_state = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let oversized_manifest = manifest("source.csv", "csv");
    fs::write(directory.path().join("manifest.json"), &oversized_manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(u64::try_from(oversized_manifest.len())?)?
        .read_bounded()?;
    let mut limits = ExtractionLimitsInput::standard();
    limits.max_manifest_bytes = u64::try_from(oversized_manifest.len().saturating_sub(1))?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&oversized_manifest)?,
        root,
        representation_state_root(&representation_state, &oversized_manifest),
        manifest_input,
        ExtractionLimits::try_new(limits)?,
        fixed_clock(),
    );
    assert!(matches!(
        source,
        Err(FileAdapterError::LimitExceeded(ParserLimit::ManifestBytes))
    ));
    for unsafe_state_root in [
        directory.path().to_path_buf(),
        directory.path().join("nested-representation-state"),
    ] {
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(u64::try_from(oversized_manifest.len())?)?
            .read_bounded()?;
        assert!(matches!(
            FileExtractionSource::try_new_with_clock(
                local_metadata(&oversized_manifest)?,
                root,
                unsafe_state_root,
                manifest_input,
                ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
                fixed_clock(),
            ),
            Err(FileAdapterError::RepresentationAuthorityScope)
        ));
    }

    let mut unknown_format: serde_json::Value = serde_json::from_slice(&oversized_manifest)?;
    unknown_format["objects"][0]["format"]["undeclared_policy"] = serde_json::json!(true);
    let unknown_format = serde_json::to_vec(&unknown_format)?;
    let mut two_objects: serde_json::Value = serde_json::from_slice(&oversized_manifest)?;
    let second = two_objects["objects"][0].clone();
    two_objects["objects"]
        .as_array_mut()
        .ok_or("manifest objects changed type")?
        .push(second);
    let two_objects = serde_json::to_vec(&two_objects)?;
    let mut two_mappings: serde_json::Value = serde_json::from_slice(&oversized_manifest)?;
    let second = two_mappings["objects"][0]["row_policy"]["fields"][0].clone();
    two_mappings["objects"][0]["row_policy"]["fields"]
        .as_array_mut()
        .ok_or("manifest mappings changed type")?
        .push(second);
    let two_mappings = serde_json::to_vec(&two_mappings)?;
    let deeply_nested = br#"{"schema_version":1,"objects":[[[[[[]]]]]]}"#.to_vec();
    let manifest_cases = [
        (
            unknown_format,
            ExtractionLimitsInput::standard(),
            FileAdapterError::InvalidManifest,
        ),
        (
            two_objects,
            ExtractionLimitsInput {
                max_manifest_objects: 1,
                ..ExtractionLimitsInput::standard()
            },
            FileAdapterError::LimitExceeded(ParserLimit::ManifestObjects),
        ),
        (
            deeply_nested,
            ExtractionLimitsInput {
                max_manifest_nesting_depth: 4,
                ..ExtractionLimitsInput::standard()
            },
            FileAdapterError::LimitExceeded(ParserLimit::ManifestNestingDepth),
        ),
        (
            two_mappings,
            ExtractionLimitsInput {
                max_manifest_mappings: 1,
                ..ExtractionLimitsInput::standard()
            },
            FileAdapterError::LimitExceeded(ParserLimit::ManifestMappings),
        ),
        (
            oversized_manifest.clone(),
            ExtractionLimitsInput {
                max_manifest_string_bytes: 8,
                ..ExtractionLimitsInput::standard()
            },
            FileAdapterError::LimitExceeded(ParserLimit::ManifestStringBytes),
        ),
        (
            oversized_manifest.clone(),
            ExtractionLimitsInput {
                max_manifest_retained_bytes: 1,
                ..ExtractionLimitsInput::standard()
            },
            FileAdapterError::LimitExceeded(ParserLimit::ManifestRetainedBytes),
        ),
    ];
    for (manifest, limits, expected) in manifest_cases {
        let directory = tempfile::tempdir()?;
        let representation_state = tempfile::tempdir()?;
        fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
        fs::write(directory.path().join("manifest.json"), &manifest)?;
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(u64::try_from(manifest.len())?)?
            .read_bounded()?;
        let result = FileExtractionSource::try_new_with_clock(
            local_metadata(&manifest)?,
            root,
            representation_state_root(&representation_state, &manifest),
            manifest_input,
            ExtractionLimits::try_new(limits)?,
            fixed_clock(),
        );
        assert!(matches!(result, Err(error) if error == expected));
    }

    let directory = tempfile::tempdir()?;
    let amplified_csv = "id,value,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,\
                         cccccccccccccccccccccccccccccccc,dddddddddddddddddddddddddddddddd,\
                         eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\n\
                         row-1,1.00,a,b,c,d,e\n\
                         row-2,2.00,a,b,c,d,e\n\
                         row-3,3.00,a,b,c,d,e\n\
                         row-4,4.00,a,b,c,d,e\n\
                         row-5,5.00,a,b,c,d,e\n";
    fs::write(directory.path().join("amplified.csv"), amplified_csv)?;
    let manifest_bytes = manifest("amplified.csv", "csv").len();
    assert!(amplified_csv.len() < manifest_bytes);
    let mut decoded_limits = ExtractionLimitsInput::standard();
    decoded_limits.max_source_bytes = u64::try_from(manifest_bytes)?;
    decoded_limits.max_decompressed_bytes = u64::try_from(manifest_bytes)?
        .checked_mul(4)
        .ok_or("decoded test limit overflow")?;
    decoded_limits.max_retained_bytes = u64::try_from(manifest_bytes)?;
    let error = extract_fixture_with_limits(&directory, "amplified.csv", "csv", decoded_limits)
        .await?
        .err()
        .ok_or("CSV retained decoded allocations escaped the decoded-byte budget")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );

    let directory = tempfile::tempdir()?;
    let structural_json = format!("[{}]", vec!["{}"; 200].join(","));
    fs::write(directory.path().join("structural.json"), &structural_json)?;
    let structural_manifest_bytes = manifest("structural.json", "json").len();
    let mut structural_limits = ExtractionLimitsInput::standard();
    structural_limits.max_source_bytes =
        u64::try_from(structural_manifest_bytes.max(structural_json.len()))?;
    structural_limits.max_decompressed_bytes = structural_limits
        .max_source_bytes
        .checked_mul(4)
        .ok_or("structural test limit overflow")?;
    structural_limits.max_retained_bytes = structural_limits.max_source_bytes;
    let error =
        extract_fixture_with_limits(&directory, "structural.json", "json", structural_limits)
            .await?
            .err()
            .ok_or("JSON structural retention escaped the decoded-byte budget")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );

    let directory = tempfile::tempdir()?;
    let escaped_json = format!(r#"[{{"id":"one","value":"{}"}}]"#, "\\u0061".repeat(512));
    fs::write(directory.path().join("escaped.json"), &escaped_json)?;
    let escaped_manifest_bytes = manifest("escaped.json", "json").len();
    let mut escaped_limits = ExtractionLimitsInput::standard();
    escaped_limits.max_source_bytes =
        u64::try_from(escaped_manifest_bytes.max(escaped_json.len()))?;
    escaped_limits.max_decompressed_bytes = escaped_limits.max_source_bytes;
    escaped_limits.max_retained_bytes = 256;
    let error = extract_fixture_with_limits(&directory, "escaped.json", "json", escaped_limits)
        .await?
        .err()
        .ok_or("escaped JSON text escaped the pre-admitted decoded-byte budget")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );

    let cases = [
        (
            "hostile.json",
            br#"[{"id":"one","value":"1.00","value":"2.00"}]"#.as_slice(),
            "json",
            FileAdapterError::DuplicateField,
        ),
        (
            "numeric-scale.json",
            br#"[{"id":"one","value":1e0}]"#.as_slice(),
            "json",
            FileAdapterError::DecimalScaleMismatch,
        ),
        (
            "numeric-overprecision.json",
            br#"[{"id":"one","value":12345678901234567890123456789.00}]"#.as_slice(),
            "json",
            FileAdapterError::InvalidDecimal,
        ),
        (
            "malformed-exponent.json",
            br#"[{"id":"one","value":"1.2.3e0"}]"#.as_slice(),
            "json",
            FileAdapterError::InvalidDecimal,
        ),
        (
            "hostile.xml",
            br#"<!DOCTYPE rows [<!ENTITY x SYSTEM 'https://example.test/x'>]><rows/>"#.as_slice(),
            "xml",
            FileAdapterError::UnsafeXml,
        ),
        (
            "fragmented.xml",
            br#"<rows><row><id>one</id><value>1.00</value></row></rows><rows/>"#.as_slice(),
            "xml",
            FileAdapterError::InvalidRecord,
        ),
        (
            "late-declaration.xml",
            br#"<rows><row><id>one</id><value>1.00</value></row></rows><?xml version="1.0"?>"#
                .as_slice(),
            "xml",
            FileAdapterError::InvalidRecord,
        ),
        (
            "hostile.csv",
            b"id,value\none,12345.00\n".as_slice(),
            "csv",
            FileAdapterError::LimitExceeded(ParserLimit::TextBytes),
        ),
    ];

    for (name, payload, format, expected) in cases {
        let directory = tempfile::tempdir()?;
        let representation_state = tempfile::tempdir()?;
        fs::write(directory.path().join(name), payload)?;
        let manifest = manifest(name, format);
        fs::write(directory.path().join("manifest.json"), &manifest)?;
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(16 * 1024)?
            .read_bounded()?;
        let mut limits = ExtractionLimitsInput::standard();
        if format == "csv" {
            limits.max_text_bytes = 4;
        }
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
            NonZeroU32::new(16).ok_or("nonzero")?,
            NonZeroU64::new(1024 * 1024).ok_or("nonzero")?,
            Timestamp::from_unix_nanos(10_000_000_000),
        )?;
        let error = match source
            .extract_file(&request, &CancellationToken::new())
            .await
        {
            Ok(_) => return Err("hostile input unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert_eq!(error, expected, "unexpected parser result for {format}");
    }
    Ok(())
}
