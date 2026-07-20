use super::*;

#[tokio::test]
async fn parquet_extracts_bounded_text_columns_and_rejects_invalid_footers()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["one"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["12.50"])) as ArrayRef,
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    let payload = writer.into_inner()?;
    fs::write(directory.path().join("valid.parquet"), payload)?;
    assert_eq!(
        extract_fixture(&directory, "valid.parquet", "parquet")
            .await??
            .records()
            .len(),
        1
    );

    fs::write(directory.path().join("truncated.parquet"), b"PAR1")?;
    let error = extract_fixture(&directory, "truncated.parquet", "parquet")
        .await?
        .err()
        .ok_or("truncated Parquet unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeParquet);

    let nullable_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let nullable_batch = RecordBatch::try_new(
        Arc::clone(&nullable_schema),
        vec![
            Arc::new(StringArray::from(vec!["one"])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), nullable_schema, None)?;
    writer.write(&nullable_batch)?;
    fs::write(
        directory.path().join("nullable.parquet"),
        writer.into_inner()?,
    )?;
    let mut limits = ExtractionLimitsInput::standard();
    limits.max_cells = 1;
    let error = extract_fixture_with_limits(&directory, "nullable.parquet", "parquet", limits)
        .await?
        .err()
        .ok_or("Parquet null cell escaped the cell budget")?;
    assert_eq!(error, FileAdapterError::LimitExceeded(ParserLimit::Cells));

    let repeated = "9".repeat(2_048);
    let identifiers = (0..512)
        .map(|index| format!("row-{index}"))
        .collect::<Vec<_>>();
    let values = vec![repeated.as_str(); identifiers.len()];
    let expanded_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let expanded_batch = RecordBatch::try_new(
        Arc::clone(&expanded_schema),
        vec![
            Arc::new(StringArray::from(identifiers)) as ArrayRef,
            Arc::new(StringArray::from(values)) as ArrayRef,
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), expanded_schema, None)?;
    writer.write(&expanded_batch)?;
    let encoded = writer.into_inner()?;
    assert!(encoded.len() < repeated.len() * 512);
    fs::write(directory.path().join("expanded.parquet"), &encoded)?;
    let mut limits = ExtractionLimitsInput::standard();
    limits.max_source_bytes = u64::try_from(encoded.len())?;
    limits.max_decompressed_bytes = limits.max_source_bytes;
    limits.max_retained_bytes = limits.max_source_bytes;
    limits.max_records = 1_024;
    limits.max_cells = 2_048;
    let error = extract_fixture_with_limits(&directory, "expanded.parquet", "parquet", limits)
        .await?
        .err()
        .ok_or("Parquet decoded buffers escaped the decoded-memory budget")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_extracts_an_allowlisted_base_table_and_rejects_views() -> Result<(), Box<dyn Error>>
{
    let directory = tempfile::tempdir()?;
    let valid_path = directory.path().join("valid.sqlite3");
    {
        let connection = Connection::open(&valid_path)?;
        connection.execute_batch(
            "CREATE TABLE prices(id TEXT PRIMARY KEY, value TEXT NOT NULL);\
             INSERT INTO prices(id, value) VALUES ('one', '12.50');",
        )?;
    }
    assert_eq!(
        extract_fixture(&directory, "valid.sqlite3", "sqlite")
            .await??
            .records()
            .len(),
        1
    );
    let database_bytes = fs::metadata(&valid_path)?.len();
    let mut memory_limits = ExtractionLimitsInput::standard();
    memory_limits.max_source_bytes = database_bytes;
    memory_limits.max_decompressed_bytes = database_bytes;
    memory_limits.max_retained_bytes = database_bytes;
    let error = extract_fixture_with_limits(&directory, "valid.sqlite3", "sqlite", memory_limits)
        .await?
        .err()
        .ok_or("SQLite in-memory image escaped the decoded-memory budget")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );

    let view_path = directory.path().join("view.sqlite3");
    {
        let connection = Connection::open(&view_path)?;
        connection.execute_batch(
            "CREATE TABLE secret(id TEXT, value TEXT);\
             CREATE VIEW prices AS SELECT id, value FROM secret;",
        )?;
    }
    let error = extract_fixture(&directory, "view.sqlite3", "sqlite")
        .await?
        .err()
        .ok_or("SQLite view unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeDatabase);
    Ok(())
}
