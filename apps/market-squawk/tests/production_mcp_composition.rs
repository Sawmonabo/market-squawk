use std::{collections::BTreeMap, ffi::OsString, sync::Arc};

use market_squawk::{DiagnosticEngine, mcp::LocalMcpComposition};
use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};
use parking_lot::RwLock;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn shipping_mcp_constructor_uses_the_bounded_sdk_durable_audit_and_controlled_artifacts()
-> TestResult {
    let temporary = tempfile::tempdir()?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(temporary.path().to_path_buf()),
            ..ConfigOverrides::default()
        },
    ))?;
    let journal_path = temporary
        .path()
        .join("journal")
        .join("coinbase-exchange.msj");
    let composition = LocalMcpComposition::try_new(
        &config,
        Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
        journal_path,
    )?;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server);
    let cancellation = CancellationToken::new();
    let task =
        tokio::spawn(composition.serve_unverified_io(server_reader, server_writer, cancellation));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-init","method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"market-squawk-tests","version":"1"}
            }
        }),
    )
    .await?;
    let initialized = read_message(&mut client_reader).await?;
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(initialized["result"]["serverInfo"]["name"], "market-squawk");
    write_message(
        &mut client_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    write_message(
        &mut client_writer,
        json!({"jsonrpc":"2.0","id":"shipping-ping","method":"ping"}),
    )
    .await?;
    assert_eq!(read_message(&mut client_reader).await?["result"], json!({}));
    client_writer.shutdown().await?;
    let _exit = task.await??;

    let audit = temporary.path().join("control").join("mcp-audit.jsonl");
    assert!(audit.is_file());
    assert!(std::fs::metadata(audit)?.len() > 0);
    assert!(temporary.path().join("artifacts").join("mcp").is_dir());
    Ok(())
}

async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: Value,
) -> TestResult {
    writer.write_all(&serde_json::to_vec(&value)?).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn read_message<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> TestResult<Value> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}
