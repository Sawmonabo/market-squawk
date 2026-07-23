use std::{collections::BTreeMap, ffi::OsString};

use market_squawk::{
    LocalProduct, application::application_capabilities, mcp::LocalMcpComposition,
};
use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};
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
    let product = LocalProduct::try_new(config)?;
    let composition = LocalMcpComposition::try_new(product.paths(), product.application())?;
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
        json!({"jsonrpc":"2.0","id":"shipping-tools","method":"tools/list"}),
    )
    .await?;
    let tools = read_message(&mut client_reader).await?;
    let names = tools["result"]["tools"]
        .as_array()
        .ok_or("tools/list response is missing tools")?
        .iter()
        .map(|tool| tool["name"].as_str().ok_or("tool is missing its name"))
        .collect::<Result<Vec<_>, _>>()?;
    let expected_capabilities = application_capabilities()?;
    let expected_names = expected_capabilities
        .tools()
        .iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    assert_eq!(names, expected_names);
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-read","method":"tools/call",
            "params":{"name":"Bot.GetStatus","arguments":{
                "resultLimits":{"maximumItems":16,"maximumBytes":65536}
            }}
        }),
    )
    .await?;
    let status = read_message(&mut client_reader).await?;
    assert_eq!(
        status["result"]["structuredContent"]["data"]["state"], "stopped",
        "unexpected status response: {status}"
    );
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-mutation","method":"tools/call",
            "params":{
                "name":"Risk.TriggerKillSwitch",
                "arguments":{
                    "confirm":true,
                    "reason":"production composition test",
                    "resultLimits":{"maximumItems":16,"maximumBytes":65536}
                }
            }
        }),
    )
    .await?;
    let mutation = read_message(&mut client_reader).await?;
    assert_eq!(
        mutation["result"]["structuredContent"]["data"]["state"],
        "stopped"
    );
    assert_eq!(
        mutation["result"]["structuredContent"]["data"]["shutdownComplete"],
        true
    );
    client_writer.shutdown().await?;
    let _exit = task.await??;

    let audit = temporary.path().join("control").join("mcp-audit.jsonl");
    assert!(audit.is_file());
    assert!(std::fs::metadata(&audit)?.len() > 0);
    let mutation_phases = std::fs::read_to_string(&audit)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            record["operation"]["kind"] == "call_tool"
                && record["operation"]["name"] == "Risk.TriggerKillSwitch"
        })
        .map(|record| record["phase"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        mutation_phases,
        [
            "mutation_admitted",
            "mutation_service_completed",
            "completed"
        ]
    );
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
