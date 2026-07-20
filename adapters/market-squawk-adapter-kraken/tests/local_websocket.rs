use std::error::Error;
use std::str::FromStr;

use futures_util::{SinkExt, StreamExt};
use market_squawk_adapter_kraken::{
    KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenDepth,
};
use market_squawk_domain::InstrumentId;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const BAD_UPDATE: &str = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0"}],"asks":[],"checksum":1,"timestamp":"2023-10-04T07:48:26Z"}]}"#;

#[tokio::test]
async fn local_websocket_reconnect_requires_snapshot_after_quarantine() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let snapshot = include_str!("../fixtures/official_book_checksum.json").to_owned();
    let server = tokio::spawn(async move {
        for messages in [
            vec![snapshot.clone(), BAD_UPDATE.to_owned()],
            vec![BAD_UPDATE.to_owned(), snapshot.clone()],
        ] {
            let (stream, _) = listener.accept().await?;
            let mut websocket = tokio_tungstenite::accept_async(stream).await?;
            for message in messages {
                websocket.send(Message::Text(message.into())).await?;
            }
            websocket.close(None).await?;
        }
        TestResult::Ok(())
    });
    let endpoint = format!("ws://{address}");
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;

    let (mut first, _) = tokio_tungstenite::connect_async(&endpoint).await?;
    let mut generation_one = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let first_snapshot = next_text(&mut first).await?;
    assert!(matches!(
        generation_one.decode_payload(first_snapshot.as_bytes())?,
        KrakenDecodeOutcome::Market(_)
    ));
    let corrupt = next_text(&mut first).await?;
    assert!(generation_one.decode_payload(corrupt.as_bytes()).is_err());
    assert_eq!(generation_one.state(), KrakenDecoderState::Quarantined);

    let (mut second, _) = tokio_tungstenite::connect_async(&endpoint).await?;
    let mut generation_two = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let update_before_snapshot = next_text(&mut second).await?;
    assert!(
        generation_two
            .decode_payload(update_before_snapshot.as_bytes())
            .is_err()
    );
    assert_eq!(generation_two.state(), KrakenDecoderState::Quarantined);
    let fresh_snapshot = next_text(&mut second).await?;
    assert!(matches!(
        generation_two.decode_payload(fresh_snapshot.as_bytes())?,
        KrakenDecodeOutcome::Market(_)
    ));
    assert_eq!(generation_two.state(), KrakenDecoderState::Healthy);

    server.await??;
    Ok(())
}

async fn next_text<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Result<String, Box<dyn Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match websocket.next().await {
        Some(Ok(Message::Text(text))) => Ok(text.to_string()),
        Some(Ok(_)) => Err("expected a text message".into()),
        Some(Err(error)) => Err(error.into()),
        None => Err("websocket ended before a message arrived".into()),
    }
}
