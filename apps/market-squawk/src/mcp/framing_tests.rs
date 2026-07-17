use std::{
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio_util::sync::CancellationToken;

use super::{BoundedMcpReader, McpFrame, McpFramingError, McpServer};
use crate::diagnostic_engine::DiagnosticEngine;
use parking_lot::RwLock;

#[derive(Debug)]
struct InstrumentedReader {
    bytes: Vec<u8>,
    position: usize,
    fragment_bytes: usize,
    largest_requested_read: Arc<AtomicUsize>,
}

impl InstrumentedReader {
    fn new(
        bytes: Vec<u8>,
        fragment_bytes: usize,
        largest_requested_read: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            bytes,
            position: 0,
            fragment_bytes,
            largest_requested_read,
        }
    }
}

impl AsyncRead for InstrumentedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.largest_requested_read
            .fetch_max(buffer.remaining(), Ordering::Relaxed);
        if self.position == self.bytes.len() {
            return Poll::Ready(Ok(()));
        }
        let available = self.bytes.len().saturating_sub(self.position);
        let count = available
            .min(buffer.remaining())
            .min(self.fragment_bytes.max(1));
        let end = self.position.saturating_add(count);
        buffer.put_slice(&self.bytes[self.position..end]);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

fn nonzero(value: usize) -> Result<NonZeroUsize, &'static str> {
    NonZeroUsize::new(value).ok_or("test bound must be nonzero")
}

#[tokio::test]
async fn exact_maximum_frame_is_accepted_at_newline_and_eof()
-> Result<(), Box<dyn std::error::Error>> {
    for suffix in [b"\n".as_slice(), b"".as_slice()] {
        let mut input = vec![b'x'; 8];
        input.extend_from_slice(suffix);
        let requested = Arc::new(AtomicUsize::new(0));
        let reader = InstrumentedReader::new(input, 3, Arc::clone(&requested));
        let mut frames = BoundedMcpReader::new(reader, nonzero(8)?, nonzero(4)?)?;

        assert_eq!(
            frames.next_frame(&CancellationToken::new()).await?,
            McpFrame::Frame(&[b'x'; 8])
        );
        assert_eq!(
            frames.next_frame(&CancellationToken::new()).await?,
            McpFrame::EndOfInput
        );
        assert_eq!(frames.frame_storage_bytes(), 9);
        assert!(requested.load(Ordering::Relaxed) <= 4);
    }
    Ok(())
}

#[tokio::test]
async fn maximum_plus_one_terminates_with_or_without_newline()
-> Result<(), Box<dyn std::error::Error>> {
    for suffix in [b"\n".as_slice(), b"".as_slice()] {
        let mut input = vec![b'x'; 9];
        input.extend_from_slice(suffix);
        let requested = Arc::new(AtomicUsize::new(0));
        let reader = InstrumentedReader::new(input, 2, Arc::clone(&requested));
        let mut frames = BoundedMcpReader::new(reader, nonzero(8)?, nonzero(3)?)?;

        assert!(matches!(
            frames.next_frame(&CancellationToken::new()).await,
            Err(McpFramingError::Oversized { maximum_bytes: 8 })
        ));
        assert_eq!(frames.frame_storage_bytes(), 9);
        assert!(requested.load(Ordering::Relaxed) <= 3);
    }
    Ok(())
}

#[tokio::test]
async fn fragmented_crlf_empty_and_multiple_frames_preserve_line_protocol()
-> Result<(), Box<dyn std::error::Error>> {
    let requested = Arc::new(AtomicUsize::new(0));
    let reader =
        InstrumentedReader::new(b"one\r\n\ntwo\nthree".to_vec(), 1, Arc::clone(&requested));
    let mut frames = BoundedMcpReader::new(reader, nonzero(8)?, nonzero(2)?)?;
    let cancellation = CancellationToken::new();

    assert_eq!(
        frames.next_frame(&cancellation).await?,
        McpFrame::Frame(b"one")
    );
    assert_eq!(
        frames.next_frame(&cancellation).await?,
        McpFrame::Frame(b"")
    );
    assert_eq!(
        frames.next_frame(&cancellation).await?,
        McpFrame::Frame(b"two")
    );
    assert_eq!(
        frames.next_frame(&cancellation).await?,
        McpFrame::Frame(b"three")
    );
    assert_eq!(
        frames.next_frame(&cancellation).await?,
        McpFrame::EndOfInput
    );
    assert!(requested.load(Ordering::Relaxed) <= 2);
    Ok(())
}

#[tokio::test]
async fn pending_frame_read_is_cancellable() -> Result<(), Box<dyn std::error::Error>> {
    let (mut writer, reader) = tokio::io::duplex(8);
    writer.write_all(b"partial").await?;
    let mut frames = BoundedMcpReader::new(reader, nonzero(16)?, nonzero(4)?)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    assert!(matches!(
        frames.next_frame(&cancellation).await,
        Err(McpFramingError::Cancelled)
    ));
    Ok(())
}

#[tokio::test]
async fn oversized_session_emits_one_bounded_error_then_terminates()
-> Result<(), Box<dyn std::error::Error>> {
    let server = McpServer::new(
        Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
        "unused.msj".into(),
    );
    let mut input = vec![b'x'; 9];
    input.extend_from_slice(b"\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n");
    let requested = Arc::new(AtomicUsize::new(0));
    let reader = InstrumentedReader::new(input, 3, Arc::clone(&requested));
    let mut output = Vec::new();

    server
        .serve_io(
            reader,
            &mut output,
            nonzero(8)?,
            nonzero(4)?,
            CancellationToken::new(),
        )
        .await?;

    let lines = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty());
    let messages = lines
        .map(serde_json::from_slice::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["error"]["code"], -32600);
    assert!(
        messages[0]["error"]["message"]
            .as_str()
            .is_some_and(|message| message.len() < 128)
    );
    assert!(requested.load(Ordering::Relaxed) <= 4);
    Ok(())
}
