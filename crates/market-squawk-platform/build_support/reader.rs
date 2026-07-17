//! Cancellable bounded pipe readers for contained build subprocesses.

use std::error::Error;
use std::io::Read;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

#[cfg(not(unix))]
const PIPE_READER_GRACE: Duration = Duration::from_millis(500);
const PIPE_READER_POLL_INTERVAL: Duration = Duration::from_millis(2);

#[derive(Debug)]
pub(super) struct BoundedReader {
    receiver: mpsc::Receiver<Result<Vec<u8>, String>>,
    cancellation: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

#[cfg(unix)]
pub(super) fn bounded_reader<R>(
    reader: R,
    maximum: usize,
    fail_before_read: bool,
) -> Result<BoundedReader, Box<dyn Error>>
where
    R: Read + std::os::fd::AsFd + Send + 'static,
{
    let flags = rustix::fs::fcntl_getfl(&reader)?;
    rustix::fs::fcntl_setfl(&reader, flags | rustix::fs::OFlags::NONBLOCK)?;
    Ok(spawn_bounded_reader(reader, maximum, fail_before_read))
}

#[cfg(not(unix))]
pub(super) fn bounded_reader<R>(
    reader: R,
    maximum: usize,
    fail_before_read: bool,
) -> Result<BoundedReader, Box<dyn Error>>
where
    R: Read + Send + 'static,
{
    Ok(spawn_bounded_reader(reader, maximum, fail_before_read))
}

fn spawn_bounded_reader<R: Read + Send + 'static>(
    reader: R,
    maximum: usize,
    fail_before_read: bool,
) -> BoundedReader {
    let (sender, receiver) = mpsc::sync_channel(1);
    let cancellation = Arc::new(AtomicBool::new(false));
    let reader_cancellation = Arc::clone(&cancellation);
    let thread = std::thread::spawn(move || {
        let result = if fail_before_read {
            Err("injected bounded-reader failure".to_owned())
        } else {
            read_bounded(reader, maximum, &reader_cancellation)
        };
        let _send_result = sender.send(result);
    });
    BoundedReader {
        receiver,
        cancellation,
        thread,
    }
}

fn read_bounded<R: Read>(
    mut reader: R,
    maximum: usize,
    cancellation: &AtomicBool,
) -> Result<Vec<u8>, String> {
    let bound = maximum
        .checked_add(1)
        .ok_or_else(|| "bounded reader maximum overflowed".to_owned())?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(bound)
        .map_err(|error| error.to_string())?;
    let mut buffer = [0_u8; 16 * 1024];
    while output.len() < bound {
        if cancellation.load(Ordering::Acquire) {
            return Err("bounded pipe reader was cancelled before EOF".to_owned());
        }
        let remaining = bound.saturating_sub(output.len());
        let requested = remaining.min(buffer.len());
        match reader.read(&mut buffer[..requested]) {
            Ok(0) => return Ok(output),
            Ok(read) => output.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(PIPE_READER_POLL_INTERVAL);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(output)
}

pub(super) fn receive_bounded_reader(
    reader: BoundedReader,
    deadline: Instant,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let result = match reader.receiver.recv_timeout(remaining) {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("bounded pipe reader exited without a result".into())
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            reader.cancellation.store(true, Ordering::Release);
            #[cfg(unix)]
            let cancellation_result = reader.receiver.recv();
            #[cfg(not(unix))]
            let cancellation_result = reader.receiver.recv_timeout(PIPE_READER_GRACE);
            match cancellation_result {
                Ok(_) => Err("bounded pipe reader did not reach EOF after group extinction".into()),
                #[cfg(unix)]
                Err(mpsc::RecvError) => {
                    Err("cancelled bounded pipe reader exited without a result".into())
                }
                #[cfg(not(unix))]
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err("cancelled bounded pipe reader exited without a result".into())
                }
                #[cfg(not(unix))]
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err("bounded pipe reader did not stop after cancellation".into());
                }
            }
        }
    };
    reader
        .thread
        .join()
        .map_err(|_panic| "bounded pipe reader thread panicked")?;
    result
}

#[cfg(all(test, unix))]
pub(crate) fn cancel_non_eof_reader_for_test<R>(
    reader: R,
    maximum: usize,
    deadline: Instant,
) -> Result<(), Box<dyn Error>>
where
    R: Read + std::os::fd::AsFd + Send + 'static,
{
    let reader = bounded_reader(reader, maximum, false)?;
    receive_bounded_reader(reader, deadline)?;
    Err("non-EOF test reader unexpectedly reached EOF".into())
}
