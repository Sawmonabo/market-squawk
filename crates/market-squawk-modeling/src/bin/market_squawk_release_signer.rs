//! Ephemeral in-memory Ed25519 authority for the closed Python release builder.

use std::env;
use std::io::{Read as _, Write as _};

use ed25519_dalek::{Signer as _, SigningKey};

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

fn main() {
    if run().is_err() {
        eprintln!("release signing request rejected");
        std::process::exit(2);
    }
}

fn run() -> Result<(), ()> {
    let maximum_request_bytes = 32 + 4 + MAX_MESSAGE_BYTES + 1;
    let mut input = Vec::with_capacity(maximum_request_bytes);
    let result = std::io::stdin()
        .take(maximum_request_bytes as u64)
        .read_to_end(&mut input)
        .map_err(|_| ())
        .and_then(|_| process(&input));
    input.fill(0);
    result
}

fn process(input: &[u8]) -> Result<(), ()> {
    let mode = env::args().nth(1).ok_or(())?;
    if env::args().nth(2).is_some() {
        return Err(());
    }
    let seed: &[u8; 32] = input.get(..32).ok_or(())?.try_into().map_err(|_| ())?;
    let signing_key = SigningKey::from_bytes(seed);
    let output = match mode.as_str() {
        "public" if input.len() == 32 => signing_key.verifying_key().to_bytes().to_vec(),
        "sign" => {
            let encoded_length: [u8; 4] =
                input.get(32..36).ok_or(())?.try_into().map_err(|_| ())?;
            let length = u32::from_be_bytes(encoded_length) as usize;
            let message = input.get(36..).ok_or(())?;
            if length == 0 || length > MAX_MESSAGE_BYTES || message.len() != length {
                return Err(());
            }
            signing_key.sign(message).to_bytes().to_vec()
        }
        _ => return Err(()),
    };
    std::io::stdout().write_all(&output).map_err(|_| ())?;
    std::io::stdout().flush().map_err(|_| ())
}
