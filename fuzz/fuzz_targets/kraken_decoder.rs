#![no_main]

use std::str::FromStr as _;

use libfuzzer_sys::fuzz_target;
use market_squawk_adapter_kraken::{KrakenDecoder, KrakenDepth};
use market_squawk_domain::InstrumentId;

const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_FRAMES: usize = 64;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    if payload.is_empty() || payload.len() > MAX_INPUT_BYTES {
        return;
    }
    let Ok(instrument) = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed") else {
        return;
    };
    if selector & 1 == 0 {
        let Ok(mut decoder) = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)
        else {
            return;
        };
        feed_frames(payload, |frame| {
            let _outcome = decoder.decode_payload(frame);
        });
    } else {
        let Ok(mut decoder) = KrakenDecoder::try_trades("BTC/USD", instrument) else {
            return;
        };
        feed_frames(payload, |frame| {
            let _outcome = decoder.decode_payload(frame);
        });
    }
});

fn feed_frames(mut bytes: &[u8], mut decode: impl FnMut(&[u8])) {
    let original = bytes;
    let mut decoded = 0_usize;
    while bytes.len() >= 2 && decoded < MAX_FRAMES {
        let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        bytes = &bytes[2..];
        if length == 0 || length > bytes.len() {
            break;
        }
        decode(&bytes[..length]);
        bytes = &bytes[length..];
        decoded += 1;
    }
    if decoded == 0 {
        decode(original);
    }
}
