#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use market_squawk_platform::{JournalReader, RawCaptureRecord};
use market_squawk_sources::RawMarketFrame;

const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_AGGREGATE_BYTES: u64 = 1024 * 1024;
const MAX_RECORDS: usize = 32;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    if payload.len() > MAX_INPUT_BYTES {
        return;
    }
    match selector % 3 {
        0 => {
            let reader = JournalReader::new(Cursor::new(payload));
            let _records = reader.read_all_bounded(MAX_RECORDS, MAX_AGGREGATE_BYTES);
        }
        1 => {
            let _record = serde_json::from_slice::<RawCaptureRecord>(payload);
        }
        _ => {
            let _frame = serde_json::from_slice::<RawMarketFrame>(payload);
        }
    }
});
