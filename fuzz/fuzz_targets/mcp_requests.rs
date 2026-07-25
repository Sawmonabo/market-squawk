#![no_main]

use libfuzzer_sys::fuzz_target;
use market_squawk_mcp::fuzz_decode_client_message;

const MAX_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_BYTES {
        fuzz_decode_client_message(data);
    }
});
