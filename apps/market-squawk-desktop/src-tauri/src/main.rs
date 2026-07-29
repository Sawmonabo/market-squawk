// Rust #159105: this macOS-only debug-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range. Release diagnostics remain
// enabled because this allowance is restricted to debug-assertion builds.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    market_squawk_desktop::run();
}
