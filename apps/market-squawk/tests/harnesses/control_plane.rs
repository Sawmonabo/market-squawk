// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![allow(linker_messages)]

#[path = "../backtest_vertical.rs"]
mod backtest_vertical;
#[path = "../journal.rs"]
mod journal;
#[path = "../journal_path_integration.rs"]
mod journal_path_integration;
#[path = "../production_mcp_composition.rs"]
mod production_mcp_composition;
#[path = "../replay.rs"]
mod replay;
#[path = "../research_vertical.rs"]
mod research_vertical;
