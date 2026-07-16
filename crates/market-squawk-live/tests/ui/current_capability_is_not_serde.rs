use market_squawk_live::LiveExecutionCapability;

fn main() {
    let _decoded: LiveExecutionCapability = serde_json::from_str("{}").unwrap();
}
