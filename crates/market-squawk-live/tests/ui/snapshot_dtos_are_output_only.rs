use market_squawk_live::ShardSnapshot;

fn main() {
    let _: ShardSnapshot = serde_json::from_str("{}").unwrap();
}
