use market_squawk_live::ShardSnapshot;

fn retain_without_reader_permit(snapshot: &ShardSnapshot) -> ShardSnapshot {
    snapshot.clone()
}

fn main() {}
