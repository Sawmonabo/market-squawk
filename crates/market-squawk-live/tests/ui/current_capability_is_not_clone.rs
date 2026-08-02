use market_squawk_live::LiveExecutionCapability;

fn require_clone<T: Clone>() {}

fn main() {
    require_clone::<LiveExecutionCapability>();
}
