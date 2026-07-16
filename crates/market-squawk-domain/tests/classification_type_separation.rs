//! Compile-time guards for classification taxonomy independence.

use market_squawk_domain::{DataQuality, ExecutionEligibility, FairValueHierarchy, MarketDepth};
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(
    FairValueHierarchy:
        Into<DataQuality>,
        TryInto<DataQuality>,
        Into<ExecutionEligibility>,
        TryInto<ExecutionEligibility>
);
assert_not_impl_any!(
    MarketDepth:
        Into<DataQuality>,
        TryInto<DataQuality>,
        Into<ExecutionEligibility>,
        TryInto<ExecutionEligibility>
);
