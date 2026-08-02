use market_squawk_domain::QualificationAssessment;
use market_squawk_live::LiveExecutionCapability;

fn consume(_: LiveExecutionCapability) {}

fn wrong_type(assessment: QualificationAssessment) {
    consume(assessment);
}

fn main() {}
