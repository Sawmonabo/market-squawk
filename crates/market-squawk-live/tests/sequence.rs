use std::error::Error;

use market_squawk_domain::{
    ConnectionGeneration, IntegrityRule, RuleVersion, SequenceIntegrity, SequenceNumber,
    SequenceValidationRule, SourceIdentifier,
};
use market_squawk_live::{SequenceTracker, SequenceValidationError};
use market_squawk_sources::{ProviderSequenceEvidence, SequenceValidationProfile};

fn rule() -> Result<IntegrityRule, Box<dyn Error>> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from("provider-sequence-v1")?,
        RuleVersion::new(1)?,
    ))
}

fn generation() -> Result<ConnectionGeneration, Box<dyn Error>> {
    Ok(ConnectionGeneration::new(1)?)
}

fn evidence(value: u64) -> Result<ProviderSequenceEvidence, Box<dyn Error>> {
    Ok(ProviderSequenceEvidence::Provided {
        value: SequenceNumber::new(value),
        rule: rule()?,
    })
}

#[test]
fn consecutive_progression_rejects_duplicate_gap_and_regression_without_advancing()
-> Result<(), Box<dyn Error>> {
    let profile = SequenceValidationProfile::Provided {
        rule: rule()?,
        progression: SequenceValidationRule::Consecutive,
    };
    let mut tracker = SequenceTracker::new(generation()?, &profile);
    assert_eq!(
        tracker.validate_snapshot(&evidence(10)?)?.integrity(),
        SequenceIntegrity::Valid
    );
    assert_eq!(
        tracker.validate_delta(&evidence(10)?),
        Err(SequenceValidationError::Duplicate {
            previous: SequenceNumber::new(10),
        })
    );
    assert_eq!(
        tracker.validate_delta(&evidence(12)?),
        Err(SequenceValidationError::Gap {
            previous: SequenceNumber::new(10),
            observed: SequenceNumber::new(12),
        })
    );
    assert_eq!(
        tracker.validate_delta(&evidence(9)?),
        Err(SequenceValidationError::Regression {
            previous: SequenceNumber::new(10),
            observed: SequenceNumber::new(9),
        })
    );
    assert_eq!(tracker.last_sequence(), Some(SequenceNumber::new(10)));
    assert_eq!(
        tracker.validate_delta(&evidence(11)?)?.integrity(),
        SequenceIntegrity::Valid
    );
    Ok(())
}

#[test]
fn delta_before_snapshot_and_rule_transplant_fail() -> Result<(), Box<dyn Error>> {
    let profile = SequenceValidationProfile::Provided {
        rule: rule()?,
        progression: SequenceValidationRule::Consecutive,
    };
    let mut tracker = SequenceTracker::new(generation()?, &profile);
    assert_eq!(
        tracker.validate_delta(&evidence(1)?),
        Err(SequenceValidationError::SnapshotRequired)
    );
    let transplanted = ProviderSequenceEvidence::Provided {
        value: SequenceNumber::new(1),
        rule: IntegrityRule::new(
            SourceIdentifier::try_from("other-sequence-rule")?,
            RuleVersion::new(1)?,
        ),
    };
    assert_eq!(
        tracker.validate_snapshot(&transplanted),
        Err(SequenceValidationError::ProfileMismatch)
    );
    Ok(())
}

#[test]
fn unsupported_sequence_remains_explicit_and_never_claims_valid() -> Result<(), Box<dyn Error>> {
    let profile = SequenceValidationProfile::Unsupported { rule: rule()? };
    let mut tracker = SequenceTracker::new(generation()?, &profile);
    let unsupported = ProviderSequenceEvidence::Unsupported { rule: rule()? };

    assert_eq!(
        tracker.validate_snapshot(&unsupported)?.integrity(),
        SequenceIntegrity::NotSupported
    );
    assert_eq!(
        tracker.validate_delta(&unsupported)?.integrity(),
        SequenceIntegrity::NotSupported
    );
    Ok(())
}

#[test]
fn non_book_sequence_progresses_without_inventing_a_snapshot() -> Result<(), Box<dyn Error>> {
    let profile = SequenceValidationProfile::Provided {
        rule: rule()?,
        progression: SequenceValidationRule::Consecutive,
    };
    let mut tracker = SequenceTracker::new(generation()?, &profile);

    assert_eq!(
        tracker.validate_non_book(&evidence(10)?)?.integrity(),
        SequenceIntegrity::Valid
    );
    assert_eq!(
        tracker.validate_non_book(&evidence(11)?)?.integrity(),
        SequenceIntegrity::Valid
    );
    assert!(matches!(
        tracker.validate_non_book(&evidence(13)?),
        Err(SequenceValidationError::Gap { .. })
    ));
    Ok(())
}
