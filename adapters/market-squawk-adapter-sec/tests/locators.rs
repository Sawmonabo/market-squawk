use std::error::Error;

use market_squawk_adapter_sec::{SecContact, SecObjectLocator};

#[test]
fn sec_urls_are_constructed_only_from_validated_provider_identifiers() -> Result<(), Box<dyn Error>>
{
    assert_eq!(
        SecObjectLocator::submissions("320193")?.url(),
        "https://data.sec.gov/submissions/CIK0000320193.json"
    );
    assert_eq!(
        SecObjectLocator::company_facts("0000320193")?.url(),
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
    );
    assert!(SecObjectLocator::companion("../secrets.json").is_err());
    assert!(
        SecObjectLocator::filing_document("320193", "0000320193-25-000079", "../escape.htm")
            .is_err()
    );
    Ok(())
}

#[test]
fn sec_contact_requires_a_declared_organization_and_administrative_email()
-> Result<(), Box<dyn Error>> {
    let contact = SecContact::try_new("Market Squawk", "ops@example.com")?;
    assert_eq!(contact.user_agent(), "Market Squawk ops@example.com");
    assert!(SecContact::try_new("Market Squawk", "not-an-email").is_err());
    assert!(SecContact::try_new("\nspoofed", "ops@example.com").is_err());
    Ok(())
}
