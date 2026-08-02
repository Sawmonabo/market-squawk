//! Canonical identities for immutable fair-value producer receipts.

use market_squawk_data::{PinnedFeatureMonetaryValue, PinnedMonetaryValue};
use market_squawk_live::QualifiedMarketObservationLease;
use market_squawk_portfolio::PortfolioRevision;
use market_squawk_valuation::MarketPriceSelection;
use sha2::{Digest as _, Sha256};

use super::{FairValueInputAuthorityError, FairValueProducerKind, FairValueReceiptReference};

pub(super) fn live_reference(
    leases: &[QualifiedMarketObservationLease],
    selected_index: usize,
    selection: MarketPriceSelection,
) -> Result<FairValueReceiptReference, FairValueInputAuthorityError> {
    let mut digest = ReceiptDigest::new(b"market-squawk/fair-value/live-receipt/v1");
    digest.usize(leases.len())?;
    digest.usize(selected_index)?;
    digest.u8(match selection {
        MarketPriceSelection::Trade => 1,
        MarketPriceSelection::Bid => 2,
        MarketPriceSelection::Ask => 3,
    });
    for lease in leases {
        digest.fixed(lease.observation().binding_digest());
    }
    receipt_reference(FairValueProducerKind::Live, digest.finish())
}

pub(super) fn research_reference(
    value: &PinnedMonetaryValue,
) -> Result<FairValueReceiptReference, FairValueInputAuthorityError> {
    let mut digest = ReceiptDigest::new(b"market-squawk/fair-value/research-receipt/v1");
    hash_manifest(&mut digest, value.manifest());
    digest.fixed(value.object_graph_digest().bytes());
    digest.fixed(value.query_identity().bytes());
    digest.fixed(value.result_digest().bytes());
    digest.usize(value.row())?;
    digest.fixed(value.payload_digest().bytes());
    receipt_reference(FairValueProducerKind::Research, digest.finish())
}

pub(super) fn analytics_reference(
    value: &PinnedFeatureMonetaryValue,
) -> Result<FairValueReceiptReference, FairValueInputAuthorityError> {
    let mut digest = ReceiptDigest::new(b"market-squawk/fair-value/analytics-receipt/v1");
    hash_manifest(&mut digest, value.manifest());
    digest.fixed(value.object_graph_digest().bytes());
    digest.fixed(value.query_identity().bytes());
    digest.fixed(value.result_digest().bytes());
    digest.usize(value.row())?;
    digest.fixed(value.lineage_digest().bytes());
    receipt_reference(FairValueProducerKind::Analytics, digest.finish())
}

pub(super) fn portfolio_reference(
    revision: &PortfolioRevision,
) -> Result<FairValueReceiptReference, FairValueInputAuthorityError> {
    let mut digest = ReceiptDigest::new(b"market-squawk/fair-value/portfolio-receipt/v1");
    digest.fixed(revision.token().bytes());
    receipt_reference(FairValueProducerKind::Portfolio, digest.finish())
}

fn hash_manifest(digest: &mut ReceiptDigest, manifest: &market_squawk_data::DatasetManifestRef) {
    digest.bytes(manifest.dataset_id().as_str().as_bytes());
    digest.u64(manifest.manifest_version());
    digest.bytes(manifest.schema().name().as_bytes());
    digest.u16(manifest.schema_version().get());
    digest.fixed(manifest.schema().fingerprint());
    digest.fixed(manifest.content_hash().bytes());
}

fn receipt_reference(
    producer: FairValueProducerKind,
    digest: [u8; 32],
) -> Result<FairValueReceiptReference, FairValueInputAuthorityError> {
    let prefix = producer_prefix(producer);
    let length = prefix
        .len()
        .checked_add(65)
        .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
    let mut value = String::new();
    value
        .try_reserve_exact(length)
        .map_err(|_| FairValueInputAuthorityError::Allocation)?;
    value.push_str(prefix);
    value.push(':');
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(FairValueReceiptReference(value.into_boxed_str()))
}

pub(super) fn valid_reference(producer: FairValueProducerKind, value: &str) -> bool {
    let prefix = producer_prefix(producer);
    value
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix(':'))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

const fn producer_prefix(producer: FairValueProducerKind) -> &'static str {
    match producer {
        FairValueProducerKind::Live => "live",
        FairValueProducerKind::Research => "research",
        FairValueProducerKind::Analytics => "analytics",
        FairValueProducerKind::Portfolio => "portfolio",
    }
}

struct ReceiptDigest(Sha256);

impl ReceiptDigest {
    fn new(domain: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(domain.len().to_be_bytes());
        digest.update(domain);
        Self(digest)
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.update(value.len().to_be_bytes());
        self.0.update(value);
    }

    fn fixed(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u16(&mut self, value: u16) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn usize(&mut self, value: usize) -> Result<(), FairValueInputAuthorityError> {
        self.u64(
            u64::try_from(value).map_err(|_| FairValueInputAuthorityError::RetainedSizeOverflow)?,
        );
        Ok(())
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}
