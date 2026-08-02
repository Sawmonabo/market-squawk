//! Producer-specific fair-value receipt publication.

use std::{mem::size_of, time::Instant};

use market_squawk_data::{PinnedFeatureMonetaryValue, PinnedMonetaryValue};
use market_squawk_live::{
    CommittedQualifiedMarketObservation, QualifiedMarketObservationLease, QualifiedMarketPrice,
};
use market_squawk_portfolio::PortfolioRevision;
use market_squawk_valuation::MarketPriceSelection;
use tokio_util::sync::CancellationToken;

use super::identity::{
    analytics_reference, live_reference, portfolio_reference, research_reference,
};
use super::{
    AnalyticsFairValueInputPublisher, FairValueInputAuthorityError, FairValueReceiptRegistration,
    LiveFairValueInputPublisher, PortfolioFairValueInputPublisher, ProducerReceipt,
    ResearchFairValueInputPublisher, StoredReceipt, ensure_publication_live, register,
};

impl LiveFairValueInputPublisher {
    /// Publishes one canonical observation set and exact selected trade or quote side.
    pub async fn publish(
        &self,
        mut leases: Vec<QualifiedMarketObservationLease>,
        selected_index: usize,
        selection: MarketPriceSelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FairValueReceiptRegistration, FairValueInputAuthorityError> {
        ensure_publication_live(deadline, cancellation)?;
        if leases.is_empty()
            || leases.len() > self.shared.limits.maximum_live_receipts_per_input
            || selected_index >= leases.len()
        {
            return Err(FairValueInputAuthorityError::InvalidReceipt);
        }
        let selected_binding = leases[selected_index].observation().binding_digest();
        leases.sort_unstable_by_key(|lease| lease.observation().binding_digest());
        if leases.windows(2).any(|pair| {
            pair[0].observation().binding_digest() == pair[1].observation().binding_digest()
                && pair[0].observation() != pair[1].observation()
        }) {
            return Err(FairValueInputAuthorityError::ReceiptConflict);
        }
        let selected_index = leases
            .iter()
            .position(|lease| lease.observation().binding_digest() == selected_binding)
            .ok_or(FairValueInputAuthorityError::InvalidReceipt)?;
        validate_live_selection(leases[selected_index].observation(), selection)?;
        let reference = live_reference(&leases, selected_index, selection)?;
        let mut retained_bytes = reference.as_str().len().checked_add(
            leases
                .len()
                .checked_mul(size_of::<CommittedQualifiedMarketObservation>())
                .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?,
        );
        for lease in &leases {
            retained_bytes =
                retained_bytes.and_then(|total| total.checked_add(lease.retained_bytes()));
        }
        let retained_bytes =
            retained_bytes.ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(leases.len())
            .map_err(|_| FairValueInputAuthorityError::Allocation)?;
        observations.extend(
            leases
                .into_iter()
                .map(QualifiedMarketObservationLease::into_observation),
        );
        register(
            &self.shared,
            StoredReceipt {
                reference,
                producer: ProducerReceipt::Live {
                    observations,
                    selected_index,
                    selection,
                },
                retained_bytes,
            },
            deadline,
            cancellation,
        )
        .await
    }
}

impl ResearchFairValueInputPublisher {
    /// Retains one immutable manifest-pinned research value.
    pub async fn publish(
        &self,
        value: PinnedMonetaryValue,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FairValueReceiptRegistration, FairValueInputAuthorityError> {
        ensure_publication_live(deadline, cancellation)?;
        let reference = research_reference(&value)?;
        let retained_bytes = value
            .retained_bytes()
            .and_then(|bytes| bytes.checked_add(reference.as_str().len()))
            .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
        register(
            &self.shared,
            StoredReceipt {
                reference,
                producer: ProducerReceipt::Research(value),
                retained_bytes,
            },
            deadline,
            cancellation,
        )
        .await
    }
}

impl AnalyticsFairValueInputPublisher {
    /// Retains one immutable manifest-pinned feature value.
    pub async fn publish(
        &self,
        value: PinnedFeatureMonetaryValue,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FairValueReceiptRegistration, FairValueInputAuthorityError> {
        ensure_publication_live(deadline, cancellation)?;
        let reference = analytics_reference(&value)?;
        let retained_bytes = value
            .retained_bytes()
            .and_then(|bytes| bytes.checked_add(reference.as_str().len()))
            .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
        register(
            &self.shared,
            StoredReceipt {
                reference,
                producer: ProducerReceipt::Analytics(value),
                retained_bytes,
            },
            deadline,
            cancellation,
        )
        .await
    }
}

impl PortfolioFairValueInputPublisher {
    /// Retains one exact producer-issued portfolio revision.
    pub async fn publish(
        &self,
        revision: PortfolioRevision,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FairValueReceiptRegistration, FairValueInputAuthorityError> {
        ensure_publication_live(deadline, cancellation)?;
        let reference = portfolio_reference(&revision)?;
        let retained_bytes = revision
            .retained_bytes()
            .checked_add(reference.as_str().len())
            .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
        register(
            &self.shared,
            StoredReceipt {
                reference,
                producer: ProducerReceipt::Portfolio(revision),
                retained_bytes,
            },
            deadline,
            cancellation,
        )
        .await
    }
}

fn validate_live_selection(
    observation: &CommittedQualifiedMarketObservation,
    selection: MarketPriceSelection,
) -> Result<(), FairValueInputAuthorityError> {
    let valid = matches!(
        (observation.price(), selection),
        (
            QualifiedMarketPrice::Trade { .. },
            MarketPriceSelection::Trade
        ) | (
            QualifiedMarketPrice::Quote { bid: Some(_), .. },
            MarketPriceSelection::Bid
        ) | (
            QualifiedMarketPrice::Quote { ask: Some(_), .. },
            MarketPriceSelection::Ask
        )
    );
    if valid {
        Ok(())
    } else {
        Err(FairValueInputAuthorityError::InvalidReceipt)
    }
}
