#[cfg(test)]
mod stream_key_tests {
    use std::mem::size_of;
    use std::collections::HashSet;

    use market_squawk_domain::{
        InstrumentId, ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, VenueId,
    };

    use super::{
        CurrentBatchKey, CurrentProviderObservation, CurrentStreamKey, RegistryError,
        current_authority_shared_allocation_charge, current_routed_batch_retained_bytes,
    };

    fn key(
        source: &str,
        venue: &str,
        instrument: &str,
        product: &str,
        channel: &str,
    ) -> Result<CurrentStreamKey, Box<dyn std::error::Error>> {
        Ok(CurrentStreamKey {
            source_id: SourceId::try_from(source)?,
            venue: VenueId::try_from(venue)?,
            instrument: instrument.parse::<InstrumentId>()?,
            provider_product: ProviderProduct::new(SourceIdentifier::try_from(product)?),
            provider_channel: ProviderChannel::new(SourceIdentifier::try_from(channel)?),
        })
    }

    #[test]
    fn hash_identity_separates_all_five_dimensions() -> Result<(), Box<dyn std::error::Error>> {
        let first_instrument = "018f0000-0000-7000-8000-000000000001";
        let second_instrument = "018f0000-0000-7000-8000-000000000002";
        let keys = HashSet::from([
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-secondary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "coinbase", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", second_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "XBT/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "level3")?,
        ]);

        assert_eq!(keys.len(), 6);
        Ok(())
    }

    #[test]
    fn routed_batch_charges_shared_authority_and_frame_allocations_once()
    -> Result<(), Box<dyn std::error::Error>> {
        const PER_OBSERVATION_DYNAMIC: usize = 137;
        const SECOND_OBSERVATION_DYNAMIC: usize = 211;
        let one = current_routed_batch_retained_bytes(53, 1, PER_OBSERVATION_DYNAMIC, 97, 101)?;
        let two = current_routed_batch_retained_bytes(
            53,
            2,
            PER_OBSERVATION_DYNAMIC + SECOND_OBSERVATION_DYNAMIC,
            97,
            101,
        )?;

        assert_eq!(
            two.checked_sub(one),
            Some(size_of::<CurrentProviderObservation>() + SECOND_OBSERVATION_DYNAMIC)
        );
        Ok(())
    }

    #[test]
    fn batch_key_charges_retained_venue_capacity() -> Result<(), Box<dyn std::error::Error>> {
        let mut venue = String::with_capacity(VenueId::MAX_LENGTH);
        venue.push('x');
        let key = CurrentBatchKey {
            venue: VenueId::try_from(venue)?,
            instrument: "018f0000-0000-7000-8000-000000000001".parse()?,
        };

        assert!(key.dynamic_retained_bytes() >= VenueId::MAX_LENGTH);
        let without_key = current_routed_batch_retained_bytes(0, 1, 0, 0, 0)?;
        let with_key =
            current_routed_batch_retained_bytes(key.dynamic_retained_bytes(), 1, 0, 0, 0)?;
        assert_eq!(
            with_key.checked_sub(without_key),
            Some(key.dynamic_retained_bytes())
        );
        Ok(())
    }

    #[test]
    fn authority_charge_adds_budget_allocation_exactly_once() -> Result<(), RegistryError> {
        const SESSION: usize = 101;
        const CAPTURE: usize = 103;
        const BUDGET: usize = 107;
        let without_budget = current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, 0)?;
        let with_budget =
            current_authority_shared_allocation_charge(SESSION, CAPTURE, BUDGET, 0)?;

        assert_eq!(with_budget.checked_sub(without_budget), Some(BUDGET));
        Ok(())
    }

    #[test]
    fn authority_charge_adds_clock_allocation_exactly_once() -> Result<(), RegistryError> {
        const SESSION: usize = 101;
        const CAPTURE: usize = 103;
        const CLOCK: usize = 109;
        let without_clock = current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, 0)?;
        let with_clock =
            current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, CLOCK)?;

        assert_eq!(with_clock.checked_sub(without_clock), Some(CLOCK));
        Ok(())
    }
}

