//! Exact activation-to-source-metadata binding for Coinbase Direct products.

use market_squawk_adapter_coinbase::CoinbaseDirectConfig;
use market_squawk_domain::{
    AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, RevisionBoundPayloadEvidence, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ProviderBudgetPolicy, SourceMetadata,
};
use sha2::{Digest as _, Sha256};

use crate::ProviderActivationLease;
use crate::live_source::instruments::narrow_effective_interval;
use crate::provider_activation::CoinbaseDirectProductActivation;

use super::product::{CoinbaseDirectProductRuntimeError, ProductRuntimeSpec};

const METADATA_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/coinbase-direct-runtime-metadata/v1\0";

/// Builds exact metadata, authorization, coverage, and provider-budget bindings for one product.
pub(super) fn try_build_product_spec(
    slot: usize,
    lease: &ProviderActivationLease,
    product: CoinbaseDirectProductActivation,
    order_level: bool,
) -> Result<ProductRuntimeSpec, CoinbaseDirectProductRuntimeError> {
    let route = product.route().clone();
    let config = try_build_product_config(lease, &product, order_level)?;
    Ok(ProductRuntimeSpec::new(slot, config, route))
}

/// Builds the exact order-level metadata set used to register Direct durable publication before
/// any credential read or network access.
pub(crate) fn try_build_product_metadata_set(
    lease: &ProviderActivationLease,
    products: &[CoinbaseDirectProductActivation],
) -> Result<Vec<SourceMetadata>, CoinbaseDirectProductRuntimeError> {
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(products.len())
        .map_err(|_error| CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    for product in products {
        metadata.push(
            try_build_product_config(lease, product, true)?
                .metadata()
                .clone(),
        );
    }
    Ok(metadata)
}

fn try_build_product_config(
    lease: &ProviderActivationLease,
    product: &CoinbaseDirectProductActivation,
    order_level: bool,
) -> Result<CoinbaseDirectConfig, CoinbaseDirectProductRuntimeError> {
    let verification = lease
        .verification_evidence_digest()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let expires_at = lease
        .verification_expires_at()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let generation = lease
        .generation()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let budget = lease
        .provider_budget_policy()
        .cloned()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let authorization_account = budget
        .scope()
        .authorization_account()
        .cloned()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let lease_effective = EffectiveInterval::new(lease.authority_effective_at(), Some(expires_at))?;
    let effective =
        narrow_effective_interval(lease_effective, std::slice::from_ref(product.mapping()))
            .map_err(|_error| CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(authorization_account),
        ExactPayloadEvidence::from_content_digest(verification),
        effective,
    );
    let metadata_digest = direct_metadata_digest(lease, product, &budget, order_level)?;
    let source_id = product.source_id().clone();
    let revision = MetadataRevision::new(SourceIdentifier::try_from(format!(
        "coinbase-direct-r{}-g{}-{}",
        lease.capability_revision().get(),
        generation.get(),
        short_hex(metadata_digest.bytes())
    ))?);
    let revision_evidence = RevisionBoundPayloadEvidence::new(
        revision,
        ExactPayloadEvidence::from_content_digest(metadata_digest),
    );
    let terms = product.route().definition().execution_terms();
    let config = if order_level {
        CoinbaseDirectConfig::try_new_order_level(
            source_id,
            revision_evidence,
            authorization,
            ExactPayloadEvidence::from_content_digest(metadata_digest),
            effective,
            product.mapping().clone(),
            terms,
            *product.freshness(),
            budget,
            product.limits(),
        )?
    } else {
        CoinbaseDirectConfig::try_new(
            source_id,
            revision_evidence,
            authorization,
            ExactPayloadEvidence::from_content_digest(metadata_digest),
            effective,
            product.mapping().clone(),
            terms,
            *product.freshness(),
            budget,
            product.limits(),
        )?
    };
    Ok(config)
}

fn direct_metadata_digest(
    lease: &ProviderActivationLease,
    product: &CoinbaseDirectProductActivation,
    budget: &ProviderBudgetPolicy,
    order_level: bool,
) -> Result<EvidenceDigest, CoinbaseDirectProductRuntimeError> {
    let verification = lease
        .verification_evidence_digest()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let expires = lease
        .verification_expires_at()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let generation = lease
        .generation()
        .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?;
    let route = product.route();
    let terms = route.definition().execution_terms();
    let freshness = *product.freshness();
    let limits = product.limits();
    let websocket = limits.websocket();
    let book = limits.book();
    let mut hasher = Sha256::new();
    hasher.update(METADATA_EVIDENCE_DOMAIN);
    hasher.update([u8::from(order_level)]);
    hash_digest(&mut hasher, lease.capability_digest());
    hash_digest(&mut hasher, lease.rights_decision_digest());
    hash_digest(&mut hasher, lease.public_configuration_digest());
    hash_digest(&mut hasher, verification);
    hasher.update(lease.capability_revision().get().to_be_bytes());
    hasher.update(generation.get().to_be_bytes());
    hasher.update(lease.authority_effective_at().unix_nanos().to_be_bytes());
    hasher.update(expires.unix_nanos().to_be_bytes());
    hash_field(
        &mut hasher,
        product.product().as_source_identifier().as_str().as_bytes(),
    )?;
    hash_field(&mut hasher, product.source_id().as_str().as_bytes())?;
    let native_identity = product.mapping().instrument_attestation();
    hash_digest(&mut hasher, native_identity.provider_identity_digest());
    hash_digest(&mut hasher, native_identity.reference_digest());
    hash_digest(&mut hasher, native_identity.definition_revision_digest());
    hasher.update(
        native_identity
            .definition_published_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    hasher.update(
        native_identity
            .identity_observed_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    hasher.update(native_identity.valid_from().unix_nanos().to_be_bytes());
    hasher.update(
        native_identity
            .valid_until()
            .map_or(i64::MAX, Timestamp::unix_nanos)
            .to_be_bytes(),
    );
    hasher.update(native_identity.selected_at().unix_nanos().to_be_bytes());
    hash_field(&mut hasher, route.route().venue().as_str().as_bytes())?;
    hasher.update(route.route().instrument().as_uuid().as_bytes());
    hasher.update(terms.definition_revision().get().to_be_bytes());
    hash_field(
        &mut hasher,
        terms.price_tick().as_decimal().to_string().as_bytes(),
    )?;
    hash_field(
        &mut hasher,
        terms.lot_size().as_decimal().to_string().as_bytes(),
    )?;
    hash_field(&mut hasher, terms.quote_currency().as_str().as_bytes())?;
    hash_field(
        &mut hasher,
        terms.settlement_denomination().to_string().as_bytes(),
    )?;
    hash_field(
        &mut hasher,
        terms.contract_multiplier().to_string().as_bytes(),
    )?;
    for value in [
        freshness.max_connection_idle_nanos(),
        freshness.max_transport_age_nanos(),
        freshness.max_source_age_nanos(),
        freshness.max_market_age_nanos(),
        freshness.max_clock_skew_nanos(),
        u64::try_from(websocket.max_frame_bytes())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(websocket.connect_timeout().as_nanos())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(websocket.io_timeout().as_nanos())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        limits.max_snapshot_bytes(),
        u64::try_from(limits.max_snapshot_segments())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(limits.product_refresh_interval().as_nanos())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(book.max_orders())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(book.max_price_levels())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(book.max_queue_events())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(book.max_queue_bytes())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
        u64::try_from(book.published_depth())
            .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?,
    ] {
        hasher.update(value.to_be_bytes());
    }
    let budget_bytes = serde_json::to_vec(budget)
        .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?;
    hash_field(&mut hasher, &budget_bytes)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn hash_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    let tag = match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1_u8,
        DigestAlgorithm::Blake3 => 2_u8,
    };
    hasher.update([tag]);
    hasher.update(digest.bytes());
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let length = u64::try_from(bytes.len())
        .map_err(|_error| CoinbaseDirectProductRuntimeError::EvidenceEncoding)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn short_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(24);
    for byte in bytes.into_iter().take(12) {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
