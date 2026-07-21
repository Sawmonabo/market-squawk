use std::sync::Arc;

use bytes::Bytes;
use market_squawk_domain::{
    CaptureResidentGenerationLease, CaptureResidentToken, ConnectionGeneration, IntegrityRule,
    RuleVersion, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ControlFrameKind, DecodeOutcome, DecodedControlFrame,
    DecoderEvidence, SessionId, TransportFrameKind, ValidatedSessionDecodeOutcome,
};
use static_assertions::assert_not_impl_any;

use crate::common::{TestResult, direct_metadata, source_identifier};

assert_not_impl_any!(ValidatedSessionDecodeOutcome: Clone, serde::Serialize, serde::de::DeserializeOwned);

#[derive(Debug)]
struct ResidentToken;

impl CaptureResidentToken for ResidentToken {}

#[test]
fn captured_subscription_ack_is_control_and_never_current_market_data() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        direct_metadata("source-a", "revision-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut capture_control, mut capture_admission, _degradation) = capabilities.into_parts();
    capture_control.mark_healthy()?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let frame = frames.try_frame(
        TransportFrameKind::Text,
        Bytes::from_static(br#"{"event":"subscribe","status":"success"}"#),
    )?;
    capture_admission.preflight(&frame)?;
    let receipt = capture_admission.issue_after_enqueue(
        &frame,
        CaptureResidentGenerationLease::new(Arc::new(ResidentToken)),
    )?;
    let validated_frame = session.validate_live_frame(&frame)?;
    let evidence = DecoderEvidence::from_validated_frame(
        &validated_frame,
        IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?),
    );
    let outcome = DecodeOutcome::Control(DecodedControlFrame::new(
        evidence,
        ControlFrameKind::SubscriptionAcknowledgement,
        None,
    ));
    assert!(outcome.retained_bytes()? > 0);

    let validated_session = registry.validate_session(&session, frame.received_at())?;
    let validated = validated_session.validate_decode_outcome_owned(outcome, receipt)?;
    let ValidatedSessionDecodeOutcome::Control(control) = validated else {
        return Err("subscription acknowledgement became market data".into());
    };
    assert_eq!(
        control.kind(),
        ControlFrameKind::SubscriptionAcknowledgement
    );
    Ok(())
}
