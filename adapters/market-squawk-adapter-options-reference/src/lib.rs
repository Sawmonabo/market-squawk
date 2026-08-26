//! Bounded provider-native option reference contracts for OCC and Cboe.
//!
//! This crate builds exact official reference-file requests, streams production responses into a
//! capability-scoped content-addressed raw store and decodes selected publications into sealed
//! disk-backed query generations. It does not create
//! canonical [`market_squawk_domain::InstrumentId`] values. A syntactically valid OSI symbol or a
//! row's presence in a publication is reference evidence, not proof of a current quote,
//! consolidated coverage, tradability, standard multiplier, or adjusted-contract economics.

#![deny(missing_docs)]

mod cboe;
mod identity;
mod occ;
mod publication;
mod service;
mod spool;
mod store;
mod transport;

pub use cboe::{
    CBOE_ALL_SERIES_MAX_BYTES, CBOE_ALL_SERIES_MAX_RECORDS, CboeAllSeriesCsvSchema,
    CboeAllSeriesParseReceipt, CboeAllSeriesParser, CboeListingEvidence, CboeParseError,
    CboeSeriesReference, CboeSeriesStatus, CboeSymbolId, CboeVenue,
};
pub use identity::{
    ExpirationResolution, MultiplierEvidence, OptionContractIdentity, OptionExpiration,
    OptionIdentityError, OptionStrike,
};
pub use occ::{
    OCC_DLP_MAX_BYTES, OCC_DLP_MAX_RECORDS, OCC_MEMO_MAX_BYTES, OCC_MEMO_MAX_RECORDS,
    OccDlpParseReceipt, OccDlpParser, OccDlpPresence, OccDlpProductReference, OccDlpSchema,
    OccExchangeCode, OccExchangeListingEvidence, OccMemoCategory, OccMemoCsvSchema,
    OccMemoDiscovery, OccMemoInterpretation, OccMemoParseReceipt, OccMemoParser, OccParseError,
    OccPositionLimit, OccProductType,
};
pub use publication::{
    CatalogConflict, CatalogConflictKind, CatalogCounts, HttpLastModifiedEvidence,
    ObjectClockEvidence, PageTerminalState, PublicationCatalog, PublicationCompleteness,
    PublicationError, PublicationLimits, PublicationRequest, ReferenceConditionalPriorEvidence,
    ReferenceConditionalValidatorEvidence, ReferenceNativeSchemaIdentity, ReferenceObjectContext,
    ReferenceOfficialRequestEvidence, ReferencePageReceipt, ReferenceProvider,
    ReferenceRequestBodyEvidence, ReferenceRequestMethod, ReferenceResponseDisposition,
    ReferenceSurface, ReferenceTransportEvidence, SurfaceCompleteness,
};
pub use service::{
    CboeReferenceObjectDoctorEvidence, OccDlpDoctorState, OccDlpRepresentation,
    OccMemoAcquisitionDisposition, OccMemoAcquisitionState, OccMemoDocumentClosureEvidence,
    OccMemoRssDiscoveryEvidence, OptionsReferenceActivationReason, OptionsReferenceActivationState,
    OptionsReferenceCurrentnessDisposition, OptionsReferenceDoctorError,
    OptionsReferenceDoctorInput, OptionsReferenceDoctorReport, OptionsReferenceGenerationHealth,
    OptionsReferenceLocalFailure, OptionsReferenceObjectClockEvidence,
    OptionsReferenceQueryFailure, OptionsReferenceQueryFamily, OptionsReferenceQueryProbe,
    REQUIRED_CBOE_VENUES,
};
pub use spool::{
    CanonicalReferenceIdentityState, CboeContractReferenceView, CboeVenuePresenceView,
    OccProductReferenceView, ReferencePageBatch, ReferencePublicationSpool, ReferenceSpoolError,
    ReferenceSpoolLimits, ReferenceSpoolSealOutcome, RejectedReferenceGeneration,
    StagedReferenceGeneration,
};
pub use store::{
    AuthenticatedReferencePage, AuthenticatedReferenceQuery, ReferenceArtifactStore,
    ReferenceCanonicalExportCursor, ReferenceCanonicalExportFamily, ReferenceGeneration,
    ReferenceGenerationObjectEvidence, ReferenceGenerationReceipt, ReferenceQueryCoordinate,
    ReferenceQueryEvidence, ReferenceRecoveryFailure, ReferenceRecoveryOutcome,
    ReferenceRecoveryRejection, ReferenceStoreError, RejectedReferenceGenerationReceipt,
    SealedReferenceRawObject,
};
pub use transport::{
    CBOE_OPTIONS_REFERENCE_PROVIDER_ID, CBOE_OPTIONS_REFERENCE_SOURCE_ID, CboeSchemaFreeze,
    ConditionalCacheRequest, HttpCacheEvidence, OCC_MEMO_DOCUMENT_MAX_BYTES,
    OCC_OPTIONS_REFERENCE_PROVIDER_ID, OCC_OPTIONS_REFERENCE_SOURCE_ID,
    OPTIONS_REFERENCE_APPLICATION_MAX_CONCURRENT,
    OPTIONS_REFERENCE_APPLICATION_REQUESTS_PER_MINUTE, OPTIONS_REFERENCE_APPLICATION_WINDOW_NANOS,
    OPTIONS_REFERENCE_MINIMUM_CONNECT_TIMEOUT_NANOS, OPTIONS_REFERENCE_MINIMUM_READ_TIMEOUT_NANOS,
    OPTIONS_REFERENCE_MINIMUM_TOTAL_TIMEOUT_NANOS, OfficialPublicationPlan,
    OfficialPublicationPolicy, OfficialReferenceRequest, OfficialReferenceStreamingClient,
    ReferenceCancellation, ReferenceFetchControl, ReferenceHeaderValue, ReferenceHttpReceipt,
    ReferenceNotModifiedReceipt, ReferenceTransportError, RetryAfterEvidence,
    SelectedReferenceDecoder, StreamedReferenceObject, StreamingReferenceFetchOutcome,
    StrictReferenceParseReceipt, StrictUninterpretedMemoDocumentReceipt,
    options_reference_application_budget_policy, options_reference_endpoint_policy,
    options_reference_provider_rate_declaration,
};

#[cfg(all(test, unix))]
use transport::{
    OfficialReferenceSource, ReferenceFetchOutcome, ReferenceHttpExecutor, ReferenceHttpRequest,
    ReferenceHttpResponse, RetrievedReferenceObject,
};

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::cell::RefCell;
    use std::error::Error;
    #[cfg(unix)]
    use std::io::{Seek as _, SeekFrom, Write as _};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_domain::{
        AvailabilityEvidence, DigestAlgorithm, EvidenceDigest, OptionKind, SourceIdentifier,
        Timestamp,
    };
    use sha2::{Digest, Sha256};

    use super::*;

    #[cfg(unix)]
    #[derive(Debug)]
    struct SingleResponseExecutor {
        expected_locator: String,
        response: RefCell<Option<ReferenceHttpResponse>>,
    }

    #[cfg(unix)]
    impl ReferenceHttpExecutor for SingleResponseExecutor {
        fn execute(
            &self,
            request: &ReferenceHttpRequest,
            control: &ReferenceFetchControl,
        ) -> Result<ReferenceHttpResponse, ReferenceTransportError> {
            let _remaining = control.remaining()?;
            assert_eq!(request.method(), ReferenceRequestMethod::Get);
            assert_eq!(request.locator().as_str(), self.expected_locator);
            assert!(request.accept_encoding_identity());
            assert_eq!(request.maximum_redirects(), 0);
            assert_eq!(request.if_none_match(), None);
            assert_eq!(request.if_modified_since(), None);
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(ReferenceTransportError::executor_failed)
        }
    }

    #[test]
    fn current_cboe_identity_keeps_underlying_and_matching_unit_provider_native()
    -> Result<(), Box<dyn Error>> {
        let bytes = b"Cboe Symbol,OSI Symbol,Underlying,Matching Unit,Closing Only\n000u56,ZVZZT 990101C00005000,SPY,25,False\n";
        let context = object_context(
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::C1,
            },
            "cboe-c1-object",
            "text/csv",
            CboeAllSeriesCsvSchema::DailyAllSeriesV1.native_schema(),
            bytes,
        )?;
        let parser = CboeAllSeriesParser::try_new(
            CboeVenue::C1,
            CboeAllSeriesCsvSchema::DailyAllSeriesV1,
            context,
        )?;
        let mut records = Vec::new();
        let receipt = parser.parse(bytes, |record| {
            records.push(record);
            Ok(())
        })?;
        assert_eq!(receipt.returned_records(), 1);
        let record = records
            .first()
            .ok_or_else(|| std::io::Error::other("decoded Cboe record is absent"))?;
        assert_eq!(record.cboe_symbol_id().as_str(), "000u56");
        assert_eq!(record.contract().root(), "ZVZZT");
        assert_eq!(record.underlying().as_str(), "SPY");
        assert_eq!(record.unit().get(), 25);
        assert_eq!(record.contract().expiration().year_two_digits(), 99);
        assert_eq!(record.contract().expiration().calendar_date(), None);
        assert_eq!(record.contract().strike().thousandths(), 5_000);
        assert_eq!(record.contract().kind(), OptionKind::Call);
        assert_eq!(record.contract().multiplier().multiplier(), None);
        Ok(())
    }

    #[test]
    fn occ_selected_daily_and_xml_are_distinct_wires_with_equal_semantics()
    -> Result<(), Box<dyn Error>> {
        let name = "American Airlines Group Inc";
        let selected = format!(
            "{:<6}\t{:<6}\t{:<50}\tABCIPX\t25000000\tEF\t\r\n",
            "1AAL", "AAL", name
        );
        let daily = format!("1AAL\tAAL\t{name}\tABCIPX\t000025000000\tEF\r\n");
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<results>\n  <record>\n    <optionSymbol>1AAL</optionSymbol>\n    <underlyingSymbol>AAL</underlyingSymbol>\n    <symbolName><![CDATA[{name}]]></symbolName>\n    <positionLimit>25000000</positionLimit>\n    <onnProductType>EF</onnProductType>\n    <exchanges>ABCIPX</exchanges>\n  </record>\n</results>"
        );
        let fixtures = [
            (
                ReferenceSurface::OccDlpSelectedText,
                OccDlpSchema::SelectedTextV1,
                selected.as_bytes(),
            ),
            (
                ReferenceSurface::OccDlpDailyText,
                OccDlpSchema::DailyTextV1,
                daily.as_bytes(),
            ),
            (
                ReferenceSurface::OccDlpDailyXml,
                OccDlpSchema::DailyXmlV1,
                xml.as_bytes(),
            ),
        ];
        let mut decoded = Vec::new();
        for (index, (surface, schema, bytes)) in fixtures.into_iter().enumerate() {
            let context = object_context(
                surface,
                &format!("occ-dlp-object-{index}"),
                schema.media_type(),
                schema.native_schema(),
                bytes,
            )?;
            let mut records = Vec::new();
            let receipt = OccDlpParser::try_new(context)?.parse(bytes, |record| {
                records.push(record);
                Ok(())
            })?;
            assert_eq!(receipt.returned_records(), 1);
            decoded.push(
                records
                    .pop()
                    .ok_or_else(|| std::io::Error::other("decoded OCC record is absent"))?,
            );
        }
        let expected_limit = std::num::NonZeroU64::new(25_000_000)
            .ok_or_else(|| std::io::Error::other("nonzero fixture limit is invalid"))?;
        for record in &decoded {
            assert_eq!(record.options_symbol().as_str(), "1AAL");
            assert_eq!(record.underlying_symbol().as_str(), "AAL");
            assert_eq!(record.symbol_name(), name);
            assert_eq!(record.product_type(), OccProductType::EquityFlex);
            assert_eq!(
                record.position_limit(),
                OccPositionLimit::EquityReported(expected_limit)
            );
            assert_eq!(
                record.exchange_listing_evidence(),
                OccExchangeListingEvidence::Reported
            );
        }

        let sentinel = format!(
            "{:<6}\t{:<6}\t{:<50}\t \t25000000\tEF\t\r\n",
            "1AAL", "AAL", name
        );
        let sentinel_context = object_context(
            ReferenceSurface::OccDlpSelectedText,
            "occ-dlp-selected-sentinel",
            OccDlpSchema::SelectedTextV1.media_type(),
            OccDlpSchema::SelectedTextV1.native_schema(),
            sentinel.as_bytes(),
        )?;
        let mut sentinel_records = Vec::new();
        OccDlpParser::try_new(sentinel_context)?.parse(sentinel.as_bytes(), |record| {
            sentinel_records.push(record);
            Ok(())
        })?;
        assert_eq!(sentinel_records[0].trading_exchanges(), &[]);
        assert_eq!(
            sentinel_records[0].exchange_listing_evidence(),
            OccExchangeListingEvidence::NotReportedInSelectedDirectory
        );

        let cross_schema_context = object_context(
            ReferenceSurface::OccDlpSelectedText,
            "occ-dlp-cross-schema",
            OccDlpSchema::DailyTextV1.media_type(),
            OccDlpSchema::DailyTextV1.native_schema(),
            daily.as_bytes(),
        )?;
        assert!(matches!(
            OccDlpParser::try_new(cross_schema_context),
            Err(OccParseError::InvalidContext)
        ));
        let malformed_lf = daily.replace("\r\n", "\n");
        let malformed_context = object_context(
            ReferenceSurface::OccDlpDailyText,
            "occ-dlp-malformed-framing",
            OccDlpSchema::DailyTextV1.media_type(),
            OccDlpSchema::DailyTextV1.native_schema(),
            malformed_lf.as_bytes(),
        )?;
        assert!(matches!(
            OccDlpParser::try_new(malformed_context)?.parse(malformed_lf.as_bytes(), |_| Ok(())),
            Err(OccParseError::IncompletePublication)
        ));
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn official_source_mock_proves_complete_core_restart_and_typed_query()
    -> Result<(), Box<dyn Error>> {
        let cboe_bytes = b"Cboe Symbol,OSI Symbol,Underlying,Matching Unit,Closing Only\n000u56,ZVZZT 990101C00005000,SPY,25,False\n";
        let occ_bytes = format!(
            "{:<6}\t{:<6}\t{:<50}\tABCIPX\t25000000\tEF\t\r\n",
            "1AAL", "AAL", "American Airlines Group Inc"
        );
        let received_at = crate::transport::trusted_timestamp()?;
        let request = PublicationRequest::try_new(
            SourceIdentifier::try_from("options-reference-http-mock")?,
            received_at.checked_sub_nanos(1_000_000_000)?,
            received_at.checked_add_nanos(60_000_000_000)?,
            vec![
                ReferenceSurface::CboeAllSeries {
                    venue: CboeVenue::C1,
                },
                ReferenceSurface::CboeAllSeries {
                    venue: CboeVenue::Bzx,
                },
                ReferenceSurface::CboeAllSeries {
                    venue: CboeVenue::C2,
                },
                ReferenceSurface::CboeAllSeries {
                    venue: CboeVenue::Edgx,
                },
                ReferenceSurface::OccDlpSelectedText,
            ],
            PublicationLimits::try_new(5, 5, 5 * 1024 * 1024, 10, 1)?,
        )?;
        let plan = OfficialPublicationPlan::try_new(
            request,
            OfficialPublicationPolicy::new(
                CboeSchemaFreeze::single(CboeAllSeriesCsvSchema::DailyAllSeriesV1),
                OccMemoCsvSchema::CategoryV1,
                None,
            ),
        )?;
        let artifacts = tempfile::tempdir()?;
        let store_capability = Dir::open_ambient_dir(artifacts.path(), ambient_authority())?;
        let store = ReferenceArtifactStore::open(store_capability)?;
        let storage_activation = store.activation_storage_probe()?;
        let publication_control = ReferenceFetchControl::for_duration(
            Duration::from_secs(60),
            ReferenceCancellation::new(),
        )?;
        let mut spool = store.begin_publication(
            plan.publication_request().clone(),
            publication_control.clone(),
            ReferenceSpoolLimits::try_new(64 * 1024 * 1024, 1024 * 1024, 1, 4)?,
        )?;
        let mut raw_objects = Vec::new();
        let mut c1_evidence = None;
        let mut cboe_query_probe = None;
        let mut occ_query_probe = None;
        for official_request in plan.requests() {
            let (body, content_type, disposition) = match official_request.surface() {
                ReferenceSurface::CboeAllSeries { venue } => {
                    let prefix = match venue {
                        CboeVenue::C1 => "cone",
                        CboeVenue::Bzx => "opt",
                        CboeVenue::C2 => "ctwo",
                        CboeVenue::Edgx => "exo",
                    };
                    (
                        cboe_bytes.to_vec(),
                        "text/csv; charset=utf-8",
                        format!(
                            "attachment; filename={prefix}_listed_symbol_reference_2026_08_14_01_13_36.csv"
                        ),
                    )
                }
                ReferenceSurface::OccDlpSelectedText => (
                    occ_bytes.as_bytes().to_vec(),
                    "text/plain; charset=utf-8",
                    "attachment; filename=dlpDownload.txt".to_owned(),
                ),
                _ => return Err(std::io::Error::other("unexpected core surface").into()),
            };
            let locator = official_request.locator().as_str();
            let response = ReferenceHttpResponse::try_new(
                200,
                SourceIdentifier::try_from(locator)?,
                Vec::new(),
                Some(ReferenceHeaderValue::try_new(content_type)?),
                Some(ReferenceHeaderValue::try_new(disposition)?),
                Some(u64::try_from(body.len())?),
                None,
                HttpCacheEvidence::new(
                    Some(ReferenceHeaderValue::try_new("\"fixture-v1\"")?),
                    Some(ReferenceHeaderValue::try_new(
                        "Fri, 14 Aug 2026 05:13:57 GMT",
                    )?),
                ),
                None,
                received_at,
                true,
                body,
            )?;
            let source = OfficialReferenceSource::new(SingleResponseExecutor {
                expected_locator: locator.to_owned(),
                response: RefCell::new(Some(response)),
            });
            let ReferenceFetchOutcome::Modified(object) =
                source.fetch(official_request, &publication_control)?
            else {
                return Err(
                    std::io::Error::other("mock response was not admitted as modified").into(),
                );
            };
            raw_objects.push(store.seal_raw_object(&object)?);
            match official_request.surface() {
                ReferenceSurface::CboeAllSeries { venue } => {
                    let mut decoded = Vec::new();
                    let receipt = CboeAllSeriesParser::try_new(
                        *venue,
                        CboeAllSeriesCsvSchema::DailyAllSeriesV1,
                        object.context().clone(),
                    )?
                    .parse(object.bytes(), |record| {
                        decoded.push(record);
                        Ok(())
                    })?;
                    let record = decoded
                        .first()
                        .ok_or_else(|| std::io::Error::other("Cboe fixture row is absent"))?;
                    if *venue == CboeVenue::C1 {
                        c1_evidence = Some(record.record_id().clone());
                        cboe_query_probe = Some(record.clone());
                    }
                    let mut page = spool.begin_page(official_request.surface().clone())?;
                    page.record_cboe(record)?;
                    page.finish(&receipt.page_receipt())?;
                }
                ReferenceSurface::OccDlpSelectedText => {
                    let mut decoded = Vec::new();
                    let receipt = OccDlpParser::try_new(object.context().clone())?.parse(
                        object.bytes(),
                        |record| {
                            decoded.push(record);
                            Ok(())
                        },
                    )?;
                    let record = decoded
                        .first()
                        .ok_or_else(|| std::io::Error::other("OCC fixture row is absent"))?;
                    occ_query_probe = Some(record.clone());
                    let mut page = spool.begin_page(official_request.surface().clone())?;
                    page.record_occ_product(record)?;
                    page.finish(&receipt.page_receipt())?;
                }
                _ => return Err(std::io::Error::other("unexpected core surface").into()),
            }
        }
        let ReferenceSpoolSealOutcome::Complete(staged) = spool.seal()? else {
            return Err(std::io::Error::other("conflict-free fixture was rejected").into());
        };
        let generation_receipt = store.publish_generation(staged, &raw_objects)?;
        let generation = store.open_generation(&generation_receipt)?;
        let symbol = CboeSymbolId::try_from_provider("000u56")?;
        let query = generation.cboe_by_symbol(&symbol)?;
        assert_eq!(
            query.evidence().database_digest(),
            generation_receipt.database_digest()
        );
        let view = query
            .value()
            .ok_or_else(|| std::io::Error::other("sealed Cboe query row is absent"))?;
        assert_eq!(view.underlying().as_str(), "SPY");
        assert_eq!(view.venues().len(), 4);
        assert!(
            view.venues()
                .iter()
                .all(|venue| venue.matching_unit() == 25)
        );
        assert_eq!(
            view.venues()[0].evidence(),
            c1_evidence
                .as_ref()
                .ok_or_else(|| std::io::Error::other("C1 evidence is absent"))?
        );
        drop(generation);
        let recovered = store.repair_active()?;
        assert!(recovered.generation().is_some());
        assert!(recovered.rejected().is_empty());
        let memo = OccMemoAcquisitionState::not_selected();
        let cboe_query_probe = cboe_query_probe
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Cboe query witness is absent"))?;
        let occ_query_probe = occ_query_probe
            .as_ref()
            .ok_or_else(|| std::io::Error::other("OCC query witness is absent"))?;
        let doctor = OptionsReferenceDoctorReport::evaluate(OptionsReferenceDoctorInput::new(
            &recovered,
            Some(&storage_activation),
            Some(OptionsReferenceQueryProbe::new(
                cboe_query_probe,
                occ_query_probe,
            )),
            &memo,
        ))?;
        assert!(doctor.core_reference_verified());
        assert_eq!(
            doctor.activation(),
            OptionsReferenceActivationState::Available
        );

        let generation = store.open_generation(&generation_receipt)?;
        let raw_path = artifacts
            .path()
            .join("raw")
            .join(raw_objects[0].storage_name().as_str());
        let mut permissions = std::fs::metadata(&raw_path)?.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(&raw_path, permissions)?;
        let mut raw_file = std::fs::OpenOptions::new().write(true).open(raw_path)?;
        raw_file.seek(SeekFrom::Start(0))?;
        raw_file.write_all(b"X")?;
        raw_file.sync_all()?;
        assert!(matches!(
            generation.cboe_by_symbol(&symbol),
            Err(ReferenceStoreError::ObjectCorrupt)
        ));
        Ok(())
    }

    fn object_context(
        surface: ReferenceSurface,
        object_id: &str,
        media_type: &str,
        native_schema: &str,
        bytes: &[u8],
    ) -> Result<ReferenceObjectContext, Box<dyn Error>> {
        let provider = surface.provider();
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let clocks = ObjectClockEvidence::try_new(
            None,
            None,
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(5)),
            Timestamp::from_unix_nanos(6),
            1,
        )?;
        let locator = SourceIdentifier::try_from("https://provider.example/reference")?;
        let canonical_media_type = SourceIdentifier::try_from(media_type)?;
        let native_schema_identity = crate::transport::native_schema_identity(native_schema)?;
        let (source_id, provider_id) = match provider {
            ReferenceProvider::Occ => (
                OCC_OPTIONS_REFERENCE_SOURCE_ID,
                OCC_OPTIONS_REFERENCE_PROVIDER_ID,
            ),
            ReferenceProvider::Cboe => (
                CBOE_OPTIONS_REFERENCE_SOURCE_ID,
                CBOE_OPTIONS_REFERENCE_PROVIDER_ID,
            ),
        };
        let transport = ReferenceTransportEvidence::try_modified(
            ReferenceOfficialRequestEvidence::try_new(
                SourceIdentifier::try_from(source_id)?,
                SourceIdentifier::try_from(provider_id)?,
                crate::transport::injected_source_contract_digest(provider),
                provider,
                surface.clone(),
                SourceIdentifier::try_from("synthetic-options-reference-request")?,
                locator.clone(),
                media_type,
                "market-squawk-test",
                u64::try_from(bytes.len())?,
                0,
                2,
                2,
                2,
                2,
                Timestamp::from_unix_nanos(4),
                Timestamp::from_unix_nanos(7),
                None,
                native_schema_identity.clone(),
                None,
            )?,
            200,
            locator.clone(),
            Vec::new(),
            media_type,
            None,
            None,
            Some(u64::try_from(bytes.len())?),
            None,
            None,
            Timestamp::from_unix_nanos(6),
            Timestamp::from_unix_nanos(6),
            1,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            u64::try_from(bytes.len())?,
            canonical_media_type.clone(),
            native_schema_identity.clone(),
        )?;
        Ok(ReferenceObjectContext::try_new(
            provider,
            surface,
            SourceIdentifier::try_from(object_id)?,
            locator.clone(),
            locator,
            canonical_media_type,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            u64::try_from(bytes.len())?,
            native_schema_identity,
            clocks,
            None,
            None,
            None,
            transport,
        )?)
    }
}
