//! Bounded provider-native option reference contracts for OCC and Cboe.
//!
//! This crate builds exact official reference-file requests, streams production responses into a
//! shared logical raw-object authority, and exports exact provider-native rows plus ambiguity
//! evidence for caller-owned composition. It does not create durable generations, PIT reads, or
//! canonical [`market_squawk_domain::InstrumentId`] values. A syntactically valid OSI symbol or a
//! row's presence in a publication is reference evidence, not proof of a current quote,
//! consolidated coverage, tradability, standard multiplier, or adjusted-contract economics.

#![deny(missing_docs)]

mod cboe;
mod export;
mod identity;
mod occ;
mod payload;
mod publication;
mod transport;

pub use cboe::{
    CBOE_ALL_SERIES_MAX_BYTES, CBOE_ALL_SERIES_MAX_RECORDS, CboeAllSeriesCsvSchema,
    CboeAllSeriesParseReceipt, CboeAllSeriesParser, CboeListingEvidence, CboeParseError,
    CboeSeriesReference, CboeSeriesStatus, CboeSymbolId, CboeVenue,
};
pub use export::{
    OptionsReferenceAliasDisposition, OptionsReferenceCurrentnessDisposition,
    OptionsReferenceIdentityDisposition, OptionsReferenceValidityDisposition,
    ReferenceAliasAssertion, ReferenceAliasKey, ReferenceAliasResolution,
    ReferenceAliasResolutionState, ReferenceAliasSortKey, ReferenceAliasTarget, ReferenceConflict,
    ReferenceConflictKind, ReferenceConflictReconciler, ReferenceConflictReconciliationReceipt,
    ReferenceExportError, ReferenceExportRecord,
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
    HttpLastModifiedEvidence, ObjectClockEvidence, PageTerminalState, PublicationError,
    PublicationLimits, PublicationRequest, ReferenceConditionalPriorEvidence,
    ReferenceConditionalValidatorEvidence, ReferenceNativeSchemaIdentity, ReferenceObjectContext,
    ReferenceOfficialRequestEvidence, ReferencePageReceipt, ReferenceProvider,
    ReferenceRequestAccountingReceipt, ReferenceRequestBodyEvidence, ReferenceRequestBudget,
    ReferenceRequestMethod, ReferenceResponseDisposition, ReferenceSurface,
    ReferenceTransportEvidence,
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
    PendingReferenceTypedHandoff, PendingUninterpretedMemoHandoff, ReferenceCancellation,
    ReferenceFetchControl, ReferenceHeaderValue, ReferenceHttpReceipt, ReferenceNotModifiedReceipt,
    ReferenceTransportError, ReferenceTypedHandoff, ReferenceUninterpretedMemoHandoff,
    RetryAfterEvidence, SelectedReferenceDecoder, StreamedReferenceObject,
    StreamingReferenceFetchOutcome, StrictReferenceParseReceipt,
    StrictUninterpretedMemoDocumentReceipt, options_reference_application_budget_policy,
    options_reference_endpoint_policy, options_reference_provider_rate_declaration,
};

#[cfg(all(test, unix))]
use transport::{
    OfficialReferenceSource, ReferenceFetchOutcome, ReferenceHttpExecutor, ReferenceHttpRequest,
    ReferenceHttpResponse,
};

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::cell::RefCell;
    use std::error::Error;
    #[cfg(unix)]
    use std::io::Read as _;
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use std::time::Duration;

    use market_squawk_domain::{
        AvailabilityEvidence, DigestAlgorithm, EvidenceDigest, OptionKind, SourceIdentifier,
        Timestamp,
    };
    #[cfg(unix)]
    use market_squawk_platform::{
        LocalPaths, ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    };
    use sha2::{Digest, Sha256};

    use super::*;

    #[cfg(unix)]
    struct TestRawObjectControl<'a> {
        fetch: &'a ReferenceFetchControl,
        wall_deadline: Timestamp,
    }

    #[cfg(unix)]
    impl ResearchObjectControl for TestRawObjectControl<'_> {
        fn checkpoint(
            &self,
            _point: ResearchObjectControlPoint,
        ) -> Result<(), ResearchObjectControlError> {
            self.fetch.ensure_open().map_err(|error| match error {
                ReferenceTransportError::Cancelled => ResearchObjectControlError::Cancelled,
                ReferenceTransportError::DeadlineExceeded => {
                    ResearchObjectControlError::DeadlineExceeded
                }
                _ => ResearchObjectControlError::Unavailable,
            })?;
            match crate::transport::trusted_timestamp() {
                Ok(now) if now <= self.wall_deadline => Ok(()),
                Ok(_) => Err(ResearchObjectControlError::DeadlineExceeded),
                Err(_) => Err(ResearchObjectControlError::Unavailable),
            }
        }
    }

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
        let receipt = parser.parse(bytes.as_slice(), |record| {
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

    #[tokio::test(flavor = "current_thread")]
    #[cfg(unix)]
    async fn official_source_mock_proves_raw_reopen_and_conflict_preserving_typed_handoff()
    -> Result<(), Box<dyn Error>> {
        let cboe_bytes = b"Cboe Symbol,OSI Symbol,Underlying,Matching Unit,Closing Only\n000u56,ZVZZT 990101C00005000,SPY,25,False\n000u57,ZVZZT 990101C00005000,SPY,25,False\n";
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
                ReferenceSurface::OccDlpSelectedText,
            ],
            PublicationLimits::try_new(2, 2, 5 * 1024 * 1024, 3, 4)?,
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
        let paths = LocalPaths::prepare(artifacts.path())?;
        let raw_store = Arc::new(paths.sealed_research_journal_store()?);
        let publication_control = ReferenceFetchControl::for_duration(
            Duration::from_secs(60),
            ReferenceCancellation::new(),
        )?;
        let mut request_budget =
            ReferenceRequestBudget::try_for_publication(plan.publication_request())?;
        let mut exports = Vec::new();
        let mut alias_assertions = Vec::new();
        let mut cboe_claim = None;
        for official_request in plan.requests() {
            let (body, content_type, disposition) = match official_request.surface() {
                ReferenceSurface::CboeAllSeries { .. } => (
                    cboe_bytes.to_vec(),
                    "text/csv; charset=utf-8",
                    "attachment; filename=cone_listed_symbol_reference_2026_08_14_01_13_36.csv"
                        .to_owned(),
                ),
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
            let mut streamed = crate::transport::capture_retrieved_for_test(
                Arc::clone(&raw_store),
                official_request,
                object,
                &publication_control,
            )?;
            match official_request.surface() {
                ReferenceSurface::CboeAllSeries { .. } => {
                    let receipt = streamed.parse_cboe_all_series(|record| {
                        let export = ReferenceExportRecord::from(record);
                        export
                            .visit_alias_assertions(|assertion| {
                                alias_assertions.push(assertion);
                                Ok(())
                            })
                            .map_err(|_| CboeParseError::SinkRejected)?;
                        exports.push(export);
                        Ok(())
                    })?;
                    let handoff = streamed
                        .complete_after_schema_validation(receipt.into())?
                        .finish()
                        .await?;
                    assert_eq!(
                        handoff.currentness(),
                        OptionsReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification
                    );
                    assert_eq!(handoff.page_receipt().returned_records(), 2);
                    request_budget.observe_typed_handoff(&handoff)?;
                    let (raw, context, http, page) = handoff.into_parts();
                    assert_eq!(raw.content_digest(), context.payload_digest());
                    assert_eq!(http.payload_digest(), context.payload_digest());
                    assert_eq!(page.context(), &context);
                    cboe_claim = Some(raw.claim().clone());
                }
                ReferenceSurface::OccDlpSelectedText => {
                    let receipt = streamed.parse_occ_dlp(|record| {
                        let export = ReferenceExportRecord::from(record);
                        export
                            .visit_alias_assertions(|assertion| {
                                alias_assertions.push(assertion);
                                Ok(())
                            })
                            .map_err(|_| OccParseError::SinkRejected)?;
                        exports.push(export);
                        Ok(())
                    })?;
                    let handoff = streamed
                        .complete_after_schema_validation(receipt.into())?
                        .finish()
                        .await?;
                    assert_eq!(handoff.page_receipt().returned_records(), 1);
                    request_budget.observe_typed_handoff(&handoff)?;
                    let (raw, context, http, page) = handoff.into_parts();
                    assert_eq!(raw.content_digest(), context.payload_digest());
                    assert_eq!(http.payload_digest(), context.payload_digest());
                    assert_eq!(page.context(), &context);
                }
                _ => return Err(std::io::Error::other("unexpected core surface").into()),
            }
        }

        assert_eq!(exports.len(), 3);
        let cboe = exports
            .iter()
            .find_map(ReferenceExportRecord::as_cboe_series)
            .ok_or_else(|| std::io::Error::other("Cboe typed export is absent"))?;
        assert_eq!(cboe.venue(), CboeVenue::C1);
        assert_eq!(cboe.cboe_symbol_id().as_str(), "000u56");
        assert_eq!(cboe.contract().osi().as_str(), "ZVZZT 990101C00005000");
        assert_eq!(
            exports[0].validity(),
            OptionsReferenceValidityDisposition::ExactSourceSnapshotOnly
        );
        assert_eq!(
            exports[0].identity(),
            OptionsReferenceIdentityDisposition::ProviderNativeReferenceOnly
        );
        let occ = exports
            .iter()
            .find_map(ReferenceExportRecord::as_occ_product)
            .ok_or_else(|| std::io::Error::other("OCC typed export is absent"))?;
        assert_eq!(occ.options_symbol().as_str(), "1AAL");
        assert_eq!(occ.underlying_symbol().as_str(), "AAL");

        alias_assertions.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        let mut alias_assertions = alias_assertions.into_iter();
        let mut resolutions = Vec::new();
        let mut conflicts = Vec::new();
        let reconciliation =
            ReferenceConflictReconciler::try_for_publication(plan.publication_request())?
                .reconcile(
                    || Ok(alias_assertions.next()),
                    |resolution| {
                        resolutions.push(resolution);
                        Ok(())
                    },
                    |conflict| {
                        conflicts.push(conflict);
                        Ok(())
                    },
                )?;
        let accounting = request_budget.finish(&reconciliation)?;
        assert_eq!(accounting.completed_pages(), 2);
        assert_eq!(accounting.returned_records(), 3);
        assert_eq!(accounting.conflicts(), 1);
        assert!(resolutions.iter().any(|resolution| {
            resolution.state() == ReferenceAliasResolutionState::Ambiguous
                && matches!(resolution.key(), ReferenceAliasKey::CboeOsi { .. })
        }));
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].kind(),
            ReferenceConflictKind::CboeOsiMapsMultipleSymbols
        );
        assert_ne!(
            conflicts[0].first_evidence(),
            conflicts[0].second_evidence()
        );

        let claim = cboe_claim
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Cboe raw claim is absent"))?;
        let raw_control = TestRawObjectControl {
            fetch: &publication_control,
            wall_deadline: plan.publication_request().deadline(),
        };
        let mut reopened = raw_store.open_verified_logical_object_claim(claim, &raw_control)?;
        let mut reopened_bytes = Vec::new();
        reopened.read_to_end(&mut reopened_bytes)?;
        assert_eq!(reopened_bytes, cboe_bytes);
        let reopened_receipt = reopened.reverify_for_commit(&raw_control)?;
        assert_eq!(reopened_receipt.claim(), claim);

        for (object_id, hostile_name) in [
            ("occ-xml-oversized-text", ">".repeat(4_097)),
            (
                "occ-xml-oversized-cdata",
                format!("<![CDATA[{}]]>", "<>".repeat(2_049)),
            ),
        ] {
            let hostile_xml = format!(
                "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><results><record><optionSymbol>1AAL</optionSymbol><underlyingSymbol>AAL</underlyingSymbol><symbolName>{hostile_name}</symbolName><positionLimit>25000000</positionLimit><onnProductType>EF</onnProductType><exchanges>ABCIPX</exchanges></record></results>"
            );
            let hostile_context = object_context(
                ReferenceSurface::OccDlpDailyXml,
                object_id,
                OccDlpSchema::DailyXmlV1.media_type(),
                OccDlpSchema::DailyXmlV1.native_schema(),
                hostile_xml.as_bytes(),
            )?;
            assert!(matches!(
                OccDlpParser::try_new(hostile_context)?.parse(hostile_xml.as_bytes(), |_| Ok(())),
                Err(OccParseError::MalformedXml)
            ));
        }
        let hostile_dtd = format!(
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><!DOCTYPE results [{}]><results><record><optionSymbol>1AAL</optionSymbol><underlyingSymbol>AAL</underlyingSymbol><symbolName>American Airlines Group Inc</symbolName><positionLimit>25000000</positionLimit><onnProductType>EF</onnProductType><exchanges>ABCIPX</exchanges></record></results>",
            "<!-- ] -->".repeat(512)
        );
        let hostile_dtd_context = object_context(
            ReferenceSurface::OccDlpDailyXml,
            "occ-xml-oversized-dtd",
            OccDlpSchema::DailyXmlV1.media_type(),
            OccDlpSchema::DailyXmlV1.native_schema(),
            hostile_dtd.as_bytes(),
        )?;
        assert!(matches!(
            OccDlpParser::try_new(hostile_dtd_context)?.parse(hostile_dtd.as_bytes(), |_| Ok(())),
            Err(OccParseError::MalformedXml)
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
