//! Bounded provider-native option reference contracts for OCC and Cboe.
//!
//! This crate builds exact official reference-file requests, validates bounded HTTP results through
//! an injected executor, and decodes selected publications. It contains no implicit network client
//! and does not create canonical [`market_squawk_domain::InstrumentId`] values. A syntactically
//! valid OSI symbol or a row's presence in a publication is reference evidence, not proof of a
//! current quote, consolidated coverage, tradability, standard multiplier, or adjusted-contract
//! economics.

#![deny(missing_docs)]

mod cboe;
mod identity;
mod occ;
mod publication;
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
    OccDlpParseReceipt, OccDlpParser, OccDlpPresence, OccDlpProductReference, OccExchangeCode,
    OccMemoCategory, OccMemoCsvSchema, OccMemoDiscovery, OccMemoInterpretation,
    OccMemoParseReceipt, OccMemoParser, OccParseError, OccPositionLimit, OccProductType,
};
pub use publication::{
    CatalogConflict, CatalogConflictKind, CatalogCounts, ObjectClockEvidence, PageTerminalState,
    PublicationCatalog, PublicationCatalogBuilder, PublicationCompleteness, PublicationError,
    PublicationLimits, PublicationRequest, ReferenceObjectContext, ReferencePageReceipt,
    ReferenceProvider, ReferenceSurface, SurfaceCompleteness,
};
pub use transport::{
    CboeSchemaFreeze, ConditionalCacheRequest, HttpCacheEvidence, OCC_MEMO_DOCUMENT_MAX_BYTES,
    OfficialPublicationPlan, OfficialPublicationPolicy, OfficialReferenceRequest,
    OfficialReferenceSource, ReferenceCancellation, ReferenceFetchControl, ReferenceFetchOutcome,
    ReferenceHeaderValue, ReferenceHttpExecutor, ReferenceHttpMethod, ReferenceHttpReceipt,
    ReferenceHttpRequest, ReferenceHttpResponse, ReferenceNotModifiedReceipt,
    ReferenceTransportError, RetrievedReferenceObject, RetryAfterEvidence,
    SelectedReferenceDecoder,
};

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::error::Error;
    use std::time::Duration;

    use market_squawk_domain::{
        AvailabilityEvidence, CalendarDate, DigestAlgorithm, EvidenceDigest, OptionKind,
        SourceIdentifier, Timestamp,
    };
    use sha2::{Digest, Sha256};

    use super::*;

    #[derive(Debug)]
    struct SingleResponseExecutor {
        expected_locator: String,
        response: RefCell<Option<ReferenceHttpResponse>>,
    }

    impl ReferenceHttpExecutor for SingleResponseExecutor {
        fn execute(
            &self,
            request: &ReferenceHttpRequest,
            control: &ReferenceFetchControl,
        ) -> Result<ReferenceHttpResponse, ReferenceTransportError> {
            let _remaining = control.remaining()?;
            assert_eq!(request.method(), ReferenceHttpMethod::Get);
            assert_eq!(request.locator().as_str(), self.expected_locator);
            assert!(request.accept_encoding_identity());
            assert_eq!(request.maximum_redirects(), 4);
            assert_eq!(request.if_none_match(), None);
            assert_eq!(request.if_modified_since(), None);
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(ReferenceTransportError::executor_failed)
        }
    }

    #[test]
    fn osi_identity_retains_exact_terms_without_inventing_expiration_or_multiplier()
    -> Result<(), Box<dyn Error>> {
        let bytes = b"Symbol,OSI Symbol,Symbol Condition,Underlying,Unit\n00mEVO,MSFT  190920C00150000,N,MSFT,1\n";
        let context = object_context(
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::C1,
            },
            "cboe-c1-object",
            "text/csv",
            CboeAllSeriesCsvSchema::SymbolV1.native_schema(),
            bytes,
        )?;
        let parser =
            CboeAllSeriesParser::try_new(CboeVenue::C1, CboeAllSeriesCsvSchema::SymbolV1, context)?;
        let mut records = Vec::new();
        let receipt = parser.parse(bytes, |record| {
            records.push(record);
            Ok(())
        })?;
        assert_eq!(receipt.returned_records(), 1);
        let record = records
            .first()
            .ok_or_else(|| std::io::Error::other("decoded Cboe record is absent"))?;
        assert_eq!(record.cboe_symbol_id().as_str(), "00mEVO");
        assert_eq!(record.contract().root(), "MSFT");
        assert_eq!(record.contract().expiration().year_two_digits(), 19);
        assert_eq!(record.contract().expiration().calendar_date(), None);
        assert_eq!(record.contract().strike().thousandths(), 150_000);
        assert_eq!(record.contract().kind(), OptionKind::Call);
        assert_eq!(record.contract().multiplier().multiplier(), None);

        let resolved = record
            .contract()
            .clone()
            .try_with_provider_expiration(
                CalendarDate::new(2019, 9, 20)?,
                SourceIdentifier::try_from("cboe:provider-expiration")?,
            )?
            .try_with_provider_multiplier(
                100,
                SourceIdentifier::try_from("occ:operative-document:multiplier")?,
            )?;
        assert_eq!(
            resolved.expiration().calendar_date(),
            Some(CalendarDate::new(2019, 9, 20)?)
        );
        assert_eq!(
            resolved.multiplier().multiplier().map(|value| value.get()),
            Some(100)
        );
        Ok(())
    }

    #[test]
    fn complete_publication_preserves_mapping_conflict_and_uninterpreted_occ_event()
    -> Result<(), Box<dyn Error>> {
        let c1_bytes = b"Symbol,OSI Symbol,Symbol Condition,Underlying,Unit\n00mEVO,MSFT  190920C00150000,N,MSFT,1\n";
        let bzx_bytes = b"Symbol,OSI Symbol,Symbol Condition,Underlying,Unit\n00mEVO,MSFT  190920P00150000,C,MSFT,2\n";
        let memo_bytes = b"Number,Post Date,Effective Date,Title,Category\n59532,08/07/2026,09/03/2026,MSFT - 2 For 1 Stock Split,Contract Adjustment|Options\n";

        let c1_context = object_context(
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::C1,
            },
            "cboe-c1-conflict-object",
            "text/csv",
            CboeAllSeriesCsvSchema::SymbolV1.native_schema(),
            c1_bytes,
        )?;
        let bzx_context = object_context(
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::Bzx,
            },
            "cboe-bzx-conflict-object",
            "text/csv",
            CboeAllSeriesCsvSchema::SymbolV1.native_schema(),
            bzx_bytes,
        )?;
        let memo_context = object_context(
            ReferenceSurface::OccMemoIndexCsv,
            "occ-memo-index-object",
            "text/csv",
            OccMemoCsvSchema::CategoryV1.native_schema(),
            memo_bytes,
        )?;

        let mut c1_records = Vec::new();
        let c1_receipt = CboeAllSeriesParser::try_new(
            CboeVenue::C1,
            CboeAllSeriesCsvSchema::SymbolV1,
            c1_context,
        )?
        .parse(c1_bytes, |record| {
            c1_records.push(record);
            Ok(())
        })?;
        let mut bzx_records = Vec::new();
        let bzx_receipt = CboeAllSeriesParser::try_new(
            CboeVenue::Bzx,
            CboeAllSeriesCsvSchema::SymbolV1,
            bzx_context,
        )?
        .parse(bzx_bytes, |record| {
            bzx_records.push(record);
            Ok(())
        })?;
        let mut memos = Vec::new();
        let memo_receipt = OccMemoParser::parse_csv(
            OccMemoCsvSchema::CategoryV1,
            memo_context,
            memo_bytes,
            |memo| {
                memos.push(memo);
                Ok(())
            },
        )?;
        let memo = memos
            .first()
            .ok_or_else(|| std::io::Error::other("decoded OCC memo is absent"))?;
        assert_eq!(
            memo.interpretation(),
            OccMemoInterpretation::FullOperativeDocumentsRequired
        );
        assert_eq!(memo.posted_date(), CalendarDate::new(2026, 8, 7)?);
        assert_eq!(memo.effective_date(), Some(CalendarDate::new(2026, 9, 3)?));

        let surfaces = vec![
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::C1,
            },
            ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::Bzx,
            },
            ReferenceSurface::OccMemoIndexCsv,
        ];
        let request = PublicationRequest::try_new(
            SourceIdentifier::try_from("options-reference-publication-test")?,
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(100),
            surfaces,
            PublicationLimits::try_new(8, 8, 1024 * 1024, 100, 10)?,
        )?;
        let mut catalog = PublicationCatalogBuilder::new(request);
        catalog.record_page(c1_receipt.page_receipt())?;
        catalog.record_page(bzx_receipt.page_receipt())?;
        catalog.record_page(memo_receipt.page_receipt())?;
        for record in c1_records.iter().chain(&bzx_records) {
            catalog.record_cboe_series(record)?;
        }
        catalog.record_occ_memo(memo)?;
        let catalog = catalog.finish();

        assert_eq!(catalog.completeness(), &PublicationCompleteness::Complete);
        assert_eq!(catalog.conflicts().len(), 1);
        assert_eq!(
            catalog.conflicts()[0].kind(),
            CatalogConflictKind::CboeSymbolMapsMultipleOsi
        );
        assert!(!catalog.publication_eligible());
        assert_eq!(catalog.counts().returned_records(), 3);
        Ok(())
    }

    #[test]
    fn official_source_mock_proves_exact_bounded_cboe_request_and_schema_receipt()
    -> Result<(), Box<dyn Error>> {
        let bytes = b"Symbol,OSI Symbol,Symbol Condition,Underlying,Unit\n00mEVO,MSFT  190920C00150000,N,MSFT,1\n";
        let request = PublicationRequest::try_new(
            SourceIdentifier::try_from("options-reference-http-mock")?,
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(100),
            vec![ReferenceSurface::CboeAllSeries {
                venue: CboeVenue::C1,
            }],
            PublicationLimits::try_new(1, 1, 1024 * 1024, 10, 1)?,
        )?;
        let plan = OfficialPublicationPlan::try_new(
            request,
            OfficialPublicationPolicy::new(
                CboeSchemaFreeze::single(CboeAllSeriesCsvSchema::SymbolV1),
                OccMemoCsvSchema::CategoryV1,
                None,
            ),
        )?;
        let official_request = plan
            .requests()
            .first()
            .ok_or_else(|| std::io::Error::other("official request is absent"))?;
        let locator = CboeVenue::C1.all_series_locator();
        assert_eq!(official_request.locator().as_str(), locator);

        let response = ReferenceHttpResponse::try_new(
            200,
            SourceIdentifier::try_from(locator)?,
            Vec::new(),
            Some(ReferenceHeaderValue::try_new("text/csv; charset=utf-8")?),
            Some(u64::try_from(bytes.len())?),
            None,
            HttpCacheEvidence::new(Some(ReferenceHeaderValue::try_new("\"fixture-v1\"")?), None),
            None,
            Timestamp::from_unix_nanos(6),
            true,
            bytes.to_vec(),
        )?;
        let source = OfficialReferenceSource::new(SingleResponseExecutor {
            expected_locator: locator.to_owned(),
            response: RefCell::new(Some(response)),
        });
        let control = ReferenceFetchControl::for_duration(
            Duration::from_secs(1),
            ReferenceCancellation::new(),
        )?;
        let fetched = source.fetch(official_request, &control)?;
        let ReferenceFetchOutcome::Modified(object) = fetched else {
            return Err(std::io::Error::other("mock response was not admitted as modified").into());
        };
        assert_eq!(
            object.decoder(),
            SelectedReferenceDecoder::CboeAllSeries(CboeAllSeriesCsvSchema::SymbolV1)
        );
        assert_eq!(object.bytes(), bytes);
        assert_eq!(
            object.receipt().observed_content_type().as_str(),
            "text/csv; charset=utf-8"
        );
        assert_eq!(object.context().media_type().as_str(), "text/csv");
        let mut decoded = Vec::new();
        let receipt = CboeAllSeriesParser::try_new(
            CboeVenue::C1,
            CboeAllSeriesCsvSchema::SymbolV1,
            object.context().clone(),
        )?
        .parse(object.bytes(), |record| {
            decoded.push(record);
            Ok(())
        })?;
        assert_eq!(receipt.returned_records(), 1);
        assert_eq!(decoded.len(), 1);
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
        )?;
        Ok(ReferenceObjectContext::try_new(
            provider,
            surface,
            SourceIdentifier::try_from(object_id)?,
            SourceIdentifier::try_from("https://provider.example/reference")?,
            SourceIdentifier::try_from("https://provider.example/reference")?,
            SourceIdentifier::try_from(media_type)?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            u64::try_from(bytes.len())?,
            SourceIdentifier::try_from(native_schema)?,
            clocks,
        )?)
    }
}
