#![no_main]

use libfuzzer_sys::fuzz_target;
use market_squawk_adapter_files::{FuzzFileFormat, fuzz_parse_bytes};
use market_squawk_adapter_sec::{
    CompanyFactsDocument, SecParserLimits, SubmissionsDocument, XbrlDocumentContext,
    XbrlDocumentParser,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp,
    XbrlTaxonomySet,
};

const MAX_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    if payload.len() > MAX_INPUT_BYTES {
        return;
    }
    let Some(sec_limits) = sec_limits() else {
        return;
    };
    let format = match selector % 11 {
        0 => Some(FuzzFileFormat::Csv),
        1 => Some(FuzzFileFormat::Tsv),
        2 => Some(FuzzFileFormat::Json),
        3 => Some(FuzzFileFormat::Ndjson),
        4 => Some(FuzzFileFormat::Xml),
        5 => Some(FuzzFileFormat::Excel),
        6 => Some(FuzzFileFormat::Parquet),
        7 => Some(FuzzFileFormat::Ofx),
        8 => {
            let _document = CompanyFactsDocument::parse(payload, sec_limits);
            None
        }
        9 => {
            let _document = SubmissionsDocument::parse(payload, sec_limits);
            None
        }
        _ => {
            if let Some(context) = xbrl_context() {
                let _document = XbrlDocumentParser::parse(payload, sec_limits, context);
            }
            None
        }
    };
    if let Some(format) = format {
        let _rows = fuzz_parse_bytes(format, payload);
    }
});

fn sec_limits() -> Option<SecParserLimits> {
    SecParserLimits::try_new(
        MAX_INPUT_BYTES,
        4_096,
        64,
        256 * 1024,
        MAX_INPUT_BYTES,
        16 * 1024 * 1024,
    )
    .ok()
}

fn xbrl_context() -> Option<XbrlDocumentContext> {
    Some(XbrlDocumentContext::new(
        SourceIdentifier::try_from("0000320193-25-000079").ok()?,
        XbrlTaxonomySet::declared(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            SourceIdentifier::try_from("us-gaap-2025").ok()?,
        ),
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [7; 32],
        )),
        Timestamp::from_unix_nanos(42),
    ))
}
