//! Critical hostile-input and local capability boundary proofs.

use std::error::Error;
use std::fs;
use std::io::{Cursor, Write as _};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_adapter_files::{
    ExtractionClock, ExtractionClockError, ExtractionClockReading, ExtractionLimits,
    ExtractionLimitsInput, FileAdapterError, FileExtractionSource, ParserLimit,
};
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, ExactPayloadEvidence, MetadataRevision,
    PayloadReference, ResearchObservation, ResearchTemporalCoordinate,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{
    ControlledInputFileError, InputFileError, InputReadCheckpoint, InputReadControl,
    InputReadControlError, InputReadPass, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, CoverageDomain,
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionRequest,
    FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceMetadataProvider, SourceObject,
    SourceProtocolProfile,
};
use parquet::arrow::ArrowWriter;
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

#[path = "hostile_files/capability.rs"]
mod capability;
#[path = "hostile_files/control.rs"]
mod control;
#[path = "hostile_files/manifest_text.rs"]
mod manifest_text;
#[path = "hostile_files/ofx.rs"]
mod ofx;
#[path = "hostile_files/storage.rs"]
mod storage;
#[path = "hostile_files/support.rs"]
mod support;
#[path = "hostile_files/xlsx.rs"]
mod xlsx;

use control::fixed_clock;
use support::*;
