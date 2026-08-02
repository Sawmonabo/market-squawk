//! Explicit versioned venue-session authority for `Day` order expiration.

use std::collections::BTreeSet;
use std::str::FromStr;

use chrono_tz::Tz;
use market_squawk_domain::{RuleVersion, SourceIdentifier, Timestamp, VenueId};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Maximum authoritative venue sessions retained by one paper configuration.
pub const MAX_PAPER_VENUE_SESSIONS: usize = 4_096;

/// One exact UTC venue interval supplied by an external calendar authority.
///
/// `closes_at_exclusive` is the first instant at which a `Day` order is no longer eligible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperVenueSession {
    session_id: SourceIdentifier,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
}

impl PaperVenueSession {
    /// Validates one nonempty half-open session interval.
    pub fn try_new(
        session_id: SourceIdentifier,
        opens_at: Timestamp,
        closes_at_exclusive: Timestamp,
    ) -> Result<Self, PaperSessionCalendarError> {
        if closes_at_exclusive <= opens_at {
            return Err(PaperSessionCalendarError::InvalidInterval);
        }
        Ok(Self {
            session_id,
            opens_at,
            closes_at_exclusive,
        })
    }

    /// Returns the source calendar's stable session identity.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    /// Returns the inclusive UTC session open.
    pub const fn opens_at(&self) -> Timestamp {
        self.opens_at
    }

    /// Returns the exclusive UTC session close.
    pub const fn closes_at_exclusive(&self) -> Timestamp {
        self.closes_at_exclusive
    }
}

/// Immutable calendar evidence used to resolve `Day` expiry without approximation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperVenueSessionCalendar {
    calendar_id: SourceIdentifier,
    ruleset_version: RuleVersion,
    venue_id: VenueId,
    time_zone: Tz,
    sessions: Box<[PaperVenueSession]>,
    digest: [u8; 32],
}

impl PaperVenueSessionCalendar {
    /// Validates explicit IANA time-zone and strictly ordered non-overlapping UTC sessions.
    pub fn try_new(
        calendar_id: SourceIdentifier,
        ruleset_version: RuleVersion,
        venue_id: VenueId,
        time_zone: &str,
        sessions: Vec<PaperVenueSession>,
    ) -> Result<Self, PaperSessionCalendarError> {
        let time_zone =
            Tz::from_str(time_zone).map_err(|_| PaperSessionCalendarError::InvalidTimeZone)?;
        if sessions.is_empty() {
            return Err(PaperSessionCalendarError::MissingSessions);
        }
        if sessions.len() > MAX_PAPER_VENUE_SESSIONS {
            return Err(PaperSessionCalendarError::TooManySessions);
        }
        let mut session_ids = BTreeSet::new();
        for pair in sessions.windows(2) {
            if pair[0].closes_at_exclusive > pair[1].opens_at {
                return Err(PaperSessionCalendarError::OverlappingOrUnordered);
            }
        }
        for session in &sessions {
            if !session_ids.insert(session.session_id.as_str()) {
                return Err(PaperSessionCalendarError::DuplicateSessionIdentity);
            }
        }

        let mut digest = Sha256::new();
        digest.update(b"market-squawk/paper-session-calendar/v1\0");
        update_text(&mut digest, calendar_id.as_str())?;
        digest.update(ruleset_version.get().to_be_bytes());
        update_text(&mut digest, venue_id.as_str())?;
        update_text(&mut digest, time_zone.name())?;
        digest.update(
            u32::try_from(sessions.len())
                .map_err(|_| PaperSessionCalendarError::TooManySessions)?
                .to_be_bytes(),
        );
        for session in &sessions {
            update_text(&mut digest, session.session_id.as_str())?;
            digest.update(session.opens_at.unix_nanos().to_be_bytes());
            digest.update(session.closes_at_exclusive.unix_nanos().to_be_bytes());
        }
        Ok(Self {
            calendar_id,
            ruleset_version,
            venue_id,
            time_zone,
            sessions: sessions.into_boxed_slice(),
            digest: digest.finalize().into(),
        })
    }

    /// Resolves the inclusive final eligible instant for one current venue session.
    ///
    /// # Errors
    ///
    /// Fails closed when the order venue does not match or no explicit session covers `observed_at`.
    pub fn day_expires_at(
        &self,
        venue_id: &VenueId,
        observed_at: Timestamp,
    ) -> Result<Timestamp, PaperSessionCalendarError> {
        if venue_id != &self.venue_id {
            return Err(PaperSessionCalendarError::VenueMismatch);
        }
        let insertion = self
            .sessions
            .partition_point(|session| session.opens_at <= observed_at);
        let session = insertion
            .checked_sub(1)
            .and_then(|index| self.sessions.get(index))
            .filter(|session| observed_at < session.closes_at_exclusive)
            .ok_or(PaperSessionCalendarError::MissingSessionEvidence)?;
        session
            .closes_at_exclusive
            .checked_add_nanos(-1)
            .map_err(|_| PaperSessionCalendarError::InvalidInterval)
    }

    /// Returns the stable calendar source identity.
    pub const fn calendar_id(&self) -> &SourceIdentifier {
        &self.calendar_id
    }

    /// Returns the source calendar ruleset revision.
    pub const fn ruleset_version(&self) -> RuleVersion {
        self.ruleset_version
    }

    /// Returns the exact venue whose sessions are authoritative.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the validated IANA time-zone identifier used by the calendar source.
    pub fn time_zone(&self) -> &'static str {
        self.time_zone.name()
    }

    /// Returns all bounded authoritative sessions in chronological order.
    pub const fn sessions(&self) -> &[PaperVenueSession] {
        &self.sessions
    }

    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

fn update_text(digest: &mut Sha256, value: &str) -> Result<(), PaperSessionCalendarError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| PaperSessionCalendarError::TextTooLong)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

/// Invalid or absent venue-session calendar evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperSessionCalendarError {
    #[error("venue session interval must be nonempty")]
    InvalidInterval,
    #[error("venue session calendar requires at least one explicit session")]
    MissingSessions,
    #[error("venue session calendar exceeds its bounded session count")]
    TooManySessions,
    #[error("venue sessions overlap or are not chronological")]
    OverlappingOrUnordered,
    #[error("venue session identities must be unique within a calendar revision")]
    DuplicateSessionIdentity,
    #[error("venue session calendar contains an invalid IANA time zone")]
    InvalidTimeZone,
    #[error("venue session calendar text exceeds its canonical digest bound")]
    TextTooLong,
    #[error("venue session calendar does not match the order venue")]
    VenueMismatch,
    #[error("no authoritative venue session covers the order timestamp")]
    MissingSessionEvidence,
}
