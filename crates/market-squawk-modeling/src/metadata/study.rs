//! Canonical transport of independently admitted dataset study qualification.

use market_squawk_data::{
    ChronologicalSplitPolicy, DatasetBuildPurpose, DatasetStudyPolicy, DatasetTargetHorizon,
    Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, FundamentalCadence, HistoricalStudyBasis, HistoricalStudyLimitation,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;

#[derive(Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TrainingPeriodWire {
    ExactTime {
        start_unix_nanos: i64,
        end_unix_nanos: i64,
    },
    FiscalDates {
        start: CalendarDate,
        end: CalendarDate,
    },
}
impl TrainingPeriodWire {
    pub(crate) fn decode(&self) -> Result<super::TrainingPeriod, super::ModelMetadataError> {
        match *self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
            } => super::TrainingPeriod::try_new(
                market_squawk_domain::Timestamp::from_unix_nanos(start_unix_nanos),
                market_squawk_domain::Timestamp::from_unix_nanos(end_unix_nanos),
            ),
            Self::FiscalDates { start, end } => super::TrainingPeriod::try_fiscal(start, end),
        }
    }
    pub(crate) const fn timestamp_bounds(&self) -> Option<[i64; 2]> {
        match *self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
            } => Some([start_unix_nanos, end_unix_nanos]),
            _ => None,
        }
    }
    pub(crate) const fn fiscal_bounds(&self) -> Option<[CalendarDate; 2]> {
        match *self {
            Self::FiscalDates { start, end } => Some([start, end]),
            _ => None,
        }
    }
    pub(crate) fn native_numeric(&self) -> (u8, [i64; 2]) {
        match *self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
            } => (1, [start_unix_nanos, end_unix_nanos]),
            Self::FiscalDates { start, end } => (
                2,
                [
                    i64::from(start.days_since_unix_epoch()),
                    i64::from(end.days_since_unix_epoch()),
                ],
            ),
        }
    }
}
impl Serialize for TrainingPeriodWire {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(Some(3))?;
        match self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
            } => {
                map.serialize_entry("end_unix_nanos", end_unix_nanos)?;
                map.serialize_entry("kind", "exact_time")?;
                map.serialize_entry("start_unix_nanos", start_unix_nanos)?;
            }
            Self::FiscalDates { start, end } => {
                map.serialize_entry("end", &canonical_date(*end))?;
                map.serialize_entry("kind", "fiscal_dates")?;
                map.serialize_entry("start", &canonical_date(*start))?;
            }
        }
        map.end()
    }
}

fn canonical_date(date: CalendarDate) -> serde_json::Value {
    serde_json::json!({"day":date.day(),"month":date.month(),"year":date.year()})
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrainingStudyWire {
    basis: HistoricalStudyBasis,
    decision_lag_nanos: Option<u64>,
    limitations: Vec<HistoricalStudyLimitation>,
    purpose: DatasetBuildPurpose,
    snapshot_as_of_unix_nanos: i64,
    source_snapshot_sha256: String,
    target_horizon: TrainingTargetHorizonWire,
}

#[derive(Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TrainingTargetHorizonWire {
    ExactElapsed {
        nanos: u64,
    },
    FiscalPeriods {
        cadence: FundamentalCadence,
        periods_ahead: NonZeroU16,
    },
}
impl Serialize for TrainingTargetHorizonWire {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(Some(match self {
            Self::ExactElapsed { .. } => 2,
            _ => 3,
        }))?;
        match self {
            Self::ExactElapsed { nanos } => {
                map.serialize_entry("kind", "exact_elapsed")?;
                map.serialize_entry("nanos", nanos)?;
            }
            Self::FiscalPeriods {
                cadence,
                periods_ahead,
            } => {
                map.serialize_entry("cadence", cadence)?;
                map.serialize_entry("kind", "fiscal_periods")?;
                map.serialize_entry("periods_ahead", periods_ahead)?;
            }
        }
        map.end()
    }
}
impl TrainingTargetHorizonWire {
    fn matches(&self, value: DatasetTargetHorizon) -> bool {
        match (self, value) {
            (Self::ExactElapsed { nanos }, DatasetTargetHorizon::ExactElapsed(duration)) => {
                u128::from(*nanos) == duration.as_nanos()
            }
            (
                Self::FiscalPeriods {
                    cadence,
                    periods_ahead,
                },
                DatasetTargetHorizon::FiscalPeriods {
                    cadence: actual_cadence,
                    periods_ahead: actual_distance,
                },
            ) => *cadence == actual_cadence && *periods_ahead == actual_distance,
            _ => false,
        }
    }
}

#[derive(Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TrainingSplitWire {
    ExactTime {
        train_end_unix_nanos: i64,
        validation_end_unix_nanos: i64,
        test_end_unix_nanos: i64,
    },
    FiscalDates {
        train_end: CalendarDate,
        validation_end: CalendarDate,
        test_end: CalendarDate,
    },
}
impl TrainingSplitWire {
    pub(crate) fn matches(&self, value: ChronologicalSplitPolicy) -> bool {
        match self {
            Self::ExactTime {
                train_end_unix_nanos,
                validation_end_unix_nanos,
                test_end_unix_nanos,
            } => {
                value
                    .timestamp_boundaries()
                    .map(|v| v.map(|t| t.unix_nanos()))
                    == Some([
                        *train_end_unix_nanos,
                        *validation_end_unix_nanos,
                        *test_end_unix_nanos,
                    ])
            }
            Self::FiscalDates {
                train_end,
                validation_end,
                test_end,
            } => value.fiscal_boundaries() == Some([*train_end, *validation_end, *test_end]),
        }
    }
    pub(crate) const fn timestamp_boundaries(&self) -> Option<[i64; 3]> {
        match self {
            Self::ExactTime {
                train_end_unix_nanos,
                validation_end_unix_nanos,
                test_end_unix_nanos,
            } => Some([
                *train_end_unix_nanos,
                *validation_end_unix_nanos,
                *test_end_unix_nanos,
            ]),
            _ => None,
        }
    }
    pub(crate) fn native_numeric(&self) -> (u8, [i64; 3]) {
        match *self {
            Self::ExactTime {
                train_end_unix_nanos,
                validation_end_unix_nanos,
                test_end_unix_nanos,
            } => (
                1,
                [
                    train_end_unix_nanos,
                    validation_end_unix_nanos,
                    test_end_unix_nanos,
                ],
            ),
            Self::FiscalDates {
                train_end,
                validation_end,
                test_end,
            } => (
                2,
                [train_end, validation_end, test_end]
                    .map(|date| i64::from(date.days_since_unix_epoch())),
            ),
        }
    }
}
impl Serialize for TrainingSplitWire {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(Some(4))?;
        match self {
            Self::ExactTime {
                train_end_unix_nanos,
                validation_end_unix_nanos,
                test_end_unix_nanos,
            } => {
                map.serialize_entry("kind", "exact_time")?;
                map.serialize_entry("test_end_unix_nanos", test_end_unix_nanos)?;
                map.serialize_entry("train_end_unix_nanos", train_end_unix_nanos)?;
                map.serialize_entry("validation_end_unix_nanos", validation_end_unix_nanos)?;
            }
            Self::FiscalDates {
                train_end,
                validation_end,
                test_end,
            } => {
                map.serialize_entry("kind", "fiscal_dates")?;
                map.serialize_entry("test_end", &canonical_date(*test_end))?;
                map.serialize_entry("train_end", &canonical_date(*train_end))?;
                map.serialize_entry("validation_end", &canonical_date(*validation_end))?;
            }
        }
        map.end()
    }
}

impl TrainingStudyWire {
    pub(crate) fn matches(&self, policy: &DatasetStudyPolicy, snapshot: Sha256Digest) -> bool {
        self.basis == policy.basis()
            && self.purpose == policy.purpose()
            && self.snapshot_as_of_unix_nanos == policy.snapshot_as_of().unix_nanos()
            && self.decision_lag_nanos.map(u128::from)
                == policy.decision_lag().map(|lag| lag.as_nanos())
            && self.target_horizon.matches(policy.target_horizon())
            && self.limitations == policy.limitations()
            && self.source_snapshot_sha256.len() == 64
            && self
                .source_snapshot_sha256
                .bytes()
                .zip(snapshot.bytes().into_iter().flat_map(|byte| {
                    [
                        b"0123456789abcdef"[usize::from(byte >> 4)],
                        b"0123456789abcdef"[usize::from(byte & 15)],
                    ]
                }))
                .all(|(actual, expected)| actual == expected)
    }
}

pub(crate) fn study_matches(
    wire: Option<&TrainingStudyWire>,
    policy: Option<&DatasetStudyPolicy>,
    snapshot: Option<Sha256Digest>,
) -> bool {
    match (wire, policy, snapshot) {
        (None, None, None) => true,
        (Some(wire), Some(policy), Some(snapshot)) => wire.matches(policy, snapshot),
        _ => false,
    }
}
