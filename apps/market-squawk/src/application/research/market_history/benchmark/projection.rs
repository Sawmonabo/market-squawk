//! Bounded common-session alignment and checked split-price indexing.

use super::{
    BenchmarkHistoryCoordinate, BenchmarkHistoryDisposition, BenchmarkHistoryObservation,
    BenchmarkHistoryPoint, Error, MAX_MEMBERS, MAX_POINTS, SourceSeries, VERSION, check, reserved,
};
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest as _, Sha256};
use std::{io, time::Instant};
use tokio_util::sync::CancellationToken;

type Aligned = (
    BenchmarkHistoryDisposition,
    Option<BenchmarkHistoryCoordinate>,
    Vec<BenchmarkHistoryPoint>,
);

pub(super) fn align(
    sources: &[Option<SourceSeries>],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Aligned, Error> {
    if sources.len() < 2 || sources.len() > MAX_MEMBERS {
        return Err(Error::IntegrityUnproven);
    }
    let Some(subject) = &sources[0] else {
        return Ok((
            BenchmarkHistoryDisposition::MissingSubject,
            None,
            Vec::new(),
        ));
    };
    let Some(selected) = &sources[1] else {
        return Ok((
            BenchmarkHistoryDisposition::MissingSelectedComparison,
            None,
            Vec::new(),
        ));
    };
    // The accompanying series cannot change the main comparison's dates or baseline.
    // It is sampled only at those exact source-owned sessions, with gaps elsewhere.
    let mut coordinates = reserved(MAX_POINTS * 2)?;
    for source in sources[..2].iter().flatten() {
        coordinates.extend(source.points.iter().map(|(coordinate, _)| *coordinate));
    }
    coordinates.sort_unstable();
    coordinates.dedup();
    if coordinates
        .windows(2)
        .any(|pair| pair[0].session_close == pair[1].session_close || pair[0].date >= pair[1].date)
    {
        // Independently valid calendars can have incompatible sessions. Do not equate
        // different closes just because the displayed nominal date happens to agree.
        return Ok((
            BenchmarkHistoryDisposition::NoCommonObservation,
            None,
            Vec::new(),
        ));
    }
    let start = coordinates.len().saturating_sub(MAX_POINTS);
    let baseline = coordinates[start..].iter().copied().find(|coordinate| {
        observation(subject, coordinate).is_some() && observation(selected, coordinate).is_some()
    });
    let Some(baseline) = baseline else {
        return Ok((
            BenchmarkHistoryDisposition::NoCommonObservation,
            None,
            Vec::new(),
        ));
    };
    let mut anchors = reserved(sources.len())?;
    for source in sources {
        anchors.push(
            source
                .as_ref()
                .and_then(|source| observation(source, &baseline))
                .map(|value| value.close),
        );
    }
    let mut points = reserved(coordinates.len() - start)?;
    for coordinate in coordinates[start..]
        .iter()
        .copied()
        .filter(|coordinate| *coordinate >= baseline)
    {
        check(deadline, cancellation)?;
        let mut observations = reserved(sources.len())?;
        for (source, anchor) in sources.iter().zip(&anchors) {
            let value = match (
                source
                    .as_ref()
                    .and_then(|source| observation(source, &coordinate)),
                anchor,
            ) {
                (Some(value), Some(anchor)) => {
                    let mut value = value.clone();
                    value.price_index = indexed(value.close, *anchor)?;
                    Some(value)
                }
                _ => None,
            };
            observations.push(value);
        }
        points.push(BenchmarkHistoryPoint {
            coordinate,
            observations,
        });
    }
    Ok((
        BenchmarkHistoryDisposition::Available,
        Some(baseline),
        points,
    ))
}

fn observation<'a>(
    source: &'a SourceSeries,
    coordinate: &BenchmarkHistoryCoordinate,
) -> Option<&'a BenchmarkHistoryObservation> {
    source
        .points
        .binary_search_by_key(coordinate, |(coordinate, _)| *coordinate)
        .ok()
        .and_then(|index| source.points[index].1.as_ref())
}

fn indexed(close: Decimal, anchor: Decimal) -> Result<Decimal, Error> {
    if close <= Decimal::ZERO || anchor <= Decimal::ZERO {
        return Err(Error::IntegrityUnproven);
    }
    close
        .checked_div(anchor)
        .and_then(|value| value.checked_mul(Decimal::from(100)))
        .map(|value| {
            value
                .round_dp_with_strategy(8, RoundingStrategy::MidpointNearestEven)
                .normalize()
        })
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(Error::IntegrityUnproven)
}

/// Preserve the exact v1 JSON commitment while hashing incrementally instead of retaining
/// a second complete serialized point buffer beside the typed projection.
pub(super) fn digest(
    disposition: BenchmarkHistoryDisposition,
    baseline: Option<BenchmarkHistoryCoordinate>,
    points: &[BenchmarkHistoryPoint],
) -> Result<[u8; 32], Error> {
    let mut writer = ProjectionHasher(Sha256::new());
    writer
        .0
        .update(b"market-squawk/saved-split-benchmark-chart/v1\0");
    serde_json::to_writer(&mut writer, &(VERSION, disposition, baseline, points))
        .map_err(|_| Error::IntegrityUnproven)?;
    Ok(writer.0.finalize().into())
}

struct ProjectionHasher(Sha256);
impl io::Write for ProjectionHasher {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
