//! Bounded common-session alignment and checked split-price indexing.

use super::{
    BenchmarkHistoryCoordinate, BenchmarkHistoryDisposition, BenchmarkHistoryPoint, Error,
    MAX_MEMBERS, SourceSeries, VERSION, check, reserved,
    source::SourcePoint,
    spool::{BenchmarkPointWriter, BenchmarkPoints},
};
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest as _, Sha256};
use std::{io, time::Instant};
use tokio_util::sync::CancellationToken;

type Aligned = (
    BenchmarkHistoryDisposition,
    Option<BenchmarkHistoryCoordinate>,
    BenchmarkPoints,
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
            BenchmarkPoints::Empty,
        ));
    };
    if sources[1].is_none() {
        return Ok((
            BenchmarkHistoryDisposition::MissingSelectedComparison,
            None,
            BenchmarkPoints::Empty,
        ));
    }
    let mut originals = reserved(sources.len())?;
    for source in sources {
        originals.push(
            source
                .as_ref()
                .map(|source| {
                    source
                        .points(deadline, cancellation)
                        .map(|points| Box::new(points) as PointIterator<'_>)
                })
                .transpose()?,
        );
    }
    let mut writer = BenchmarkPointWriter::new(
        subject.history.operation_scratch(),
        deadline,
        cancellation.clone(),
    )?;
    let (disposition, baseline) = merge_points(originals, deadline, cancellation, |point| {
        writer.push(&point)
    })?;
    // A later calendar mismatch invalidates the entire comparison. Private staged rows are
    // discarded, exactly as the previous complete-union validation returned no observations.
    let points = if disposition == BenchmarkHistoryDisposition::Available {
        writer.finish()?
    } else {
        BenchmarkPoints::Empty
    };
    Ok((disposition, baseline, points))
}

type PointIterator<'a> = Box<dyn Iterator<Item = Result<SourcePoint, Error>> + 'a>;

fn merge_points(
    mut sources: Vec<Option<PointIterator<'_>>>,
    deadline: Instant,
    cancellation: &CancellationToken,
    mut emit: impl FnMut(BenchmarkHistoryPoint) -> Result<(), Error>,
) -> Result<
    (
        BenchmarkHistoryDisposition,
        Option<BenchmarkHistoryCoordinate>,
    ),
    Error,
> {
    if !(2..=MAX_MEMBERS).contains(&sources.len()) {
        return Err(Error::IntegrityUnproven);
    };
    let mut heads = reserved(sources.len())?;
    for source in &mut sources {
        heads.push(next(source)?);
    }
    let mut previous = None;
    let mut baseline = None;
    let mut anchors = vec![None; sources.len()];
    loop {
        check(deadline, cancellation)?;
        let coordinate = match (heads[0].as_ref(), heads[1].as_ref()) {
            (Some(a), Some(b)) => a.0.min(b.0),
            (Some(a), None) => a.0,
            (None, Some(b)) => b.0,
            (None, None) => break,
        };
        if previous.is_some_and(|prior: BenchmarkHistoryCoordinate| {
            prior.session_close == coordinate.session_close || prior.date >= coordinate.date
        }) {
            return Ok((BenchmarkHistoryDisposition::NoCommonObservation, None));
        }
        previous = Some(coordinate);
        let mut observations = reserved(sources.len())?;
        for index in 0..sources.len() {
            // Only the optional accompanying source can have rows outside the two main series'
            // coordinate union. It cannot add dates or move their common baseline.
            while heads[index]
                .as_ref()
                .is_some_and(|point| point.0 < coordinate)
            {
                heads[index] = next(&mut sources[index])?;
            }
            let observation = if heads[index]
                .as_ref()
                .is_some_and(|point| point.0 == coordinate)
            {
                let point = heads[index].take().ok_or(Error::IntegrityUnproven)?;
                heads[index] = next(&mut sources[index])?;
                point.1
            } else {
                None
            };
            observations.push(observation);
        }
        if baseline.is_none() && observations[0].is_some() && observations[1].is_some() {
            baseline = Some(coordinate);
            for (anchor, observation) in anchors.iter_mut().zip(&observations) {
                *anchor = observation.as_ref().map(|value| value.close);
            }
        }
        if baseline.is_some() {
            for (observation, anchor) in observations.iter_mut().zip(&anchors) {
                if let (Some(value), Some(anchor)) = (observation.as_mut(), anchor) {
                    value.price_index = indexed(value.close, *anchor)?;
                } else {
                    *observation = None;
                }
            }
            emit(BenchmarkHistoryPoint {
                coordinate,
                observations,
            })?;
        }
    }
    // Consume the optional source's remaining originals as well, preserving fallible source
    // validation even though its later dates cannot expand the main comparison's domain.
    for (source, head) in sources.iter_mut().zip(&mut heads) {
        while head.is_some() {
            *head = next(source)?;
        }
    }
    Ok(if baseline.is_some() {
        (BenchmarkHistoryDisposition::Available, baseline)
    } else {
        (BenchmarkHistoryDisposition::NoCommonObservation, None)
    })
}
fn next(source: &mut Option<PointIterator<'_>>) -> Result<Option<SourcePoint>, Error> {
    source.as_mut().and_then(|source| source.next()).transpose()
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
    points: &BenchmarkPoints,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<[u8; 32], Error> {
    let mut writer = ProjectionHasher(Sha256::new());
    writer
        .0
        .update(b"market-squawk/saved-split-benchmark-chart/v1\0");
    serde_json::to_writer(&mut writer, &(VERSION, disposition, baseline, points)).map_err(
        |_| {
            check(deadline, cancellation)
                .err()
                .unwrap_or(Error::IntegrityUnproven)
        },
    )?;
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

#[cfg(test)]
mod tests {
    use super::super::BenchmarkHistoryObservation;
    use super::*;
    use market_squawk_domain::{CalendarDate, DataQuality, Timestamp};
    use std::time::Duration;

    fn original(day: u8, close: Option<i64>) -> Result<SourcePoint, Box<dyn std::error::Error>> {
        Ok((
            BenchmarkHistoryCoordinate {
                date: CalendarDate::new(2026, 9, day)?,
                session_close: Timestamp::from_unix_nanos(i64::from(day) * 100),
            },
            close.map(|close| BenchmarkHistoryObservation {
                close: Decimal::from(close),
                price_index: Decimal::ZERO,
                available_at: Timestamp::from_unix_nanos(1000),
                provider_completed_at: Some(Timestamp::from_unix_nanos(i64::from(day) * 100 + 1)),
                quality: DataQuality::OfficialDelayed,
            }),
        ))
    }
    fn rows(points: Vec<SourcePoint>) -> Option<PointIterator<'static>> {
        Some(Box::new(points.into_iter().map(Ok)))
    }

    #[test]
    fn streaming_comparison_preserves_common_anchor_gaps_and_accompanying_independence()
    -> Result<(), Box<dyn std::error::Error>> {
        let subject = vec![
            original(1, Some(10))?,
            original(2, None)?,
            original(3, Some(20))?,
            original(5, Some(30))?,
            original(7, None)?,
        ];
        let selected = vec![
            original(2, Some(5))?,
            original(3, Some(8))?,
            original(4, Some(9))?,
            original(5, None)?,
            original(7, Some(12))?,
        ];
        let accompanying = vec![
            original(1, Some(100))?,
            original(4, Some(110))?,
            original(5, Some(120))?,
            original(6, Some(130))?,
        ];
        let mut actual = Vec::new();
        let (disposition, baseline) = merge_points(
            vec![rows(subject), rows(selected), rows(accompanying)],
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
            |point| {
                actual.push(point);
                Ok(())
            },
        )
        .map_err(|error| format!("comparison failed: {error:?}"))?;
        assert_eq!(disposition, BenchmarkHistoryDisposition::Available);
        assert_eq!(baseline, Some(original(3, None)?.0));
        let mut expected = Vec::new();
        for (day, subject, selected) in [
            (
                3,
                Some((20, Decimal::from(100))),
                Some((8, Decimal::from(100))),
            ),
            (4, None, Some((9, Decimal::new(1125, 1)))),
            (5, Some((30, Decimal::from(150))), None),
            (7, None, Some((12, Decimal::from(150)))),
        ] {
            let observation = |value: Option<(i64, Decimal)>| -> Result<
                Option<BenchmarkHistoryObservation>,
                Box<dyn std::error::Error>,
            > {
                value
                    .map(|(close, index)| {
                        let mut point = original(day, Some(close))?.1.ok_or("missing fixture")?;
                        point.price_index = index;
                        Ok(point)
                    })
                    .transpose()
            };
            expected.push(BenchmarkHistoryPoint {
                coordinate: original(day, None)?.0,
                observations: vec![observation(subject)?, observation(selected)?, None],
            });
        }
        assert_eq!(actual, expected);
        // Precision remains checked decimal arithmetic, including eight-place nearest-even output.
        assert_eq!(
            indexed(Decimal::ONE, Decimal::from(6)).map_err(|error| format!("{error:?}"))?,
            Decimal::new(1_666_666_667, 8)
        );
        // A same-date, different-close calendar conflict invalidates even an earlier common row.
        let mut conflict = original(4, Some(9))?;
        conflict.0.session_close = Timestamp::from_unix_nanos(401);
        let (disposition, baseline) = merge_points(
            vec![
                rows(vec![original(3, Some(20))?, original(4, Some(30))?]),
                rows(vec![original(3, Some(8))?, conflict]),
            ],
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
            |_| Ok(()),
        )
        .map_err(|error| format!("comparison failed: {error:?}"))?;
        assert_eq!(
            (disposition, baseline),
            (BenchmarkHistoryDisposition::NoCommonObservation, None)
        );
        Ok(())
    }
}
