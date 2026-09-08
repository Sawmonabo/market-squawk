//! One bounded parser dispatch shared by extraction and path-free previews.

use rust_decimal::Decimal;

use crate::manifest::FileFormat;
use crate::{
    FileAdapterError, ParseBudget, ParsedRow, csv, database, excel, json, ofx, parquet, xml,
};

pub(crate) fn parse_rows(
    format: &FileFormat,
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    match format {
        FileFormat::Csv { delimiter, .. } => csv::parse(bytes, *delimiter, budget),
        FileFormat::Tsv { .. } => csv::parse(bytes, b'\t', budget),
        FileFormat::Json { .. } => json::parse_json(bytes, budget),
        FileFormat::Ndjson { .. } => json::parse_ndjson(bytes, budget),
        FileFormat::Xml { record_element, .. } => xml::parse(bytes, record_element, budget),
        FileFormat::Excel { formula_policy } => excel::parse(bytes, *formula_policy, budget),
        FileFormat::Parquet { .. } => parquet::parse(bytes, budget),
        FileFormat::Sqlite {
            table,
            columns,
            order_by,
        } => database::parse(bytes, table, columns, order_by, budget),
        FileFormat::Ofx {
            account_id,
            currency,
        }
        | FileFormat::Qfx {
            account_id,
            currency,
        } => ofx::parse(bytes, account_id, currency, budget),
    }
}

pub(crate) fn parse_decimal_lexeme(value: &str) -> Result<Decimal, FileAdapterError> {
    let Some(exponent_index) = value.find(['e', 'E']) else {
        return Decimal::from_str_exact(value).map_err(|_| FileAdapterError::InvalidDecimal);
    };
    let (base, exponent) = value.split_at(exponent_index);
    let exponent = exponent
        .get(1..)
        .ok_or(FileAdapterError::InvalidDecimal)?
        .parse::<i32>()
        .map_err(|_| FileAdapterError::InvalidDecimal)?;
    let (negative, unsigned) = match base.as_bytes().first() {
        Some(b'-') => (true, base.get(1..).ok_or(FileAdapterError::InvalidDecimal)?),
        Some(b'+') => (
            false,
            base.get(1..).ok_or(FileAdapterError::InvalidDecimal)?,
        ),
        _ => (false, base),
    };
    let (whole, fractional) = match unsigned.split_once('.') {
        Some((whole, fractional))
            if !whole.is_empty() && !fractional.is_empty() && !fractional.contains('.') =>
        {
            (whole, fractional)
        }
        Some(_) => return Err(FileAdapterError::InvalidDecimal),
        None if !unsigned.is_empty() => (unsigned, ""),
        None => return Err(FileAdapterError::InvalidDecimal),
    };
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let scale = i32::try_from(fractional.len())
        .map_err(|_| FileAdapterError::InvalidDecimal)?
        .checked_sub(exponent)
        .ok_or(FileAdapterError::InvalidDecimal)?;
    if scale > i32::try_from(Decimal::MAX_SCALE).map_err(|_| FileAdapterError::InvalidDecimal)? {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let extra_zeroes = if scale < 0 {
        scale
            .checked_neg()
            .ok_or(FileAdapterError::InvalidDecimal)?
    } else {
        0
    };
    if extra_zeroes
        > i32::try_from(Decimal::MAX_SCALE).map_err(|_| FileAdapterError::InvalidDecimal)?
    {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let mut canonical = String::new();
    let zeroes = usize::try_from(extra_zeroes).map_err(|_| FileAdapterError::InvalidDecimal)?;
    let capacity = usize::from(negative)
        .checked_add(whole.len())
        .and_then(|bytes| bytes.checked_add(fractional.len()))
        .and_then(|bytes| bytes.checked_add(zeroes))
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or(FileAdapterError::InvalidDecimal)?;
    canonical
        .try_reserve_exact(capacity)
        .map_err(|_| FileAdapterError::InvalidDecimal)?;
    if negative {
        canonical.push('-');
    }
    canonical.push_str(whole);
    canonical.push_str(fractional);
    canonical.extend(std::iter::repeat_n('0', zeroes));
    let mut decimal =
        Decimal::from_str_exact(&canonical).map_err(|_| FileAdapterError::InvalidDecimal)?;
    if scale > 0 {
        decimal
            .set_scale(u32::try_from(scale).map_err(|_| FileAdapterError::InvalidDecimal)?)
            .map_err(|_| FileAdapterError::InvalidDecimal)?;
    }
    Ok(decimal)
}
