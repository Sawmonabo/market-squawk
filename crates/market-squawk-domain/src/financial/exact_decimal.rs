//! Exact fixed-point kernels used at financial-domain conversion boundaries.
//!
//! `rust_decimal` deliberately rounds some checked division and multiplication results to its
//! 96-bit mantissa. That behavior is useful for explicitly rounded APIs, but it cannot establish
//! exact tick, lot, money, or notional invariants. These kernels operate on mantissas and powers
//! of ten directly and discard no nonzero digit.
//!
//! Exact qualification, scaled conversion, notional, and money arithmetic use stack-only
//! checked integer operations after factor cancellation. `BigUint` is isolated to the explicitly
//! rounded provider-decimal boundary: aligning two Decimal values can require 189 bits, even when
//! the final rounded result fits `i64`. No wide integer appears in a public type, and post-
//! conversion tick/lot hot-path operations remain checked `i64` arithmetic.

use num_bigint::BigUint;
use rust_decimal::Decimal;

const MAX_DECIMAL_MANTISSA: u128 = 79_228_162_514_264_337_593_543_950_335;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RatioError {
    Inexact,
    Overflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RoundMode {
    NearestEven,
    AwayFromZero,
    TowardZero,
    Floor,
    Ceiling,
}

/// Divides two decimals and returns an integer only when the rational result is exact.
pub(super) fn exact_ratio_to_i64(value: Decimal, increment: Decimal) -> Result<i64, RatioError> {
    if value.is_zero() {
        return Ok(0);
    }

    let (is_negative, numerator, denominator) = reduced_ratio(value, increment);
    if denominator.iter().any(|factor| *factor != 1) {
        return Err(RatioError::Inexact);
    }

    let magnitude_limit = if is_negative {
        i64::MAX as u128 + 1
    } else {
        i64::MAX as u128
    };
    let magnitude = multiply_with_limit(&numerator, magnitude_limit).ok_or(RatioError::Overflow)?;
    signed_i64(magnitude, is_negative).ok_or(RatioError::Overflow)
}

/// Rounds an exact rational decimal quotient according to an explicit caller policy.
///
/// Scale alignment can require 189 bits even when the rounded answer fits in `i64` (a 96-bit
/// mantissa shifted by 28 decimal places). The exact-only path proves divisibility using factor
/// cancellation and checked `u128`; this intentionally rounded path narrowly uses `BigUint` for
/// the quotient and remainder so its pre-rounding value is never first rounded by `Decimal`.
pub(super) fn rounded_ratio_to_i64(
    value: Decimal,
    increment: Decimal,
    mode: RoundMode,
) -> Result<i64, RatioError> {
    if value.is_zero() {
        return Ok(0);
    }
    let (is_negative, numerator, denominator) = reduced_ratio(value, increment);
    let numerator = multiply_big(&numerator);
    let denominator = multiply_big(&denominator);
    let mut quotient = &numerator / &denominator;
    let remainder = numerator % &denominator;
    let has_remainder = remainder != BigUint::from(0_u8);

    let increment_magnitude = match mode {
        RoundMode::TowardZero => false,
        RoundMode::AwayFromZero => has_remainder,
        RoundMode::Floor => is_negative && has_remainder,
        RoundMode::Ceiling => !is_negative && has_remainder,
        RoundMode::NearestEven => {
            let doubled_remainder = &remainder * 2_u8;
            doubled_remainder > denominator || (doubled_remainder == denominator && quotient.bit(0))
        }
    };
    if increment_magnitude {
        quotient += 1_u8;
    }

    let magnitude = u128::try_from(quotient).map_err(|_| RatioError::Overflow)?;
    signed_i64(magnitude, is_negative).ok_or(RatioError::Overflow)
}

/// Multiplies decimals without allowing the 96-bit decimal representation to round the result.
pub(super) fn exact_product<const N: usize>(factors: [Decimal; N]) -> Result<Decimal, ()> {
    if factors.iter().any(Decimal::is_zero) {
        return Ok(Decimal::ZERO);
    }

    let is_negative = factors
        .iter()
        .filter(|factor| factor.is_sign_negative())
        .count()
        % 2
        == 1;
    let mut magnitudes = factors.map(|factor| factor.mantissa().unsigned_abs());
    let scale = factors.iter().try_fold(0_u32, |total, factor| {
        total.checked_add(factor.scale()).ok_or(())
    })?;

    let powers_of_two = magnitudes.iter().map(|factor| valuation(*factor, 2)).sum();
    let powers_of_five = magnitudes.iter().map(|factor| valuation(*factor, 5)).sum();
    let canceled_tens = scale.min(powers_of_two).min(powers_of_five);
    divide_prime_factors(&mut magnitudes, 2, canceled_tens);
    divide_prime_factors(&mut magnitudes, 5, canceled_tens);

    let normalized_scale = scale - canceled_tens;
    if normalized_scale > Decimal::MAX_SCALE {
        return Err(());
    }
    let magnitude = multiply_with_limit(&magnitudes, MAX_DECIMAL_MANTISSA).ok_or(())?;
    decimal_from_parts(magnitude, normalized_scale, is_negative)
}

/// Adds decimals after exact scale alignment, returning an error instead of rounded carry loss.
pub(super) fn exact_add(left: Decimal, right: Decimal) -> Result<Decimal, ()> {
    exact_add_or_subtract(left, right, false)
}

/// Subtracts decimals after exact scale alignment, returning an error instead of rounded borrow.
pub(super) fn exact_subtract(left: Decimal, right: Decimal) -> Result<Decimal, ()> {
    exact_add_or_subtract(left, right, true)
}

fn exact_add_or_subtract(left: Decimal, right: Decimal, subtract: bool) -> Result<Decimal, ()> {
    let scale = left.scale().max(right.scale());
    let left_mantissa = align_mantissa(left, scale)?;
    let right_mantissa = align_mantissa(right, scale)?;
    let result = if subtract {
        left_mantissa.checked_sub(right_mantissa)
    } else {
        left_mantissa.checked_add(right_mantissa)
    }
    .ok_or(())?;
    normalized_decimal(result, scale)
}

fn align_mantissa(value: Decimal, target_scale: u32) -> Result<i128, ()> {
    let exponent = target_scale.checked_sub(value.scale()).ok_or(())?;
    value
        .mantissa()
        .checked_mul(10_i128.checked_pow(exponent).ok_or(())?)
        .ok_or(())
}

fn normalized_decimal(mut mantissa: i128, mut scale: u32) -> Result<Decimal, ()> {
    while scale > 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|_| ())
}

fn decimal_from_parts(magnitude: u128, scale: u32, is_negative: bool) -> Result<Decimal, ()> {
    let mantissa = i128::try_from(magnitude).map_err(|_| ())?;
    let signed = if is_negative { -mantissa } else { mantissa };
    Decimal::try_from_i128_with_scale(signed, scale).map_err(|_| ())
}

fn signed_i64(magnitude: u128, is_negative: bool) -> Option<i64> {
    if is_negative && magnitude == i64::MAX as u128 + 1 {
        return Some(i64::MIN);
    }
    let value = i64::try_from(magnitude).ok()?;
    Some(if is_negative { -value } else { value })
}

fn reduced_ratio(value: Decimal, increment: Decimal) -> (bool, [u128; 3], [u128; 3]) {
    let is_negative = value.is_sign_negative() != increment.is_sign_negative();
    let mut numerator = [value.mantissa().unsigned_abs(), 1, 1];
    let mut denominator = [increment.mantissa().unsigned_abs(), 1, 1];

    match increment.scale().cmp(&value.scale()) {
        std::cmp::Ordering::Greater => {
            let exponent = increment.scale() - value.scale();
            numerator[1] = 2_u128.pow(exponent);
            numerator[2] = 5_u128.pow(exponent);
        }
        std::cmp::Ordering::Less => {
            let exponent = value.scale() - increment.scale();
            denominator[1] = 2_u128.pow(exponent);
            denominator[2] = 5_u128.pow(exponent);
        }
        std::cmp::Ordering::Equal => {}
    }

    cancel_factors(&mut numerator, &mut denominator);
    (is_negative, numerator, denominator)
}

fn multiply_big<const N: usize>(factors: &[u128; N]) -> BigUint {
    factors.iter().fold(BigUint::from(1_u8), |product, factor| {
        product * BigUint::from(*factor)
    })
}

fn cancel_factors<const N: usize, const D: usize>(
    numerator: &mut [u128; N],
    denominator: &mut [u128; D],
) {
    for numerator_factor in numerator {
        for denominator_factor in &mut *denominator {
            let divisor = greatest_common_divisor(*numerator_factor, *denominator_factor);
            *numerator_factor /= divisor;
            *denominator_factor /= divisor;
        }
    }
}

fn greatest_common_divisor(mut left: u128, mut right: u128) -> u128 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn valuation(mut value: u128, prime: u128) -> u32 {
    let mut exponent = 0;
    while value.is_multiple_of(prime) {
        value /= prime;
        exponent += 1;
    }
    exponent
}

fn divide_prime_factors<const N: usize>(factors: &mut [u128; N], prime: u128, mut exponent: u32) {
    for factor in factors {
        while exponent > 0 && factor.is_multiple_of(prime) {
            *factor /= prime;
            exponent -= 1;
        }
    }
}

fn multiply_with_limit<const N: usize>(factors: &[u128; N], limit: u128) -> Option<u128> {
    factors.iter().try_fold(1_u128, |product, factor| {
        product.checked_mul(*factor).filter(|value| *value <= limit)
    })
}
