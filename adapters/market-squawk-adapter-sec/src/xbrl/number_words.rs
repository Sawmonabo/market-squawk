//! SEC numwordsen: complete short-scale integer grammar, with no intermediate allocations.
//!
//! Grammar: Arelle/EDGAR 4891c4c9889c77af247428ceff4fa7cc52d534ea,
//! transform/transformationRegistry/schema/inlinexbrl-sec-transformation.xsd, numwordsenType
//! (anchored general-regex form), and transform/conf/tests.xml. The pattern includes quintillion;
//! its older annotation's billion ceiling is not the lexical contract. This implements that
//! grammar, not the more permissive text2num accumulator used after upstream validation.

use super::SecXbrlError;

const MAGNITUDES: [(&str, u128); 5] = [
    ("quintillion", 1_000_000_000_000_000_000),
    ("quadrillion", 1_000_000_000_000_000),
    ("trillion", 1_000_000_000_000),
    ("billion", 1_000_000_000),
    ("million", 1_000_000),
];

const SMALL: [(&str, u128); 27] = [
    ("one", 1),
    ("two", 2),
    ("three", 3),
    ("four", 4),
    ("five", 5),
    ("six", 6),
    ("seven", 7),
    ("eight", 8),
    ("nine", 9),
    ("ten", 10),
    ("eleven", 11),
    ("twelve", 12),
    ("thirteen", 13),
    ("fourteen", 14),
    ("fifteen", 15),
    ("sixteen", 16),
    ("seventeen", 17),
    ("eighteen", 18),
    ("nineteen", 19),
    ("twenty", 20),
    ("thirty", 30),
    ("forty", 40),
    ("fifty", 50),
    ("sixty", 60),
    ("seventy", 70),
    ("eighty", 80),
    ("ninety", 90),
];

pub(super) fn transform(value: &str) -> Result<String, SecXbrlError> {
    // Only XML whitespace is removable at the edges. Interior separators use the registry's
    // Unicode whitespace class, including NBSP. In particular, do not Unicode-trim here.
    let mut words = Words {
        rest: value.trim_matches([' ', '\t', '\r', '\n']),
    };
    if matches!(
        words.rest,
        "no" | "No" | "none" | "None" | "nil" | "Nil" | "zero" | "Zero"
    ) {
        return Ok("0".to_owned());
    }
    let invalid = || SecXbrlError::InvalidNumericFact;
    let mut total = 0_u128;
    let mut consumed = false;
    // Each magnitude appears at most once, in descending order. Each coefficient is a full
    // group, including the registry's eleven-hundred through nineteen-hundred forms.
    for (name, magnitude) in MAGNITUDES {
        let mut next = words;
        if let Some(coefficient) = next.group()
            && next.space()
            && next.named(name)
            && (next.rest.is_empty() || next.separator())
        {
            total = coefficient
                .checked_mul(magnitude)
                .and_then(|value| total.checked_add(value))
                .ok_or_else(invalid)?;
            words = next;
            consumed = true;
        }
    }
    if let Some(tail) = words.thousands_or_group() {
        total = total.checked_add(tail).ok_or_else(invalid)?;
        consumed = true;
    }
    if !consumed || !words.rest.is_empty() {
        return Err(invalid());
    }
    Ok(total.to_string())
}

/// A copied cursor provides bounded lookahead without token arrays or normalized text copies.
/// Optional grammar clauses commit their cursor only when the entire clause is valid.
#[derive(Clone, Copy)]
struct Words<'a> {
    rest: &'a str,
}

impl<'a> Words<'a> {
    fn word(&mut self) -> Option<&'a str> {
        let end = self
            .rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(self.rest.len());
        if end == 0 {
            return None;
        }
        let (word, rest) = self.rest.split_at(end);
        self.rest = rest;
        Some(word)
    }

    fn named(&mut self, expected: &str) -> bool {
        let mut next = *self;
        if next.word().is_some_and(|word| matches_word(word, expected)) {
            *self = next;
            true
        } else {
            false
        }
    }

    fn space(&mut self) -> bool {
        let rest = self.rest.trim_start_matches(char::is_whitespace);
        let consumed = rest.len() != self.rest.len();
        self.rest = rest;
        consumed
    }

    /// Registry group boundary: whitespace with an optional comma, or a comma alone.
    fn separator(&mut self) -> bool {
        let spaced = self.space();
        if let Some(rest) = self.rest.strip_prefix(',') {
            self.rest = rest;
            self.space();
            // The anchored regex permits a terminal comma after million and above, but the
            // complete upstream transform rejects it: comma replacement leaves an empty final
            // text2num token. A comma must therefore lead into another numeric clause.
            !self.rest.is_empty()
        } else {
            spaced
        }
    }

    fn small(&mut self) -> Option<u128> {
        let mut next = *self;
        let word = next.word()?;
        let value = SMALL
            .iter()
            .find_map(|(name, value)| matches_word(word, name).then_some(*value))?;
        *self = next;
        Some(value)
    }

    fn sub_hundred(&mut self) -> Option<u128> {
        let mut value = self.small()?;
        if value >= 20 {
            let mut next = *self;
            let separated = if let Some(dash) = next.rest.chars().next().filter(|c| is_dash(*c)) {
                next.rest = &next.rest[dash.len_utf8()..];
                true
            } else {
                next.space()
            };
            if separated && let Some(digit) = next.small().filter(|value| *value < 10) {
                value = value.checked_add(digit)?;
                *self = next;
            }
        }
        Some(value)
    }

    fn group(&mut self) -> Option<u128> {
        let mut hundreds = *self;
        if let Some(coefficient) = hundreds.small().filter(|value| *value < 20)
            && hundreds.space()
            && hundreds.named("hundred")
        {
            let mut value = coefficient.checked_mul(100)?;
            let mut next = hundreds;
            if next.space()
                && (!next.named("and") || next.space())
                && let Some(remainder) = next.sub_hundred()
            {
                value = value.checked_add(remainder)?;
                hundreds = next;
            }
            *self = hundreds;
            Some(value)
        } else {
            self.sub_hundred()
        }
    }

    fn thousands_or_group(&mut self) -> Option<u128> {
        let mut thousands = *self;
        if let Some(coefficient) = thousands.group()
            && thousands.space()
            && thousands.named("thousand")
        {
            let mut value = coefficient.checked_mul(1_000)?;
            let mut next = thousands;
            if next.separator() {
                // The optional post-thousand "and" introduces only a sub-hundred clause.
                // Above thousand, a following magnitude/group has no leading "and".
                let remainder = if next.named("and") {
                    if next.space() {
                        next.sub_hundred()
                    } else {
                        None
                    }
                } else {
                    next.group()
                };
                if let Some(remainder) = remainder {
                    value = value.checked_add(remainder)?;
                    thousands = next;
                }
            }
            *self = thousands;
            Some(value)
        } else {
            self.group()
        }
    }
}

fn matches_word(word: &str, expected: &str) -> bool {
    // The published pattern varies the first letter only. The connective is lowercase "and".
    word == expected
        || (expected != "and"
            && word.eq_ignore_ascii_case(expected)
            && word.get(1..) == expected.get(1..))
}

fn is_dash(character: char) -> bool {
    matches!(
        character,
        '-' | '\u{058a}'
            | '\u{05be}'
            | '\u{2010}'
            | '\u{2011}'
            | '\u{2012}'
            | '\u{2013}'
            | '\u{2014}'
            | '\u{2015}'
            | '\u{fe58}'
            | '\u{fe63}'
            | '\u{ff0d}'
    )
}
