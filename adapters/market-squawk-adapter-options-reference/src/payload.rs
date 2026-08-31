use std::io::{ErrorKind, Read};

use market_squawk_domain::DigestAlgorithm;
use sha2::{Digest, Sha256};

use crate::ReferenceObjectContext;

const PAYLOAD_READ_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum TokenBoundary {
    Line,
    Csv,
    Markup,
    Json,
}

#[derive(Clone, Copy)]
enum XmlLexicalState {
    Text,
    AfterLessThan,
    Tag {
        quote: Option<u8>,
    },
    ProcessingInstruction {
        quote: Option<u8>,
        previous_question: bool,
    },
    AfterBang,
    AfterBangDash,
    CdataOpening {
        matched: usize,
    },
    Comment {
        trailing_hyphens: u8,
    },
    Cdata {
        trailing_brackets: u8,
    },
}

/// Reader-side token ceiling applied before a parser library can grow its own event buffer.
pub(crate) struct BoundedTokenReader<R> {
    inner: R,
    boundary: TokenBoundary,
    maximum_span_bytes: usize,
    span_bytes: usize,
    json_in_string: bool,
    json_escaped: bool,
    csv_in_quotes: bool,
    xml_state: XmlLexicalState,
}

impl<R> BoundedTokenReader<R>
where
    R: Read,
{
    pub(crate) fn lines(inner: R, maximum_span_bytes: usize) -> Result<Self, std::io::Error> {
        Self::try_new(inner, TokenBoundary::Line, maximum_span_bytes)
    }

    pub(crate) fn markup(inner: R, maximum_span_bytes: usize) -> Result<Self, std::io::Error> {
        Self::try_new(inner, TokenBoundary::Markup, maximum_span_bytes)
    }

    pub(crate) fn csv(inner: R, maximum_span_bytes: usize) -> Result<Self, std::io::Error> {
        Self::try_new(inner, TokenBoundary::Csv, maximum_span_bytes)
    }

    pub(crate) fn json(inner: R, maximum_span_bytes: usize) -> Result<Self, std::io::Error> {
        Self::try_new(inner, TokenBoundary::Json, maximum_span_bytes)
    }

    pub(crate) fn into_inner(self) -> R {
        self.inner
    }

    fn try_new(
        inner: R,
        boundary: TokenBoundary,
        maximum_span_bytes: usize,
    ) -> Result<Self, std::io::Error> {
        if maximum_span_bytes == 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "provider token bound must be nonzero",
            ));
        }
        Ok(Self {
            inner,
            boundary,
            maximum_span_bytes,
            span_bytes: 0,
            json_in_string: false,
            json_escaped: false,
            csv_in_quotes: false,
            xml_state: XmlLexicalState::Text,
        })
    }

    fn observe(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        for byte in bytes {
            match self.boundary {
                TokenBoundary::Line => {
                    if *byte == b'\n' {
                        self.span_bytes = 0;
                    } else {
                        self.increment_span()?;
                    }
                }
                TokenBoundary::Csv => {
                    if *byte == b'"' {
                        self.csv_in_quotes = !self.csv_in_quotes;
                        self.increment_span()?;
                    } else if *byte == b'\n' && !self.csv_in_quotes {
                        self.span_bytes = 0;
                    } else {
                        self.increment_span()?;
                    }
                }
                TokenBoundary::Markup => self.observe_xml_byte(*byte)?,
                TokenBoundary::Json if self.json_in_string => {
                    self.increment_span()?;
                    if self.json_escaped {
                        self.json_escaped = false;
                    } else if *byte == b'\\' {
                        self.json_escaped = true;
                    } else if *byte == b'"' {
                        self.json_in_string = false;
                        self.span_bytes = 0;
                    }
                }
                TokenBoundary::Json => {
                    if *byte == b'"' {
                        self.json_in_string = true;
                        self.json_escaped = false;
                        self.span_bytes = 0;
                    } else if byte.is_ascii_whitespace()
                        || matches!(*byte, b'{' | b'}' | b'[' | b']' | b',' | b':')
                    {
                        self.span_bytes = 0;
                    } else {
                        self.increment_span()?;
                    }
                }
            }
        }
        Ok(())
    }

    fn observe_xml_byte(&mut self, byte: u8) -> Result<(), std::io::Error> {
        match self.xml_state {
            XmlLexicalState::Text => {
                if byte == b'<' {
                    self.span_bytes = 0;
                    self.increment_span()?;
                    self.xml_state = XmlLexicalState::AfterLessThan;
                } else {
                    self.increment_span()?;
                }
            }
            XmlLexicalState::AfterLessThan => {
                self.increment_span()?;
                self.xml_state = match byte {
                    b'!' => XmlLexicalState::AfterBang,
                    b'?' => XmlLexicalState::ProcessingInstruction {
                        quote: None,
                        previous_question: false,
                    },
                    b'>' => {
                        self.finish_xml_event();
                        return Ok(());
                    }
                    b'\'' | b'"' => XmlLexicalState::Tag { quote: Some(byte) },
                    _ => XmlLexicalState::Tag { quote: None },
                };
            }
            XmlLexicalState::Tag { quote } => {
                self.increment_span()?;
                match quote {
                    Some(delimiter) if byte == delimiter => {
                        self.xml_state = XmlLexicalState::Tag { quote: None };
                    }
                    Some(_) => {}
                    None if matches!(byte, b'\'' | b'"') => {
                        self.xml_state = XmlLexicalState::Tag { quote: Some(byte) };
                    }
                    None if byte == b'>' => self.finish_xml_event(),
                    None => {}
                }
            }
            XmlLexicalState::ProcessingInstruction {
                quote,
                previous_question,
            } => {
                self.increment_span()?;
                match quote {
                    Some(delimiter) if byte == delimiter => {
                        self.xml_state = XmlLexicalState::ProcessingInstruction {
                            quote: None,
                            previous_question: false,
                        };
                    }
                    Some(_) => {
                        self.xml_state = XmlLexicalState::ProcessingInstruction {
                            quote,
                            previous_question: false,
                        };
                    }
                    None if matches!(byte, b'\'' | b'"') => {
                        self.xml_state = XmlLexicalState::ProcessingInstruction {
                            quote: Some(byte),
                            previous_question: false,
                        };
                    }
                    None if byte == b'>' && previous_question => self.finish_xml_event(),
                    None => {
                        self.xml_state = XmlLexicalState::ProcessingInstruction {
                            quote: None,
                            previous_question: byte == b'?',
                        };
                    }
                }
            }
            XmlLexicalState::AfterBang => {
                self.increment_span()?;
                self.xml_state = match byte {
                    b'-' => XmlLexicalState::AfterBangDash,
                    b'[' => XmlLexicalState::CdataOpening { matched: 0 },
                    _ => return Err(invalid_xml_markup()),
                };
            }
            XmlLexicalState::AfterBangDash => {
                self.increment_span()?;
                self.xml_state = if byte == b'-' {
                    XmlLexicalState::Comment {
                        trailing_hyphens: 0,
                    }
                } else {
                    return Err(invalid_xml_markup());
                };
            }
            XmlLexicalState::CdataOpening { matched } => {
                const CDATA_OPENING_REMAINDER: &[u8] = b"CDATA[";
                self.increment_span()?;
                if CDATA_OPENING_REMAINDER.get(matched) == Some(&byte) {
                    self.xml_state = if matched + 1 == CDATA_OPENING_REMAINDER.len() {
                        XmlLexicalState::Cdata {
                            trailing_brackets: 0,
                        }
                    } else {
                        XmlLexicalState::CdataOpening {
                            matched: matched + 1,
                        }
                    };
                } else {
                    return Err(invalid_xml_markup());
                }
            }
            XmlLexicalState::Comment { trailing_hyphens } => {
                self.increment_span()?;
                if byte == b'>' && trailing_hyphens >= 2 {
                    self.finish_xml_event();
                } else {
                    self.xml_state = XmlLexicalState::Comment {
                        trailing_hyphens: if byte == b'-' {
                            trailing_hyphens.saturating_add(1).min(2)
                        } else {
                            0
                        },
                    };
                }
            }
            XmlLexicalState::Cdata { trailing_brackets } => {
                self.increment_span()?;
                if byte == b'>' && trailing_brackets >= 2 {
                    self.finish_xml_event();
                } else {
                    self.xml_state = XmlLexicalState::Cdata {
                        trailing_brackets: if byte == b']' {
                            trailing_brackets.saturating_add(1).min(2)
                        } else {
                            0
                        },
                    };
                }
            }
        }
        Ok(())
    }

    fn finish_xml_event(&mut self) {
        self.span_bytes = 0;
        self.xml_state = XmlLexicalState::Text;
    }

    fn increment_span(&mut self) -> Result<(), std::io::Error> {
        self.span_bytes = self.span_bytes.checked_add(1).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "provider token length overflowed")
        })?;
        if self.span_bytes > self.maximum_span_bytes {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "provider token exceeded its parser bound",
            ));
        }
        Ok(())
    }
}

fn invalid_xml_markup() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::InvalidData,
        "unsupported XML markup in closed provider schema",
    )
}

impl<R> Read for BoundedTokenReader<R>
where
    R: Read,
{
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let remaining = self
            .maximum_span_bytes
            .checked_sub(self.span_bytes)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or_else(|| {
                std::io::Error::new(ErrorKind::InvalidData, "provider token bound overflowed")
            })?;
        let admitted = buffer.len().min(PAYLOAD_READ_CHUNK_BYTES).min(remaining);
        let read = self.inner.read(&mut buffer[..admitted])?;
        self.observe(&buffer[..read])?;
        Ok(read)
    }
}

/// Exact terminal framing observed while a provider parser consumed one complete raw object.
pub(crate) struct ExactPayloadEvidence {
    contains_nul: bool,
    last_byte: Option<u8>,
    penultimate_byte: Option<u8>,
    only_crlf_line_endings: bool,
}

impl ExactPayloadEvidence {
    pub(crate) const fn contains_nul(&self) -> bool {
        self.contains_nul
    }

    pub(crate) const fn ends_with_lf(&self) -> bool {
        matches!(self.last_byte, Some(b'\n'))
    }

    pub(crate) const fn has_only_complete_crlf_records(&self) -> bool {
        self.only_crlf_line_endings
            && matches!(
                (self.penultimate_byte, self.last_byte),
                (Some(b'\r'), Some(b'\n'))
            )
    }
}

/// Bounded hashing reader that proves a parser consumed the exact context-bound object.
pub(crate) struct ExactPayloadReader<R> {
    inner: R,
    expected_bytes: u64,
    expected_digest: [u8; 32],
    observed_bytes: u64,
    hasher: Sha256,
    saw_eof: bool,
    contains_nul: bool,
    last_byte: Option<u8>,
    penultimate_byte: Option<u8>,
    only_crlf_line_endings: bool,
}

impl<R> ExactPayloadReader<R>
where
    R: Read,
{
    pub(crate) fn try_new(
        inner: R,
        context: &ReferenceObjectContext,
        maximum_bytes: usize,
    ) -> Result<Self, std::io::Error> {
        let maximum_bytes = u64::try_from(maximum_bytes)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))?;
        if context.payload_bytes() == 0
            || context.payload_bytes() > maximum_bytes
            || context.payload_digest().algorithm() != DigestAlgorithm::Sha256
        {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "invalid exact provider payload evidence",
            ));
        }
        Ok(Self {
            inner,
            expected_bytes: context.payload_bytes(),
            expected_digest: context.payload_digest().bytes(),
            observed_bytes: 0,
            hasher: Sha256::new(),
            saw_eof: false,
            contains_nul: false,
            last_byte: None,
            penultimate_byte: None,
            only_crlf_line_endings: true,
        })
    }

    pub(crate) fn finish(mut self) -> Result<ExactPayloadEvidence, std::io::Error> {
        let mut buffer = [0_u8; PAYLOAD_READ_CHUNK_BYTES];
        while self.read(&mut buffer)? != 0 {}
        if !self.saw_eof
            || self.observed_bytes != self.expected_bytes
            || <[u8; 32]>::from(self.hasher.finalize()) != self.expected_digest
        {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "provider payload does not match exact retained evidence",
            ));
        }
        let only_crlf_line_endings =
            self.only_crlf_line_endings && !matches!(self.last_byte, Some(b'\r'));
        Ok(ExactPayloadEvidence {
            contains_nul: self.contains_nul,
            last_byte: self.last_byte,
            penultimate_byte: self.penultimate_byte,
            only_crlf_line_endings,
        })
    }

    fn observe(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let bytes_len = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
        self.observed_bytes = self
            .observed_bytes
            .checked_add(bytes_len)
            .filter(|observed| *observed <= self.expected_bytes)
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    "provider payload exceeded retained evidence",
                )
            })?;
        self.hasher.update(bytes);
        for byte in bytes {
            if *byte == 0 {
                self.contains_nul = true;
            }
            if (*byte == b'\n' && self.last_byte != Some(b'\r'))
                || (self.last_byte == Some(b'\r') && *byte != b'\n')
            {
                self.only_crlf_line_endings = false;
            }
            self.penultimate_byte = self.last_byte;
            self.last_byte = Some(*byte);
        }
        Ok(())
    }
}

impl<R> Read for ExactPayloadReader<R>
where
    R: Read,
{
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.saw_eof {
            return Ok(0);
        }
        let remaining = self
            .expected_bytes
            .checked_sub(self.observed_bytes)
            .ok_or_else(|| {
                std::io::Error::new(ErrorKind::InvalidData, "provider payload length overflowed")
            })?;
        if remaining == 0 {
            let mut trailing = [0_u8; 1];
            if self.inner.read(&mut trailing)? != 0 {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "provider payload exceeded retained evidence",
                ));
            }
            self.saw_eof = true;
            return Ok(0);
        }
        let admitted = buffer
            .len()
            .min(PAYLOAD_READ_CHUNK_BYTES)
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buffer[..admitted])?;
        if read == 0 {
            return Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "provider payload ended before retained evidence",
            ));
        }
        self.observe(&buffer[..read])?;
        Ok(read)
    }
}
