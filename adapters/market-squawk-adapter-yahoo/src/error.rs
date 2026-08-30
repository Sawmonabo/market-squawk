use thiserror::Error;

/// Fail-closed construction and parsing failures at the Yahoo adapter boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum YahooAdapterError {
    #[error("explicit demand id must not be empty")]
    EmptyDemandId,
    #[error("explicit demand id exceeds the application string bound")]
    DemandIdTooLong,
    #[error("Yahoo symbol must not be empty")]
    EmptySymbol,
    #[error("Yahoo symbol exceeds the application string bound")]
    SymbolTooLong,
    #[error("Yahoo symbol contains an unsafe delimiter or control character")]
    InvalidSymbol,
    #[error("duplicate Yahoo symbol in one request plan: {0}")]
    DuplicateSymbol(String),
    #[error("request must contain at least one selected symbol")]
    EmptySymbolSet,
    #[error("application bound `{name}` must be greater than zero")]
    ZeroApplicationBound { name: &'static str },
    #[error("application bound `{name}` exceeded: actual {actual}, maximum {maximum}")]
    ApplicationBoundExceeded {
        name: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("Yahoo fallback cooldown plus jitter exceeds its integer bound")]
    InvalidFallbackCooldown,
    #[error("locale value is empty or contains an unsafe character")]
    InvalidLocale,
    #[error("search text is empty")]
    EmptySearchText,
    #[error("history start must be strictly earlier than history end")]
    InvalidHistoryWindow,
    #[error("option expiration must be a positive Unix timestamp")]
    InvalidOptionExpiration,
    #[error("request family does not match this parser")]
    WrongRequestFamily,
    #[error("response exceeds the admitted byte bound: actual {actual}, maximum {maximum}")]
    ResponseTooLarge { actual: usize, maximum: usize },
    #[error("response is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("provider response envelope is missing `{0}`")]
    MissingEnvelope(&'static str),
    #[error("provider response schema is invalid at `{path}`: {reason}")]
    InvalidSchema { path: String, reason: String },
    #[error("provider returned an unexpected symbol: {0}")]
    UnexpectedSymbol(String),
    #[error("provider returned a duplicate record identity: {0}")]
    DuplicateReturnedIdentity(String),
    #[error("numeric field `{path}` is outside the admitted representation")]
    InvalidNumber { path: String },
    #[error("string field `{path}` exceeds the application string bound")]
    StringTooLong { path: String },
    #[error("provider returned an unsupported crypto asset")]
    UnsupportedCryptoAsset,
    #[error("URL construction failed: {0}")]
    InvalidUrl(String),
}
