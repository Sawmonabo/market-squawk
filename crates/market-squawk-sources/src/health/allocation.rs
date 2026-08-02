impl AuthorizationHealth {
    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Valid {
                evidence,
                valid_until: _,
            } => evidence.dynamic_retained_bytes(),
            Self::Uninitialized | Self::Invalid => Some(0),
        }
    }
}

impl CoverageHealth {
    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Sufficient {
                evidence,
                provider_product,
                provider_channel,
                valid_until: _,
            } => evidence
                .dynamic_retained_bytes()?
                .checked_add(provider_product.as_source_identifier().retained_bytes())?
                .checked_add(provider_channel.as_source_identifier().retained_bytes()),
            Self::Uninitialized | Self::Limited => Some(0),
        }
    }
}
