/// Exact and structurally dynamic endpoint allowlist with hardened request bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointPolicy {
    endpoints: BoundedVec<NormalizedEndpoint, MAX_ENDPOINTS>,
    api_rules: BoundedVec<ApiEndpointRule, MAX_API_RULES>,
    request_bounds: HttpRequestBounds,
    client_profile: HttpClientProfile,
}

impl EndpointPolicy {
    /// Creates an allowlist with hardened default request bounds.
    ///
    /// # Errors
    ///
    /// Rejects an empty/oversized set and any insecure, ambiguous, or invalid endpoint.
    pub fn try_new<I, S>(endpoints: I) -> Result<Self, NetworkPolicyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::try_new_with_bounds(endpoints, HttpRequestBounds::default())
    }

    /// Creates an allowlist with explicit hardened request bounds.
    ///
    /// # Errors
    ///
    /// Rejects an empty/oversized set, duplicates, or an invalid endpoint.
    pub fn try_new_with_bounds<I, S>(
        endpoints: I,
        request_bounds: HttpRequestBounds,
    ) -> Result<Self, NetworkPolicyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = Vec::new();
        for endpoint in endpoints {
            if normalized.len() == MAX_ENDPOINTS {
                return Err(NetworkPolicyError::TooManyEndpoints { max: MAX_ENDPOINTS });
            }
            let endpoint = NormalizedEndpoint::parse(endpoint.as_ref())?;
            if normalized.contains(&endpoint) {
                return Err(NetworkPolicyError::DuplicateEndpoint);
            }
            normalized.push(endpoint);
        }
        if normalized.is_empty() {
            return Err(NetworkPolicyError::EmptyEndpointSet);
        }
        Ok(Self {
            endpoints: BoundedVec::try_new(normalized)
                .map_err(|error| NetworkPolicyError::TooManyEndpoints { max: error.max })?,
            api_rules: BoundedVec::empty(),
            request_bounds,
            client_profile: HttpClientProfile::hardened(),
        })
    }

    /// Creates a policy for one or more bounded dynamic API rules.
    ///
    /// # Errors
    ///
    /// Rejects an empty or oversized rule set.
    pub fn try_from_api_rules(
        rules: Vec<ApiEndpointRule>,
        request_bounds: HttpRequestBounds,
    ) -> Result<Self, NetworkPolicyError> {
        if rules.is_empty() {
            return Err(NetworkPolicyError::EmptyEndpointSet);
        }
        let api_rules = BoundedVec::try_new(rules)
            .map_err(|error| NetworkPolicyError::TooManyEndpoints { max: error.max })?;
        Ok(Self {
            endpoints: BoundedVec::empty(),
            api_rules,
            request_bounds,
            client_profile: HttpClientProfile::hardened(),
        })
    }

    /// Authorizes an initial request only when every normalized component exactly matches.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::EndpointDenied`] for any non-allowlisted target.
    pub fn authorize(&self, target: &str) -> Result<(), NetworkPolicyError> {
        self.authorize_request(target).map(|_| ())
    }

    /// Fully authorizes a final target and returns only redacted sensitivity metadata.
    ///
    /// # Errors
    ///
    /// Rejects every target not matched by an exact endpoint or structural API rule.
    pub fn authorize_request(&self, target: &str) -> Result<AuthorizedRequest, NetworkPolicyError> {
        let normalized = NormalizedEndpoint::parse_target(target)?;
        let url = Url::parse(target).map_err(|_| NetworkPolicyError::EndpointDenied {
            reason: EndpointDenialReason::Invalid,
        })?;
        if url.query().is_none() && self.endpoints.as_slice().contains(&normalized) {
            return Ok(AuthorizedRequest {
                contains_sensitive_query: false,
            });
        }
        for rule in self.api_rules.as_slice() {
            match rule.authorize(target, &normalized) {
                Ok(contains_sensitive_query) => {
                    return Ok(AuthorizedRequest {
                        contains_sensitive_query,
                    });
                }
                Err(NetworkPolicyError::EndpointDenied { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        {
            Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::NotAllowlisted,
            })
        }
    }

    /// Revalidates a redirect against the same exact allowlist.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::EndpointDenied`] rather than allowing a host transition.
    pub fn authorize_redirect(&self, target: &str) -> Result<(), NetworkPolicyError> {
        self.authorize(target)
    }

    /// Checks a declared or accumulated response size before further buffering.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::ResponseTooLarge`] above the configured byte ceiling.
    pub fn validate_response_size(&self, size: u64) -> Result<(), NetworkPolicyError> {
        let max = self.request_bounds.max_response_bytes();
        if size > max {
            Err(NetworkPolicyError::ResponseTooLarge { actual: size, max })
        } else {
            Ok(())
        }
    }

    /// Returns explicit HTTP request bounds.
    pub const fn request_bounds(&self) -> HttpRequestBounds {
        self.request_bounds
    }

    /// Returns mandatory client-construction flags.
    pub const fn client_profile(&self) -> HttpClientProfile {
        self.client_profile
    }

    pub(crate) fn canonical_network_authorities(
        &self,
    ) -> Result<BoundedVec<CanonicalNetworkAuthority, MAX_CANONICAL_AUTHORITIES>, NetworkPolicyError>
    {
        let mut authorities = self
            .endpoints
            .as_slice()
            .iter()
            .map(NormalizedEndpoint::canonical_network_authority)
            .chain(
                self.api_rules
                    .as_slice()
                    .iter()
                    .map(|rule| rule.base.canonical_network_authority()),
            )
            .collect::<Vec<_>>();
        authorities.sort_unstable();
        authorities.dedup();
        if authorities.is_empty() {
            return Err(NetworkPolicyError::EmptyEndpointSet);
        }
        BoundedVec::try_new(authorities)
            .map_err(|error| NetworkPolicyError::TooManyEndpoints { max: error.max })
    }

    /// Reauthorizes a redirect and decides whether sensitive headers may be forwarded.
    ///
    /// # Errors
    ///
    /// Cross-origin redirects are rejected even if both origins are separately allowlisted.
    pub fn authorize_redirect_from(
        &self,
        previous: &str,
        target: &str,
        carried_sensitive_headers: bool,
    ) -> Result<RedirectAuthorization, NetworkPolicyError> {
        let previous_normalized = NormalizedEndpoint::parse_target(previous)?;
        let target_normalized = NormalizedEndpoint::parse_target(target)?;
        self.authorize_request(previous)?;
        let target_authorization = self.authorize_request(target)?;
        if !previous_normalized.same_origin(&target_normalized) {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::NotAllowlisted,
            });
        }
        Ok(RedirectAuthorization {
            forward_sensitive_headers: carried_sensitive_headers,
            target_contains_sensitive_query: target_authorization.contains_sensitive_query,
        })
    }
}

fn endpoint_text_is_ambiguous(value: &str) -> bool {
    if value.contains('\\')
        || value
            .chars()
            .any(|character| character.is_ascii_control() || character.is_ascii_whitespace())
    {
        return true;
    }
    let lower = value.to_ascii_lowercase();
    let without_query = lower.split(['?', '#']).next().unwrap_or(&lower);
    let path = without_query
        .split_once("://")
        .and_then(|(_, remainder)| remainder.find('/').map(|index| &remainder[index..]))
        .unwrap_or("/");
    path.contains("%2e")
        || path.contains("%2f")
        || path.contains("%5c")
        || path.contains("%25")
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
}

/// Redacted result of final target authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorizedRequest {
    contains_sensitive_query: bool,
}

impl AuthorizedRequest {
    /// Returns whether an allowlisted secret query key was present, never its value.
    pub const fn contains_sensitive_query(self) -> bool {
        self.contains_sensitive_query
    }
}

/// Sensitive forwarding decision for one fully reauthorized redirect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedirectAuthorization {
    forward_sensitive_headers: bool,
    target_contains_sensitive_query: bool,
}

impl RedirectAuthorization {
    /// Returns whether same-origin sensitive headers may be forwarded.
    pub const fn forward_sensitive_headers(self) -> bool {
        self.forward_sensitive_headers
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct EndpointPolicySerializeWire {
    endpoints: Vec<EndpointText>,
    api_rules: BoundedVec<ApiEndpointRule, MAX_API_RULES>,
    request_bounds: HttpRequestBounds,
    client_profile: HttpClientProfile,
}

impl Serialize for EndpointPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let endpoints = self
            .endpoints
            .as_slice()
            .iter()
            .map(NormalizedEndpoint::canonical_string)
            .map(EndpointText::try_new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::ser::Error::custom)?;
        EndpointPolicySerializeWire {
            endpoints,
            api_rules: self.api_rules.clone(),
            request_bounds: self.request_bounds,
            client_profile: self.client_profile,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointPolicyWire {
    endpoints: BoundedVec<EndpointText, MAX_ENDPOINTS>,
    api_rules: BoundedVec<ApiEndpointRule, MAX_API_RULES>,
    request_bounds: HttpRequestBounds,
    client_profile: HttpClientProfile,
}

impl<'de> Deserialize<'de> for EndpointPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EndpointPolicyWire::deserialize(deserializer)?;
        if wire.endpoints.is_empty() && wire.api_rules.is_empty() {
            return Err(serde::de::Error::custom(
                NetworkPolicyError::EmptyEndpointSet,
            ));
        }
        let mut exact = Vec::new();
        for endpoint in wire.endpoints.as_slice() {
            let parsed =
                NormalizedEndpoint::parse(endpoint.as_str()).map_err(serde::de::Error::custom)?;
            if exact.contains(&parsed) {
                return Err(serde::de::Error::custom(
                    NetworkPolicyError::DuplicateEndpoint,
                ));
            }
            exact.push(parsed);
        }
        Ok(Self {
            endpoints: BoundedVec::try_new(exact).map_err(serde::de::Error::custom)?,
            api_rules: wire.api_rules,
            request_bounds: wire.request_bounds,
            client_profile: wire.client_profile,
        })
    }
}
