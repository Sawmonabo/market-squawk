//! Closed shared native workflow requests and the existing Desktop presentation DTOs.
use super::{
    bounded_array as array, closed, closed_complete as object, constant, enumeration, nullable,
    one_of, unsigned, uuid,
};
use serde_json::{Value, json};
fn text(max: usize) -> Value {
    json!({"type":"string","minLength":1,"maxLength":max})
}
fn token(prefix: &str) -> Value {
    json!({"type":"string","pattern":format!("^{prefix}_[0-9a-f]{{32}}$")})
}
fn nanos() -> Value {
    json!({"type":"string","pattern":"^[1-9][0-9]*$","maxLength":20})
}
fn boolean() -> Value {
    json!({"type":"boolean"})
}
fn count(max: u64) -> Value {
    json!({"type":"integer","minimum":0,"maximum":max})
}
fn scope() -> Value {
    enumeration(&["focused", "balanced", "broad"])
}
fn cursor() -> Value {
    object(vec![
        ("partition", nullable(count(65_535))),
        ("offset", count(65_536)),
    ])
}
fn financial_input() -> Value {
    object(vec![
        ("coverage", enumeration(&["stocks_and_etfs", "stocks"])),
        ("modelChoice", text(64)),
        ("allowRetrospectiveStudies", boolean()),
        (
            "fields",
            array(object(vec![("key", text(128)), ("value", text(32))]), 64),
        ),
    ])
}
fn command_variant(action: &str, mut fields: Vec<(&str, Value)>) -> Value {
    fields.insert(0, ("action", constant(action)));
    object(fields)
}
pub(in crate::application::contracts) fn command(update: bool) -> Value {
    if update {
        one_of(vec![
            command_variant("copyRecommended", vec![("displayName", text(64))]),
            command_variant(
                "updateProfile",
                vec![
                    ("profileToken", token("profile")),
                    ("profileStateToken", token("state")),
                    ("displayName", text(64)),
                    ("analysisScope", scope()),
                    ("financialPreferences", financial_input()),
                ],
            ),
            command_variant(
                "validateProfile",
                vec![
                    ("profileToken", token("profile")),
                    ("profileStateToken", token("state")),
                ],
            ),
            command_variant(
                "activateProfile",
                vec![
                    ("profileToken", token("profile")),
                    ("profileStateToken", token("state")),
                    ("validationToken", token("validation")),
                    ("activationToken", token("activation")),
                ],
            ),
            command_variant(
                "restoreRecommended",
                vec![("activationToken", token("activation"))],
            ),
            closed(
                vec![
                    ("action", constant("findOpportunities")),
                    ("benchmarkInstrumentId", uuid()),
                ],
                &["action"],
            ),
            closed(
                vec![
                    ("action", constant("analyzeInvestment")),
                    ("selectionToken", super::market_token("market")),
                    ("benchmarkInstrumentId", uuid()),
                ],
                &["action", "selectionToken"],
            ),
            command_variant("resumeWorkflow", vec![("workflowToken", token("workflow"))]),
            command_variant("cancelWorkflow", vec![("workflowToken", token("workflow"))]),
        ])
    } else {
        one_of(vec![
            command_variant("status", vec![]),
            closed(
                vec![
                    ("action", constant("profileOptions")),
                    ("cursor", nullable(text(512))),
                    ("limit", json!({"type":"integer","minimum":1,"maximum":100})),
                ],
                &["action"],
            ),
            command_variant(
                "compareWithRecommended",
                vec![("profileToken", token("profile"))],
            ),
            closed(
                vec![
                    ("action", constant("history")),
                    ("afterToken", nullable(token("history"))),
                    ("limit", json!({"type":"integer","minimum":1,"maximum":100})),
                ],
                &["action", "limit"],
            ),
            closed(
                vec![
                    ("action", constant("workflowCoverage")),
                    ("workflowToken", token("workflow")),
                    ("after", nullable(cursor())),
                ],
                &["action", "workflowToken"],
            ),
        ])
    }
}
fn difference() -> Value {
    object(vec![("label", text(64)), ("explanation", text(256))])
}
fn preferences() -> Value {
    object(vec![
        ("coverage", enumeration(&["stocks_and_etfs", "stocks"])),
        ("modelChoice", text(64)),
        ("allowRetrospectiveStudies", boolean()),
        (
            "fields",
            array(
                object(vec![
                    ("key", text(128)),
                    ("label", text(96)),
                    ("group", text(64)),
                    ("value", text(32)),
                    ("unit", enumeration(&["", "%", "seconds", "daily returns"])),
                    (
                        "choices",
                        array(object(vec![("value", text(64)), ("label", text(64))]), 5),
                    ),
                ]),
                64,
            ),
        ),
    ])
}
fn profile() -> Value {
    object(vec![
        ("profileToken", token("profile")),
        ("profileStateToken", token("state")),
        ("displayName", text(64)),
        (
            "version",
            json!({"type":"integer","minimum":1,"maximum":4294967295u64}),
        ),
        ("mode", enumeration(&["recommended", "custom"])),
        ("active", boolean()),
        (
            "validation",
            object(vec![
                (
                    "state",
                    enumeration(&["built_in", "needs_validation", "unavailable", "validated"]),
                ),
                ("label", text(64)),
                ("explanation", text(256)),
                ("validatedAt", nullable(nanos())),
            ]),
        ),
        ("validationToken", nullable(token("validation"))),
        ("activationToken", nullable(token("activation"))),
        ("differencesFromRecommended", array(difference(), 11)),
        ("createdAt", nanos()),
        ("updatedAt", nanos()),
        ("activatedAt", nullable(nanos())),
        ("canValidate", boolean()),
        ("canActivate", boolean()),
        ("canRestoreRecommended", boolean()),
        ("canEdit", boolean()),
        ("analysisScope", scope()),
        ("financialPreferences", preferences()),
    ])
}
fn availability() -> Value {
    object(vec![
        ("state", enumeration(&["available", "unavailable"])),
        ("explanation", text(512)),
        ("nextAction", text(256)),
    ])
}
fn workflow() -> Value {
    object(vec![
        ("workflowToken", token("workflow")),
        (
            "kind",
            enumeration(&[
                "opportunity_discovery",
                "investment_analysis",
                "track_record_refresh",
            ]),
        ),
        (
            "state",
            enumeration(&[
                "waiting",
                "in_progress",
                "paused",
                "cancelling",
                "complete",
                "cancelled",
                "unavailable",
            ]),
        ),
        (
            "progress",
            object(vec![
                (
                    "stage",
                    enumeration(&[
                        "preparing",
                        "gathering_evidence",
                        "building_results",
                        "finalizing",
                        "complete",
                        "unavailable",
                    ]),
                ),
                ("completedSteps", count(8192)),
                ("waitingForBackgroundWork", boolean()),
            ]),
        ),
        (
            "coverage",
            nullable(object(vec![
                ("completeness", enumeration(&["complete", "partial"])),
                ("searched", unsigned()),
                ("population", unsigned()),
                ("inputUnavailable", unsigned()),
                ("excluded", unsigned()),
                ("deeplyAnalyzed", unsigned()),
                ("generated", unsigned()),
                ("noAction", unsigned()),
                ("unavailable", unsigned()),
            ])),
        ),
        ("resultCount", count(128)),
        ("resultActionTokens", array(uuid(), 128)),
        (
            "resultOrdering",
            nullable(enumeration(&[
                "estimated_gain_descending",
                "unavailable_incomparable_horizons",
            ])),
        ),
        (
            "unavailableMembers",
            array(
                object(vec![
                    ("candidateId", text(256)),
                    (
                        "reason",
                        enumeration(&["identity_unavailable", "source_evidence_unavailable"]),
                    ),
                    (
                        "missingEvidence",
                        array(
                            enumeration(&[
                                "current_market",
                                "interest_rate_history",
                                "benchmark_history",
                                "investment_history",
                                "corporate_actions",
                                "equity_premium",
                            ]),
                            6,
                        ),
                    ),
                ]),
                32,
            ),
        ),
        ("startedAt", nanos()),
        ("updatedAt", nanos()),
        ("explanation", nullable(text(256))),
        ("canCancel", boolean()),
        ("canResume", boolean()),
    ])
}
fn variant(kind: &str, mut fields: Vec<(&str, Value)>) -> Value {
    fields.insert(0, ("kind", constant(kind)));
    object(fields)
}
pub(super) fn failure() -> Value {
    variant(
        "unavailable",
        vec![
            (
                "code",
                enumeration(crate::application::analytical_workflow::WORKFLOW_ERROR_CODES),
            ),
            ("message", text(2048)),
        ],
    )
}
pub(super) fn result(update: bool) -> Value {
    let mut results = if update {
        vec![
            variant("workflow", vec![("workflow", workflow())]),
            variant("profile", vec![("profile", profile())]),
            variant("validation", vec![("profile", profile())]),
            variant("activation", vec![("activeProfile", profile())]),
        ]
    } else {
        vec![
            variant(
                "status",
                vec![
                    ("activeProfile", profile()),
                    ("profiles", array(profile(), 32)),
                    ("workflows", array(workflow(), 256)),
                    ("workflowAvailability", availability()),
                    ("canCreateCustomProfile", boolean()),
                    ("profileRecoveryNotice", nullable(text(512))),
                ],
            ),
            variant(
                "profile_options",
                vec![(
                    "options",
                    object(vec![
                        ("nextCursor", nullable(text(512))),
                        (
                            "benchmarkChoices",
                            super::analytical_profile::benchmark_choices(),
                        ),
                        (
                            "modelChoices",
                            array(object(vec![("token", text(64)), ("label", text(256))]), 101),
                        ),
                        (
                            "fixedSettings",
                            array(
                                object(vec![
                                    ("label", text(64)),
                                    ("value", text(256)),
                                    ("explanation", text(1024)),
                                ]),
                                8,
                            ),
                        ),
                    ]),
                )],
            ),
            variant(
                "comparison",
                vec![
                    ("recommendedProfile", profile()),
                    ("selectedProfile", profile()),
                    ("equivalent", boolean()),
                    ("differences", array(difference(), 11)),
                ],
            ),
            variant(
                "history",
                vec![
                    ("completeness", enumeration(&["complete", "truncated"])),
                    ("returnedCount", count(100)),
                    ("availableCount", count(256)),
                    ("nextAfterToken", nullable(token("history"))),
                    (
                        "entries",
                        array(
                            object(vec![
                                ("historyToken", token("history")),
                                ("profileToken", token("profile")),
                                ("profileName", text(64)),
                                (
                                    "action",
                                    enumeration(&[
                                        "recommended_initialized",
                                        "custom_created",
                                        "custom_updated",
                                        "validation_unavailable",
                                        "custom_validated",
                                        "custom_activated",
                                        "recommended_restored",
                                    ]),
                                ),
                                ("recordedAt", nanos()),
                                ("differencesFromRecommended", array(difference(), 11)),
                            ]),
                            100,
                        ),
                    ),
                ],
            ),
            variant(
                "workflow_coverage",
                vec![
                    ("workflowToken", token("workflow")),
                    (
                        "rows",
                        array(
                            object(vec![
                                ("instrumentId", uuid()),
                                (
                                    "reason",
                                    enumeration(&[
                                        "no_effective_canonical_definition",
                                        "outside_profile_asset_scope",
                                        "missing_official_listing",
                                        "ambiguous_official_listing",
                                        "source_history_unavailable",
                                        "required_feature_unavailable",
                                        "source_rights_unavailable",
                                        "freshness_unavailable",
                                        "calendar_unavailable",
                                    ]),
                                ),
                            ]),
                            64,
                        ),
                    ),
                    ("nextAfter", nullable(cursor())),
                ],
            ),
        ]
    };
    results.push(failure());
    one_of(results)
}
pub(super) fn product() -> Value {
    one_of(vec![
        object(vec![
            ("label", text(64)),
            ("kind", enumeration(&["recommended", "custom"])),
            ("activatedAt", nanos()),
            (
                "workflowAvailability",
                enumeration(&["available", "unavailable"]),
            ),
            ("nextAction", text(256)),
        ]),
        failure(),
    ])
}
pub(super) fn delivery() -> Value {
    one_of(vec![
        object(vec![
            ("jobId", uuid()),
            ("generation", nanos()),
            (
                "sequence",
                json!({"type":"string","pattern":"^(?:0|[1-9][0-9]*)$","maxLength":20}),
            ),
            (
                "state",
                enumeration(&[
                    "queued",
                    "preparing",
                    "running",
                    "awaiting_confirmation",
                    "cancelling",
                    "completed",
                    "failed",
                    "cancelled",
                    "interrupted",
                    "recovering",
                ]),
            ),
        ]),
        failure(),
    ])
}
