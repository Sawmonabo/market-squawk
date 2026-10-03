import { z } from "zod"

const unixNanosSchema = z.string().regex(/^[1-9]\d*$/)
const opaqueToken = (prefix: string) =>
  z.string().regex(new RegExp(`^${prefix}_[0-9a-f]{32}$`))

const profileTokenSchema = opaqueToken("profile")
const profileStateTokenSchema = opaqueToken("state")
const validationTokenSchema = opaqueToken("validation")
const activationTokenSchema = opaqueToken("activation")
const historyTokenSchema = opaqueToken("history")
const workflowTokenSchema = opaqueToken("workflow")

export const analyticalProductProjectionSchema = z
  .object({
    label: z.string().min(1).max(64),
    kind: z.enum(["recommended", "custom"]),
    activatedAt: unixNanosSchema,
    workflowAvailability: z.enum(["available", "unavailable"]),
    nextAction: z.string().min(1).max(256),
  })
  .strict()

const profileDifferenceSchema = z
  .object({
    label: z.string().min(1).max(64),
    explanation: z.string().min(1).max(256),
  })
  .strict()

const profileValidationSchema = z
  .object({
    state: z.enum(["built_in", "needs_validation", "unavailable", "validated"]),
    label: z.string().min(1).max(64),
    explanation: z.string().min(1).max(256),
    validatedAt: unixNanosSchema.nullable(),
  })
  .strict()

const preferenceFieldSchema = z.object({
  key: z.string().min(1).max(128),
  label: z.string().min(1).max(96),
  group: z.string().min(1).max(64),
  value: z.string().min(1).max(32),
  unit: z.enum(["", "%", "seconds", "daily returns"]),
  choices: z.array(z.object({ value: z.string().min(1).max(64), label: z.string().min(1).max(64) }).strict()).max(5),
}).strict()

const financialPreferencesSchema = z.object({
  coverage: z.enum(["stocks_and_etfs", "stocks"]),
  modelChoice: z.string().min(1).max(64),
  allowRetrospectiveStudies: z.boolean(),
  fields: z.array(preferenceFieldSchema).max(64),
}).strict()

const benchmarkChoiceSchema = z.object({
  instrumentId: z.string().uuid(),
  displayName: z.string().min(1).max(512),
  symbol: z.string().min(1).max(32),
  comparisonDescription: z.string().min(1).max(521),
  isDefault: z.boolean(),
}).strict()

const profileOptionsSchema = z.object({
  benchmarkChoices: z.array(benchmarkChoiceSchema).max(3),
  modelChoices: z.array(z.object({ token: z.string().min(1).max(64), label: z.string().min(1).max(256) }).strict()).min(1).max(101),
  nextCursor: z.string().min(1).max(512).nullable(),
  fixedSettings: z.array(z.object({ label: z.string().min(1).max(64), value: z.string().min(1).max(256), explanation: z.string().min(1).max(1_024) }).strict()).max(8),
}).strict()

export const analyticalProfileSchema = z
  .object({
    profileToken: profileTokenSchema,
    profileStateToken: profileStateTokenSchema,
    displayName: z.string().min(1).max(64),
    version: z.number().int().positive(),
    mode: z.enum(["recommended", "custom"]),
    active: z.boolean(),
    validation: profileValidationSchema,
    validationToken: validationTokenSchema.nullable(),
    activationToken: activationTokenSchema.nullable(),
    differencesFromRecommended: z.array(profileDifferenceSchema).max(11),
    createdAt: unixNanosSchema,
    updatedAt: unixNanosSchema,
    activatedAt: unixNanosSchema.nullable(),
    canValidate: z.boolean(),
    canActivate: z.boolean(),
    canRestoreRecommended: z.boolean(),
    canEdit: z.boolean(),
    analysisScope: z.enum(["focused", "balanced", "broad"]),
    financialPreferences: financialPreferencesSchema,
  })
  .strict()

const coverageSchema = z
  .object({
    completeness: z.enum(["complete", "partial"]),
    searched: z.number().int().nonnegative(),
    population: z.number().int().nonnegative(),
    inputUnavailable: z.number().int().nonnegative(),
    excluded: z.number().int().nonnegative(),
    deeplyAnalyzed: z.number().int().nonnegative(),
    generated: z.number().int().nonnegative(),
    noAction: z.number().int().nonnegative(),
    unavailable: z.number().int().nonnegative(),
  })
  .strict()

const missingEvidenceSchema = z.enum([
  "current_market", "interest_rate_history", "benchmark_history",
  "investment_history", "corporate_actions", "equity_premium",
])
const unavailableMemberSchema = z.object({
  candidateId: z.string().min(1).max(256),
  reason: z.enum(["identity_unavailable", "source_evidence_unavailable"]),
  missingEvidence: z.array(missingEvidenceSchema).max(6),
}).strict()
export type MissingInvestmentEvidence = z.infer<typeof missingEvidenceSchema>

const workflowSchema = z
  .object({
    workflowToken: workflowTokenSchema,
    kind: z.enum([
      "opportunity_discovery",
      "investment_analysis",
      "track_record_refresh",
    ]),
    state: z.enum([
      "waiting",
      "in_progress",
      "paused",
      "cancelling",
      "complete",
      "cancelled",
      "unavailable",
    ]),
    progress: z
      .object({
        stage: z.enum([
          "preparing",
          "gathering_evidence",
          "building_results",
          "finalizing",
          "complete",
          "unavailable",
        ]),
        completedSteps: z.number().int().nonnegative().max(8_192),
        waitingForBackgroundWork: z.boolean(),
      })
      .strict(),
    coverage: coverageSchema.nullable(),
    resultCount: z.number().int().nonnegative().max(128),
    resultActionTokens: z.array(z.string().uuid()).max(128),
    resultOrdering: z.enum(["estimated_gain_descending", "unavailable_incomparable_horizons"]).nullable(),
    unavailableMembers: z.array(unavailableMemberSchema).max(32),
    startedAt: unixNanosSchema,
    updatedAt: unixNanosSchema,
    explanation: z.string().min(1).max(256).nullable(),
    canCancel: z.boolean(),
    canResume: z.boolean(),
  })
  .strict()

const profileHistoryEntrySchema = z
  .object({
    historyToken: historyTokenSchema,
    profileToken: profileTokenSchema,
    profileName: z.string().min(1).max(64),
    action: z.enum([
      "recommended_initialized",
      "custom_created",
      "custom_updated",
      "validation_unavailable",
      "custom_validated",
      "custom_activated",
      "recommended_restored",
    ]),
    recordedAt: unixNanosSchema,
    differencesFromRecommended: z.array(profileDifferenceSchema).max(11),
  })
  .strict()

export const analyticalControllerStatusSchema = z
  .object({
    kind: z.literal("status"),
    activeProfile: analyticalProfileSchema,
    profiles: z.array(analyticalProfileSchema).min(1).max(32),
    workflows: z.array(workflowSchema).max(256),
    workflowAvailability: z
      .object({
        state: z.enum(["available", "unavailable"]),
        explanation: z.string().min(1).max(512),
        nextAction: z.string().min(1).max(256),
      })
      .strict(),
    canCreateCustomProfile: z.boolean(),
    profileRecoveryNotice: z.string().min(1).max(512).nullable(),
  })
  .strict()

const coverageCursorSchema = z.object({ partition: z.number().int().nonnegative().max(65_535).nullable(), offset: z.number().int().nonnegative().max(65_536) }).strict()
const coverageReasonSchema = z.object({
  instrumentId: z.string().uuid(),
  reason: z.enum(["no_effective_canonical_definition", "outside_profile_asset_scope",
    "missing_official_listing", "ambiguous_official_listing", "source_history_unavailable", "required_feature_unavailable",
    "source_rights_unavailable", "freshness_unavailable", "calendar_unavailable"]),
}).strict()
export type WorkflowCoverageCursor = z.infer<typeof coverageCursorSchema>

export const analyticalControllerResponseSchema = z.discriminatedUnion("kind", [
  analyticalControllerStatusSchema,
  z.object({ kind: z.literal("workflow_coverage"), workflowToken: workflowTokenSchema, rows: z.array(coverageReasonSchema).max(64), nextAfter: coverageCursorSchema.nullable() }).strict(),
  z.object({ kind: z.literal("profile_options"), options: profileOptionsSchema }).strict(),
  z.object({ kind: z.literal("workflow"), workflow: workflowSchema }).strict(),
  z.object({ kind: z.literal("profile"), profile: analyticalProfileSchema }).strict(),
  z.object({ kind: z.literal("validation"), profile: analyticalProfileSchema }).strict(),
  z
    .object({
      kind: z.literal("comparison"),
      recommendedProfile: analyticalProfileSchema,
      selectedProfile: analyticalProfileSchema,
      equivalent: z.boolean(),
      differences: z.array(profileDifferenceSchema).max(11),
    })
    .strict(),
  z
    .object({
      kind: z.literal("activation"),
      activeProfile: analyticalProfileSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("history"),
      completeness: z.enum(["complete", "truncated"]),
      returnedCount: z.number().int().nonnegative().max(100),
      availableCount: z.number().int().nonnegative().max(256),
      nextAfterToken: historyTokenSchema.nullable(),
      entries: z.array(profileHistoryEntrySchema).max(100),
    })
    .strict(),
])

export type AnalyticalProductProjection = z.infer<
  typeof analyticalProductProjectionSchema
>
export type AnalyticalControllerStatus = z.infer<
  typeof analyticalControllerStatusSchema
>
export type AnalyticalControllerResponse = z.infer<
  typeof analyticalControllerResponseSchema
>
export type AnalyticalProfile = z.infer<typeof analyticalProfileSchema>
export type FinancialPreferences = z.infer<typeof financialPreferencesSchema>
export type ProfileOptions = z.infer<typeof profileOptionsSchema>
export type FinancialPreferencesInput = Pick<FinancialPreferences, "coverage" | "modelChoice" | "allowRetrospectiveStudies"> & {
  fields: Array<{ key: string; value: string }>
}

export type AnalyticalControllerRequest =
  | { action: "status" }
  | { action: "profileOptions"; cursor?: string; limit?: number }
  | { action: "copyRecommended"; displayName: string }
  | {
      action: "updateProfile"
      profileToken: string
      profileStateToken: string
      displayName: string
      analysisScope: AnalyticalProfile["analysisScope"]
      financialPreferences: FinancialPreferencesInput
    }
  | {
      action: "validateProfile"
      profileToken: string
      profileStateToken: string
    }
  | { action: "compareWithRecommended"; profileToken: string }
  | {
      action: "activateProfile"
      profileToken: string
      profileStateToken: string
      validationToken: string
      activationToken: string
    }
  | { action: "restoreRecommended"; activationToken: string }
  | { action: "history"; afterToken?: string; limit: number }
  | { action: "findOpportunities"; benchmarkInstrumentId?: string }
  | { action: "analyzeInvestment"; selectionToken: string; benchmarkInstrumentId?: string }
  | { action: "resumeWorkflow"; workflowToken: string }
  | { action: "cancelWorkflow"; workflowToken: string }
  | { action: "workflowCoverage"; workflowToken: string; after?: WorkflowCoverageCursor }
