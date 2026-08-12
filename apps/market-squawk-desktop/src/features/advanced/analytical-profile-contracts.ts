import { z } from "zod"

const canonicalUuidSchema = z
  .string()
  .regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  )
  .refine(
    (value) => value !== "00000000-0000-0000-0000-000000000000",
    "Expected a non-nil canonical UUID.",
  )
const digestSchema = z
  .string()
  .regex(/^[0-9a-f]{64}$/)
  .refine((value) => /[1-9a-f]/.test(value), "Expected a nonzero digest.")
const unsignedIntegerSchema = z.string().regex(/^(?:0|[1-9]\d*)$/)
const positiveIntegerSchema = z.string().regex(/^[1-9]\d*$/)
const nonemptyIdentitySchema = z.string().min(1).max(128)

const profileComponentBindingSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("default_required") }).strict(),
  z
    .object({
      kind: z.literal("exact"),
      identity: nonemptyIdentitySchema,
      version: z.string().min(1).max(64),
      digest: digestSchema,
    })
    .strict(),
])

export const analyticalProfileConfigSchema = z
  .object({
    supportedInvestmentPolicy: profileComponentBindingSchema,
    pointInTimeDatasetPolicy: profileComponentBindingSchema,
    requiredFeatureSet: profileComponentBindingSchema,
    modelBundlePolicy: profileComponentBindingSchema,
    trainingCalibrationPolicy: profileComponentBindingSchema,
    forecastHorizonPolicy: profileComponentBindingSchema,
    valuationPolicy: profileComponentBindingSchema,
    backtestCostPolicy: profileComponentBindingSchema,
    recommendationPolicy: profileComponentBindingSchema,
    riskFreshnessAbstentionPolicy: profileComponentBindingSchema,
  })
  .strict()

const serviceResultReferenceSchema = z
  .object({
    operation: z.string().min(1).max(128),
    resultId: z.string().min(1).max(256),
    contentSha256: digestSchema,
  })
  .strict()

const profileValidationReceiptSchema = z
  .object({
    receiptId: canonicalUuidSchema,
    profileId: canonicalUuidSchema,
    profileRevision: unsignedIntegerSchema,
    configDigest: digestSchema,
    validatedAt: unsignedIntegerSchema,
    basis: z.enum([
      "identical_to_immutable_default",
      "backend_component_receipts",
    ]),
    backendReceipts: z.array(serviceResultReferenceSchema).max(128),
  })
  .strict()

export const analyticalProfileSchema = z
  .object({
    profileId: canonicalUuidSchema,
    ownerWorkspaceId: canonicalUuidSchema,
    displayName: z.string().min(1).max(64),
    kind: z.enum(["default", "custom"]),
    version: z.number().int().positive(),
    revision: unsignedIntegerSchema,
    configDigest: digestSchema,
    config: analyticalProfileConfigSchema,
    validationState: z.enum([
      "default_immutable",
      "not_validated",
      "blocked",
      "validated",
    ]),
    lastValidation: profileValidationReceiptSchema.nullable(),
    createdAt: unsignedIntegerSchema,
    updatedAt: unsignedIntegerSchema,
  })
  .strict()

export const activeProfileBindingSchema = z
  .object({
    profileId: canonicalUuidSchema,
    ownerWorkspaceId: canonicalUuidSchema,
    displayName: z.string().min(1).max(64),
    kind: z.enum(["default", "custom"]),
    version: z.number().int().positive(),
    profileRevision: unsignedIntegerSchema,
    configDigest: digestSchema,
    activationRevision: unsignedIntegerSchema,
    activatedAt: unsignedIntegerSchema,
  })
  .strict()

const serviceJobReferenceSchema = z
  .object({
    jobId: canonicalUuidSchema,
    generation: positiveIntegerSchema,
    terminalSequence: unsignedIntegerSchema.nullable(),
    result: serviceResultReferenceSchema.nullable(),
  })
  .strict()

const coverageCountsSchema = z
  .object({
    searched: z.number().int().nonnegative(),
    completeEvidence: z.number().int().nonnegative(),
    excluded: z.number().int().nonnegative(),
    deeplyAnalyzed: z.number().int().nonnegative(),
    generated: z.number().int().nonnegative(),
    noAction: z.number().int().nonnegative(),
    unavailable: z.number().int().nonnegative(),
  })
  .strict()

const workflowCheckpointSchema = z
  .object({
    sequence: unsignedIntegerSchema,
    stage: z.enum([
      "created",
      "capability_completed",
      "waiting_for_service_job",
      "results_retained",
      "coverage_closed",
      "ranking_closed",
      "terminal",
    ]),
    recordedAt: unsignedIntegerSchema,
    childJob: serviceJobReferenceSchema.nullable(),
    result: serviceResultReferenceSchema.nullable(),
  })
  .strict()

const workflowRunSchema = z
  .object({
    runId: canonicalUuidSchema,
    schemaVersion: z.literal(1),
    ownerWorkspaceId: canonicalUuidSchema,
    kind: z.enum([
      "find_opportunities",
      "analyze_investment",
      "outcome_refresh",
    ]),
    state: z.enum([
      "blocked",
      "queued",
      "running",
      "waiting_for_service_job",
      "completed",
      "cancelled",
      "failed",
      "stale",
    ]),
    targetInstrumentId: canonicalUuidSchema.nullable(),
    profile: z
      .object({
        active: activeProfileBindingSchema,
        resolvedComponentReceiptSha256: digestSchema,
      })
      .strict(),
    createdAt: unsignedIntegerSchema,
    updatedAt: unsignedIntegerSchema,
    checkpointJournal: z.array(workflowCheckpointSchema).max(64),
    childJobs: z.array(serviceJobReferenceSchema).max(64),
    resultReferences: z.array(serviceResultReferenceSchema).max(128),
    coverageReceipt: z
      .object({
        receiptId: canonicalUuidSchema,
        completeness: z.enum(["complete", "truncated"]),
        counts: coverageCountsSchema,
        contentSha256: digestSchema,
      })
      .strict()
      .nullable(),
    exclusionReceipt: z
      .object({
        receiptId: canonicalUuidSchema,
        excludedCount: z.number().int().nonnegative(),
        reasonsResult: serviceResultReferenceSchema,
        contentSha256: digestSchema,
      })
      .strict()
      .nullable(),
    rankingReceipt: z
      .object({
        receiptId: canonicalUuidSchema,
        orderedResultIds: z.array(z.string().min(1).max(256)).max(128),
        policyResult: serviceResultReferenceSchema,
        contentSha256: digestSchema,
      })
      .strict()
      .nullable(),
    executionEligibility: z.literal("execution_ineligible"),
    lastError: z.string().min(1).max(1_024).nullable(),
  })
  .strict()

const profileHistoryEntrySchema = z
  .object({
    eventId: canonicalUuidSchema,
    ownerWorkspaceId: canonicalUuidSchema,
    controllerRevision: unsignedIntegerSchema,
    action: z.enum([
      "initialized_default",
      "copied_default",
      "updated_custom",
      "validation_blocked",
      "validated_custom",
      "activated_custom",
      "restored_default",
    ]),
    profileId: canonicalUuidSchema,
    profileVersion: z.number().int().positive(),
    profileRevision: unsignedIntegerSchema,
    configDigest: digestSchema,
    config: analyticalProfileConfigSchema,
    validationReceipt: profileValidationReceiptSchema.nullable(),
    recordedAt: unsignedIntegerSchema,
    supersedesProfileId: canonicalUuidSchema.nullable(),
  })
  .strict()

export const analyticalControllerStatusSchema = z
  .object({
    kind: z.literal("status"),
    controllerSchemaVersion: z.literal(1),
    ownerWorkspaceId: canonicalUuidSchema,
    controllerRevision: unsignedIntegerSchema,
    activeProfile: activeProfileBindingSchema,
    profiles: z.array(analyticalProfileSchema).min(1).max(32),
    workflowRuns: z.array(workflowRunSchema).max(256),
    workflowReadiness: z
      .object({
        state: z.literal("blocked"),
        blockers: z
          .array(
            z
              .object({
                code: z.string().min(1).max(128),
                detail: z.string().min(1).max(1_024),
                owner: z.enum(["installed_application", "desktop"]),
              })
              .strict(),
          )
          .min(1)
          .max(16),
      })
      .strict(),
  })
  .strict()

export const analyticalControllerResponseSchema = z.discriminatedUnion("kind", [
  analyticalControllerStatusSchema,
  z.object({ kind: z.literal("profile"), profile: analyticalProfileSchema }).strict(),
  z
    .object({
      kind: z.literal("validation"),
      profile: analyticalProfileSchema,
      receipt: profileValidationReceiptSchema.nullable(),
      blockers: z
        .array(
          z
            .object({
              code: z.string().min(1).max(128),
              detail: z.string().min(1).max(1_024),
            })
            .strict(),
        )
        .max(16),
    })
    .strict(),
  z
    .object({
      kind: z.literal("comparison"),
      defaultProfile: analyticalProfileSchema,
      selectedProfile: analyticalProfileSchema,
      equivalent: z.boolean(),
      differentComponents: z
        .array(
          z.enum([
            "supported_investment_policy",
            "point_in_time_dataset_policy",
            "required_feature_set",
            "model_bundle_policy",
            "training_calibration_policy",
            "forecast_horizon_policy",
            "valuation_policy",
            "backtest_cost_policy",
            "recommendation_policy",
            "risk_freshness_abstention_policy",
          ]),
        )
        .max(10),
    })
    .strict(),
  z
    .object({
      kind: z.literal("activation"),
      activeProfile: activeProfileBindingSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("history"),
      completeness: z.enum(["complete", "truncated"]),
      returnedCount: z.number().int().nonnegative().max(100),
      availableCount: z.number().int().nonnegative().max(256),
      nextAfterRevision: unsignedIntegerSchema.nullable(),
      entries: z.array(profileHistoryEntrySchema).max(100),
    })
    .strict(),
])

export type AnalyticalProfileConfig = z.infer<
  typeof analyticalProfileConfigSchema
>
export type AnalyticalControllerStatus = z.infer<
  typeof analyticalControllerStatusSchema
>
export type AnalyticalControllerResponse = z.infer<
  typeof analyticalControllerResponseSchema
>

export type AnalyticalControllerRequest =
  | { action: "status" }
  | { action: "copyDefault"; displayName: string }
  | {
      action: "updateCustom"
      profileId: string
      expectedRevision: string
      config: AnalyticalProfileConfig
    }
  | {
      action: "validateCustom"
      profileId: string
      expectedRevision: string
    }
  | { action: "compareWithDefault"; profileId: string }
  | {
      action: "activateCustom"
      profileId: string
      expectedRevision: string
      validationReceiptId: string
    }
  | {
      action: "restoreDefault"
      expectedActivationRevision: string
    }
  | { action: "history"; afterRevision?: string; limit: number }
