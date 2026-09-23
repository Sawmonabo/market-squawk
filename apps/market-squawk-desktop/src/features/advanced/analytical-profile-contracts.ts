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
    workflowAvailability: z.literal("unavailable"),
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
    differencesFromRecommended: z.array(profileDifferenceSchema).max(10),
    createdAt: unixNanosSchema,
    updatedAt: unixNanosSchema,
    activatedAt: unixNanosSchema.nullable(),
    canValidate: z.boolean(),
    canActivate: z.boolean(),
    canRestoreRecommended: z.boolean(),
  })
  .strict()

const coverageSchema = z
  .object({
    completeness: z.enum(["complete", "partial"]),
    searched: z.number().int().nonnegative(),
    completeEvidence: z.number().int().nonnegative(),
    excluded: z.number().int().nonnegative(),
    deeplyAnalyzed: z.number().int().nonnegative(),
    generated: z.number().int().nonnegative(),
    noAction: z.number().int().nonnegative(),
    unavailable: z.number().int().nonnegative(),
  })
  .strict()

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
        completedSteps: z.number().int().nonnegative().max(64),
        waitingForBackgroundWork: z.boolean(),
      })
      .strict(),
    coverage: coverageSchema.nullable(),
    resultCount: z.number().int().nonnegative().max(128),
    startedAt: unixNanosSchema,
    updatedAt: unixNanosSchema,
    explanation: z.string().min(1).max(256).nullable(),
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
    differencesFromRecommended: z.array(profileDifferenceSchema).max(10),
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
        state: z.literal("unavailable"),
        explanation: z.string().min(1).max(512),
        nextAction: z.string().min(1).max(256),
      })
      .strict(),
    canCreateCustomProfile: z.boolean(),
  })
  .strict()

export const analyticalControllerResponseSchema = z.discriminatedUnion("kind", [
  analyticalControllerStatusSchema,
  z.object({ kind: z.literal("profile"), profile: analyticalProfileSchema }).strict(),
  z.object({ kind: z.literal("validation"), profile: analyticalProfileSchema }).strict(),
  z
    .object({
      kind: z.literal("comparison"),
      recommendedProfile: analyticalProfileSchema,
      selectedProfile: analyticalProfileSchema,
      equivalent: z.boolean(),
      differences: z.array(profileDifferenceSchema).max(10),
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

export type AnalyticalControllerRequest =
  | { action: "status" }
  | { action: "copyRecommended"; displayName: string }
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
    }
  | { action: "restoreRecommended"; activationToken: string }
  | { action: "history"; afterToken?: string; limit: number }
