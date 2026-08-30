import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const uuidSchema = z.string().uuid()
const identifierSchema = z.string().min(1).max(256)
const timestampSchema = z.string().datetime({ offset: true })
const amountSchema = z
  .object({
    amount: z.string().regex(/^-?\d+(?:\.\d+)?$/),
    currency: z.string().regex(/^[A-Z]{3}$/),
    scale: z.number().int().min(0).max(28),
    amountBasis: z.enum([
      "per_instrument_unit",
      "reporting_entity_total",
      "position_total",
    ]),
  })
  .strict()
const hierarchySchema = z.enum(["level_1", "level_2", "level_3", "unclassified"])
const classificationSchema = z
  .object({
    classificationToken: uuidSchema,
    hierarchy: hierarchySchema,
    basis: z.object({ kind: z.enum(["rules", "override"]) }).strict(),
    checkCount: z.number().int().nonnegative(),
    reasonCount: z.number().int().nonnegative(),
  })
  .strict()
const marketAccessAssessmentSchema = z
  .object({
    conclusion: z.enum(["accessible", "inaccessible", "not_assessed"]),
    effectiveFrom: timestampSchema,
    effectiveUntil: timestampSchema,
    rationale: z.string().max(4_096),
    preparedBy: identifierSchema,
    preparedAt: timestampSchema,
    approvedBy: identifierSchema,
    approvedAt: timestampSchema,
  })
  .strict()
const inputSchema = z
  .object({
    inputToken: uuidSchema,
    marketInputToken: uuidSchema.nullable(),
    referenceInstrumentId: uuidSchema,
    relationship: z.enum(["identical", "similar", "proxy"]),
    amount: amountSchema,
    significance: z.enum(["significant", "not_significant"]),
    observability: z.enum(["quoted_price", "observable", "unobservable"]),
    adjustment: z.enum(["none", "observable", "unobservable"]),
    marketActivity: z.enum(["active", "inactive", "not_assessed"]),
    marketAccess: z.enum(["accessible", "inaccessible", "not_assessed"]),
    marketAccessAssessment: marketAccessAssessmentSchema.nullable(),
    dataQuality: z.enum([
      "direct_verified",
      "direct_unverified",
      "official_delayed",
      "aggregated",
      "indicative",
      "modeled",
      "estimated",
      "stale",
      "quarantined",
    ]),
    useAssessment: z
      .object({
        relationship: z.enum(["identical", "similar", "proxy"]),
        observability: z.enum(["quoted_price", "observable", "unobservable"]),
        adjustment: z.enum(["none", "observable", "unobservable"]),
        rationale: z.string().max(4_096),
        assessedBy: identifierSchema,
        assessedAt: timestampSchema,
      })
      .strict()
      .nullable(),
    evidence: z
      .object({
        kind: z.enum(["market_observation", "published_research", "analysis", "portfolio"]),
        label: z.string().min(1).max(128),
        observedAt: timestampSchema.nullable(),
        effectiveAt: timestampSchema.nullable(),
        publishedAt: timestampSchema.nullable(),
        availableAt: timestampSchema.nullable(),
        receivedAt: timestampSchema.nullable(),
        validUntil: timestampSchema.nullable(),
        recordedAt: timestampSchema,
        verification: z.enum(["verified", "unverified"]),
      })
      .strict(),
  })
  .strict()
const approvalSchema = z
  .object({
    approvalToken: uuidSchema,
    approvedBy: identifierSchema,
    approvedAt: timestampSchema,
    expiresAt: timestampSchema,
    status: z.enum(["not_yet_effective", "active", "expired", "revoked"]),
    revocation: z
      .object({
        revokedBy: identifierSchema,
        revokedAt: timestampSchema,
        reason: z.string().min(1).max(4_096),
      })
      .strict()
      .nullable(),
  })
  .strict()
const explanationSchema = z
  .object({
    checks: z.array(
      z.object({ inputToken: uuidSchema, check: identifierSchema, passed: z.boolean() }).strict(),
    ),
    reasons: z.array(
      z.object({ inputToken: uuidSchema.nullable(), reason: identifierSchema }).strict(),
    ),
  })
  .strict()
const measurementSummarySchema = z
  .object({
    measurementToken: uuidSchema,
    accountId: uuidSchema,
    instrumentId: uuidSchema,
    amount: amountSchema,
    measurementAt: timestampSchema,
    preparedAt: timestampSchema,
    preparedBy: identifierSchema,
    method: z.enum([
      "quoted_market_price",
      "market_approach",
      "income_approach",
      "cost_approach",
    ]),
    inputCount: z.number().int().positive(),
    classification: classificationSchema.nullable(),
  })
  .strict()
const measurementDetailSchema = measurementSummarySchema.extend({
  inputs: z.array(inputSchema),
  explanation: explanationSchema.nullable(),
  approvals: z.array(approvalSchema),
})
export const fairValueMeasurementSchema = measurementDetailSchema
const workspaceSchema = z
  .object({
    measurements: z.array(measurementSummarySchema),
    selectedMeasurement: measurementDetailSchema.nullable(),
  })
  .strict()

const governancePrincipalSchema = z
  .object({
    principalId: uuidSchema,
    displayName: z.string().min(1),
    roles: z.array(z.string().min(1)).min(1),
  })
  .strict()
const governancePreviewSchema = z
  .object({
    previewId: uuidSchema,
    digest: z.string().regex(/^[0-9a-f]{64}$/),
    requiredRoles: z.array(z.string().min(1)).min(1),
    distinctPrincipalCount: z.number().int().positive(),
    eligiblePrincipalIds: z.array(uuidSchema).min(1),
    expiresAt: timestampSchema,
    effects: z.array(z.object({ kind: z.string().min(1) })).min(1),
  })
  .strict()
const governanceAuthorizationSchema = z
  .object({
    authorizationHandle: uuidSchema,
    previewId: uuidSchema,
    principalId: uuidSchema,
    expiresAt: timestampSchema,
  })
  .strict()
const governanceCommitSchema = z.object({
  receipt: z.object({
    receiptId: uuidSchema,
    previewId: uuidSchema,
    digest: z.string().regex(/^[0-9a-f]{64}$/),
    committedAt: timestampSchema,
    authorizedPrincipals: z.array(
      z.object({ principalId: uuidSchema, roles: z.array(z.string().min(1)).min(1) }),
    ),
    effects: z.array(z.object({ kind: z.string().min(1) })),
  }),
})

export type FairValueApproval = z.infer<typeof approvalSchema>
export type FairValueClassification = z.infer<typeof classificationSchema>
export type FairValueHierarchy = z.infer<typeof hierarchySchema>
export type FairValueInput = z.infer<typeof inputSchema>
export type FairValueMarketAccess = z.infer<typeof marketAccessAssessmentSchema>
export type FairValueMeasurement = z.infer<typeof measurementDetailSchema>
export type FairValueMeasurementSummary = z.infer<typeof measurementSummarySchema>
export type GovernanceActionPreview = z.infer<typeof governancePreviewSchema>
export type GovernanceAuthorization = z.infer<typeof governanceAuthorizationSchema>
export type GovernanceCommit = z.infer<typeof governanceCommitSchema>["receipt"]
export type GovernancePrincipal = z.infer<typeof governancePrincipalSchema>

export type FairValueGovernanceProposal =
  | {
      kind: "approve"
      measurementToken: string
      classificationToken: string
      expiresAt: string
    }
  | {
      kind: "override"
      measurementToken: string
      classificationToken: string
      requestedHierarchy: "level_2" | "level_3"
      justification: string
      expiresAt: string
    }
  | { kind: "revoke"; approvalToken: string; reason: string }
  | {
      kind: "market_access"
      marketInputToken: string
      conclusion: "accessible" | "inaccessible"
      effectiveFrom: string
      effectiveUntil: string
      rationale: string
    }

export interface FairValueWorkspace {
  measurements: FairValueMeasurementSummary[]
  selectedMeasurement: FairValueMeasurement | null
  completeness: string
  returnedItems: number
  availableItems: number
}

export function parseFairValueWorkspace(result: ApplicationResult): FairValueWorkspace {
  const parsed = workspaceSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error("The installed service returned an unsupported fair-value workspace.")
  }
  return {
    ...parsed.data,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parseGovernancePrincipals(result: ApplicationResult) {
  const parsed = z.object({ principals: z.array(governancePrincipalSchema) }).safeParse(result.data)
  if (!parsed.success) throw new Error("Eligible reviewers could not be read.")
  return parsed.data.principals
}

export function parseFairValueGovernancePreview(result: ApplicationResult) {
  const parsed = z.object({ preview: governancePreviewSchema }).safeParse(result.data)
  if (!parsed.success) throw new Error("The governance review could not be prepared.")
  return parsed.data.preview
}

export function parseGovernanceAuthorization(
  result: ApplicationResult,
  expectedPreviewId: string,
) {
  const parsed = z.object({ authorization: governanceAuthorizationSchema }).safeParse(result.data)
  if (!parsed.success || parsed.data.authorization.previewId !== expectedPreviewId) {
    throw new Error("The reviewer authorization did not match this proposal.")
  }
  return parsed.data.authorization
}

export function parseFairValueGovernanceCommit(
  result: ApplicationResult,
  expectedPreviewId: string,
) {
  const parsed = governanceCommitSchema.safeParse(result.data)
  if (!parsed.success || parsed.data.receipt.previewId !== expectedPreviewId) {
    throw new Error("The recorded action did not match this proposal.")
  }
  return parsed.data.receipt
}
