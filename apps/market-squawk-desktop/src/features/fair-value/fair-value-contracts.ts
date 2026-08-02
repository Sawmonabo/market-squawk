import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const identifierSchema = z.string().min(1)

const amountSchema = z.object({
  amount: z.string().regex(/^-?\d+(?:\.\d+)?$/),
  currency: z.string().regex(/^[A-Z]{3}$/),
  scale: z.number().int().nonnegative(),
})

const hierarchySchema = z.enum([
  "level_1",
  "level_2",
  "level_3",
  "unclassified",
])

const dataQualitySchema = z.enum([
  "direct_verified",
  "direct_unverified",
  "official_delayed",
  "aggregated",
  "indicative",
  "modeled",
  "estimated",
  "stale",
  "quarantined",
])

const marketDepthSchema = z.enum([
  "top_of_book",
  "price_level",
  "order_level",
])

const classificationSchema = z.object({
  decisionId: digestSchema,
  measurementId: digestSchema,
  evidenceHash: digestSchema,
  rulesetVersion: z.number().int().positive(),
  rulesetHash: digestSchema,
  hierarchy: hierarchySchema,
  basis: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("rules") }),
    z.object({
      kind: z.literal("override"),
      baseDecisionId: digestSchema,
      overrideId: digestSchema,
    }),
  ]),
  truthTableItemCount: z.number().int().nonnegative(),
  reasonCount: z.number().int().nonnegative(),
})

const predicateSchema = z.object({
  inputId: digestSchema,
  predicate: z.string().min(1),
  passed: z.boolean(),
})

const reasonSchema = z.object({
  inputId: digestSchema.nullable(),
  code: z.string().min(1),
})

const marketAccessSchema = z.object({
  assessmentId: digestSchema,
  accountId: identifierSchema,
  venueId: identifierSchema,
  instrumentId: identifierSchema,
  conclusion: z.enum(["accessible", "inaccessible", "not_assessed"]),
  effectiveFrom: identifierSchema,
  effectiveUntil: identifierSchema,
  rationale: z.string(),
  preparedBy: identifierSchema,
  preparedAt: identifierSchema,
  approvedBy: identifierSchema,
  approvedAt: identifierSchema,
  supersedes: digestSchema.nullable(),
})

const evidenceOriginSchema = z
  .object({
    kind: z.enum(["live", "research", "analytics", "portfolio"]),
    venueId: identifierSchema.optional(),
    datasetId: identifierSchema.optional(),
    row: z.number().int().nonnegative().optional(),
    revision: z.union([z.string(), z.number().int().nonnegative()]).optional(),
  })
  .loose()

const inputEvidenceSchema = z.object({
  evidenceHash: digestSchema,
  sourceId: identifierSchema,
  sourceIdentifier: identifierSchema,
  origin: evidenceOriginSchema,
  sourceTimestamp: identifierSchema.nullable(),
  effectiveAt: identifierSchema.nullable(),
  publishedAt: identifierSchema.nullable(),
  availableAt: identifierSchema.nullable(),
  receivedAt: identifierSchema.nullable(),
  qualificationEvaluatedAt: identifierSchema.nullable(),
  qualificationValidUntil: identifierSchema.nullable(),
  ingestedAt: identifierSchema,
  verification: z.enum(["verified", "unverified"]),
})

const valuationInputSchema = z.object({
  inputId: digestSchema,
  subjectInstrumentId: identifierSchema,
  referenceInstrumentId: identifierSchema,
  relationship: z.enum(["identical", "similar", "proxy"]),
  amount: amountSchema,
  significance: z.enum(["significant", "not_significant"]),
  observability: z.enum(["quoted_price", "observable", "unobservable"]),
  adjustment: z.enum(["none", "observable", "unobservable"]),
  marketActivity: z.enum(["active", "inactive", "not_assessed"]),
  marketAccess: z.enum(["accessible", "inaccessible", "not_assessed"]),
  marketAccessAssessment: marketAccessSchema.nullable(),
  dataQuality: dataQualitySchema,
  marketDepth: marketDepthSchema.optional(),
  evidence: inputEvidenceSchema,
})

const revocationSchema = z.object({
  revocationId: digestSchema,
  approvalId: digestSchema,
  revokedBy: identifierSchema,
  revokedAt: identifierSchema,
  reason: z.string().min(1),
})

const approvalSchema = z.object({
  approvalId: digestSchema,
  decisionId: digestSchema,
  measurementId: digestSchema,
  overrideId: digestSchema.nullable(),
  approvedBy: identifierSchema,
  approvedAt: identifierSchema,
  expiresAt: identifierSchema,
  status: z.enum(["not_yet_effective", "active", "expired", "revoked"]),
  revocation: revocationSchema.nullable(),
})

const auditSubjectSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("classified"),
    measurementId: digestSchema,
    decisionId: digestSchema,
  }),
  z.object({
    kind: z.literal("override_proposed"),
    overrideId: digestSchema,
    decisionId: digestSchema,
  }),
  z.object({
    kind: z.literal("approved"),
    approvalId: digestSchema,
    decisionId: digestSchema,
  }),
  z.object({
    kind: z.literal("revoked"),
    revocationId: digestSchema,
    approvalId: digestSchema,
  }),
  z.object({
    kind: z.literal("market_access_approved"),
    assessmentId: digestSchema,
  }),
])

const auditEventSchema = z.object({
  auditEventId: digestSchema,
  sequence: z.number().int().positive(),
  previousEventId: digestSchema.nullable(),
  subject: auditSubjectSchema,
  actor: identifierSchema,
  businessAt: identifierSchema,
  occurredAt: identifierSchema,
})

const explanationSchema = z.object({
  truthTable: z.array(predicateSchema),
  reasons: z.array(reasonSchema),
})

const evidenceBundleSchema = z.object({
  inputs: z.array(valuationInputSchema),
})

const approvalBundleSchema = z.object({
  at: identifierSchema.optional(),
  approvals: z.array(approvalSchema),
})

const measurementSchema = z
  .object({
    measurementId: digestSchema,
    evidenceHash: digestSchema,
    accountId: identifierSchema,
    instrumentId: identifierSchema,
    amount: amountSchema,
    measurementAt: identifierSchema,
    preparedAt: identifierSchema,
    preparedBy: identifierSchema,
    method: z.enum([
      "quoted_market_price",
      "market_approach",
      "income_approach",
      "cost_approach",
    ]),
    inputCount: z.number().int().positive(),
    classification: classificationSchema.optional(),
    explanation: explanationSchema.optional(),
    evidence: evidenceBundleSchema.optional(),
    approvalStatus: approvalBundleSchema.optional(),
    marketAccess: z.array(marketAccessSchema).optional(),
    auditEvents: z.array(auditEventSchema).optional(),
  })
  .loose()

const measurementPageSchema = z.object({
  measurements: z.array(measurementSchema),
})

const classificationResultSchema = z.object({
  measurement: measurementSchema,
  classification: classificationSchema,
})

const explanationResultSchema = z.object({
  classification: classificationSchema,
  truthTable: z.array(predicateSchema),
  reasons: z.array(reasonSchema),
})

const evidenceResultSchema = z.object({
  measurementId: digestSchema,
  evidenceHash: digestSchema,
  inputs: z.array(valuationInputSchema),
})

const approvalResultSchema = z.object({
  measurementId: digestSchema,
  at: identifierSchema,
  approvals: z.array(approvalSchema),
})

const auditCursorSchema = z.object({
  sequence: z.number().int().positive(),
  eventId: digestSchema,
})

const auditPageSchema = z.object({
  events: z.array(auditEventSchema),
  totalEventCount: z.number().int().nonnegative(),
  nextCursor: auditCursorSchema.nullable(),
})

const marketAccessResultSchema = z.object({
  marketAccess: marketAccessSchema,
})

const classificationControlResultSchema = z.object({
  classification: classificationSchema,
  classificationReplay: z.boolean(),
})

const governancePrincipalSchema = z.object({
  principalId: identifierSchema,
  displayName: z.string().min(1),
  roles: z.array(z.string().min(1)).min(1),
})

const governancePreviewSchema = z.object({
  previewId: identifierSchema,
  digest: digestSchema,
  requiredRoles: z.array(z.string().min(1)).min(1),
  distinctPrincipalCount: z.number().int().positive(),
  eligiblePrincipalIds: z.array(identifierSchema).min(1),
  expiresAt: identifierSchema,
  effects: z.array(z.object({ kind: z.string().min(1) })).min(1),
})

const governanceAuthorizationSchema = z.object({
  authorizationHandle: identifierSchema,
  previewId: identifierSchema,
  principalId: identifierSchema,
  expiresAt: identifierSchema,
})

const governanceCommitSchema = z.object({
  receipt: z.object({
    receiptId: identifierSchema,
    previewId: identifierSchema,
    digest: digestSchema,
    committedAt: identifierSchema,
    authorizedPrincipals: z
      .array(z.object({ principalId: identifierSchema, roles: z.array(z.string().min(1)).min(1) }))
      .min(1),
    effects: z.array(z.object({ kind: z.string().min(1) })).min(1),
  }),
})

export type FairValueApproval = z.infer<typeof approvalSchema>
export type FairValueAuditEvent = z.infer<typeof auditEventSchema>
export type FairValueAuditCursor = z.infer<typeof auditCursorSchema>
export type FairValueClassification = z.infer<typeof classificationSchema>
export type FairValueHierarchy = z.infer<typeof hierarchySchema>
export type FairValueInput = z.infer<typeof valuationInputSchema>
export type FairValueMarketAccess = z.infer<typeof marketAccessSchema>
export type FairValueMeasurement = z.infer<typeof measurementSchema>
export type FairValueReason = z.infer<typeof reasonSchema>
export type GovernanceActionPreview = z.infer<typeof governancePreviewSchema>
export type GovernanceAuthorization = z.infer<typeof governanceAuthorizationSchema>
export type GovernanceCommit = z.infer<typeof governanceCommitSchema>["receipt"]
export type GovernancePrincipal = z.infer<typeof governancePrincipalSchema>

/// Typed proposal sent exactly once to the service for canonical preview. Actor, action time,
/// roles, approval evidence, and immutable audit identities are deliberately absent: the service
/// derives them from admitted principals and the retained fair-value records.
export type FairValueGovernanceProposal =
  | {
      kind: "approve"
      measurementId: string
      decisionId: string
      expiresAt: string
    }
  | {
      kind: "override"
      measurementId: string
      decisionId: string
      requestedHierarchy: "level_2" | "level_3"
      justification: string
      expiresAt: string
    }
  | {
      kind: "revoke"
      approvalId: string
      reason: string
    }
  | {
      kind: "market_access"
      accountId: string
      venueId: string
      instrumentId: string
      conclusion: "accessible" | "inaccessible"
      effectiveFrom: string
      effectiveUntil: string
      rationale: string
    }

export interface FairValueWorkspace {
  measurements: FairValueMeasurement[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface FairValueExplanation {
  classification: FairValueClassification
  truthTable: z.infer<typeof predicateSchema>[]
  reasons: FairValueReason[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface FairValueEvidenceBundle {
  evidenceHash: string
  inputs: FairValueInput[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface FairValueApprovalBundle {
  at: string
  approvals: FairValueApproval[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface FairValueAuditPage {
  events: FairValueAuditEvent[]
  totalEventCount: number
  nextCursor: FairValueAuditCursor | null
  completeness: string
  returnedItems: number
  availableItems: number
}

export function parseFairValueWorkspace(
  result: ApplicationResult,
): FairValueWorkspace {
  const page = measurementPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported fair-value response.",
    )
  }
  return {
    measurements: page.data.measurements,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parseFairValueClassification(
  result: ApplicationResult,
  expectedMeasurement: FairValueMeasurement,
): FairValueClassification {
  const parsed = classificationResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.measurement.measurementId !== expectedMeasurement.measurementId ||
    parsed.data.measurement.evidenceHash !== expectedMeasurement.evidenceHash ||
    parsed.data.classification.measurementId !== expectedMeasurement.measurementId ||
    parsed.data.classification.evidenceHash !== expectedMeasurement.evidenceHash
  ) {
    throw unsupportedDetail("classification")
  }
  return parsed.data.classification
}

export function parseFairValueExplanation(
  result: ApplicationResult,
  expectedMeasurement: FairValueMeasurement,
): FairValueExplanation {
  const parsed = explanationResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.classification.measurementId !== expectedMeasurement.measurementId ||
    parsed.data.classification.evidenceHash !== expectedMeasurement.evidenceHash
  ) {
    throw unsupportedDetail("classification explanation")
  }
  return withMetadata(parsed.data, result)
}

export function parseFairValueEvidence(
  result: ApplicationResult,
  expectedMeasurement: FairValueMeasurement,
): FairValueEvidenceBundle {
  const parsed = evidenceResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.measurementId !== expectedMeasurement.measurementId ||
    parsed.data.evidenceHash !== expectedMeasurement.evidenceHash
  ) {
    throw unsupportedDetail("evidence")
  }
  return withMetadata(
    { evidenceHash: parsed.data.evidenceHash, inputs: parsed.data.inputs },
    result,
  )
}

export function parseFairValueApprovals(
  result: ApplicationResult,
  expectedMeasurementId: string,
): FairValueApprovalBundle {
  const parsed = approvalResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.measurementId !== expectedMeasurementId ||
    parsed.data.approvals.some(
      (approval) => approval.measurementId !== expectedMeasurementId,
    )
  ) {
    throw unsupportedDetail("approval status")
  }
  return withMetadata(
    { at: parsed.data.at, approvals: parsed.data.approvals },
    result,
  )
}

export function parseFairValueAuditPage(
  result: ApplicationResult,
): FairValueAuditPage {
  const parsed = auditPageSchema.safeParse(result.data)
  if (!parsed.success) throw unsupportedDetail("audit history")
  return withMetadata(parsed.data, result)
}

export function parseFairValueMarketAccess(
  result: ApplicationResult,
  expectedAssessmentId: string,
): FairValueMarketAccess {
  const parsed = marketAccessResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.marketAccess.assessmentId !== expectedAssessmentId
  ) {
    throw unsupportedDetail("market-access assessment")
  }
  return parsed.data.marketAccess
}

export function parseFairValueClassificationControl(
  result: ApplicationResult,
  expectedMeasurement: FairValueMeasurement,
): { classification: FairValueClassification; replay: boolean } {
  const parsed = classificationControlResultSchema.safeParse(result.data)
  if (
    !parsed.success ||
    parsed.data.classification.measurementId !== expectedMeasurement.measurementId ||
    parsed.data.classification.evidenceHash !== expectedMeasurement.evidenceHash
  ) {
    throw unsupportedDetail("classification result")
  }
  return {
    classification: parsed.data.classification,
    replay: parsed.data.classificationReplay,
  }
}

export function parseGovernancePrincipals(result: ApplicationResult): GovernancePrincipal[] {
  const parsed = z
    .object({
      principals: z.array(governancePrincipalSchema),
      nextAfter: identifierSchema.nullable(),
    })
    .safeParse(result.data)
  if (!parsed.success) throw unsupportedDetail("governance principal")
  return parsed.data.principals
}

export function parseFairValueGovernancePreview(
  result: ApplicationResult,
): GovernanceActionPreview {
  const parsed = z.object({ preview: governancePreviewSchema }).safeParse(result.data)
  if (!parsed.success) throw unsupportedDetail("governance preview")
  return parsed.data.preview
}

export function parseGovernanceAuthorization(
  result: ApplicationResult,
  expectedPreviewId: string,
): GovernanceAuthorization {
  const parsed = z.object({ authorization: governanceAuthorizationSchema }).safeParse(result.data)
  if (!parsed.success || parsed.data.authorization.previewId !== expectedPreviewId) {
    throw unsupportedDetail("governance authorization")
  }
  return parsed.data.authorization
}

export function parseFairValueGovernanceCommit(
  result: ApplicationResult,
  expectedPreviewId: string,
): GovernanceCommit {
  const parsed = governanceCommitSchema.safeParse(result.data)
  if (!parsed.success || parsed.data.receipt.previewId !== expectedPreviewId) {
    throw unsupportedDetail("governance commit")
  }
  return parsed.data.receipt
}

function withMetadata<Value extends object>(
  value: Value,
  result: ApplicationResult,
): Value & {
  completeness: string
  returnedItems: number
  availableItems: number
} {
  return {
    ...value,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

function unsupportedDetail(section: string) {
  return new Error(
    `The installed service returned unsupported fair-value ${section} data.`,
  )
}
