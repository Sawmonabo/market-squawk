import { z } from "zod"

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
