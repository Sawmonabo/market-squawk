import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const MAXIMUM_PAGE_ITEMS = 250
const MAXIMUM_TEXT_BYTES = 256
const MAXIMUM_DECIMAL_BYTES = 96

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const uuidSchema = z.string().uuid()
const boundedTextSchema = z.string().min(1).max(MAXIMUM_TEXT_BYTES)
const rfc3339Schema = z.string().datetime({ offset: true })
const exactDecimalSchema = z
  .string()
  .min(1)
  .max(MAXIMUM_DECIMAL_BYTES)
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const amountBasisSchema = z.enum([
  "per_instrument_unit",
  "reporting_entity_total",
  "position_total",
])
const losslessIntegerSchema = z.union([
  z.number().int().safe(),
  z.string().regex(/^-?\d+$/),
])

const moneySchema = z
  .object({
    amount: exactDecimalSchema,
    currency: currencySchema,
  })
  .strict()

const revisionSchema = z
  .object({
    revisionId: digestSchema,
    effectiveAtUnixNanos: z.string().regex(/^-?\d+$/),
    availableAtUnixNanos: z.string().regex(/^-?\d+$/).nullable(),
    sourceId: boundedTextSchema,
    sourceCoverage: z.array(boundedTextSchema).max(MAXIMUM_PAGE_ITEMS),
    artifactSha256: digestSchema,
    holdingCount: z.number().int().nonnegative(),
    transactionCount: z.number().int().nonnegative(),
    reconciliationDiscrepancies: z.number().int().nonnegative(),
  })
  .strict()

const accountSchema = z
  .object({
    accountId: uuidSchema,
    currency: currencySchema,
    currentRevision: revisionSchema,
    holdingCount: z.number().int().nonnegative(),
    transactionCount: z.number().int().nonnegative(),
    reconciliationDiscrepancies: z.number().int().nonnegative(),
  })
  .strict()

const holdingBasisSchema = z.discriminatedUnion("status", [
  z
    .object({
      status: z.literal("resolved"),
      observation: z
        .object({
          amount: moneySchema,
          lot_method: boundedTextSchema,
          source_reference: boundedTextSchema,
        })
        .strict(),
    })
    .strict(),
  z.object({ status: z.literal("missing") }).strict(),
  z
    .object({
      status: z.literal("ambiguous"),
      candidates: z.array(moneySchema).max(MAXIMUM_PAGE_ITEMS),
      lot_method: boundedTextSchema,
    })
    .strict(),
])

const markEvidenceSchema = z
  .object({
    sourceReference: boundedTextSchema,
    observedAtUnixNanos: losslessIntegerSchema,
    venue: boundedTextSchema.nullable(),
    venueStatus: boundedTextSchema,
    state: boundedTextSchema,
    quality: boundedTextSchema,
    executionEligible: z.boolean(),
    freshness: z
      .object({ status: boundedTextSchema, reason: boundedTextSchema })
      .strict(),
    fallback: z
      .object({ status: boundedTextSchema, reason: boundedTextSchema })
      .strict(),
  })
  .strict()

const holdingSchema = z
  .object({
    account_id: uuidSchema,
    instrument_id: uuidSchema,
    currency: currencySchema,
    quantity: exactDecimalSchema,
    lot_size: exactDecimalSchema,
    market_value: moneySchema,
    as_of: losslessIntegerSchema,
    basis: holdingBasisSchema,
    source_reference: boundedTextSchema,
    revisionId: digestSchema,
    effectiveAtUnixNanos: z.string().regex(/^-?\d+$/),
    availableAtUnixNanos: z.string().regex(/^-?\d+$/).nullable(),
    sourceId: boundedTextSchema,
    artifactSha256: digestSchema,
    markEvidence: markEvidenceSchema,
  })
  .strict()

const principalSchema = z
  .object({
    principalId: uuidSchema,
    displayName: boundedTextSchema,
    roles: z.array(boundedTextSchema).min(1).max(32),
  })
  .strict()

const amountSchema = z
  .object({
    amount: exactDecimalSchema,
    currency: currencySchema,
    scale: z.number().int().min(0).max(28),
    amountBasis: amountBasisSchema,
  })
  .strict()

const hierarchySchema = z.enum(["level_1", "level_2", "level_3", "unclassified"])
const methodSchema = z.enum([
  "quoted_market_price",
  "market_approach",
  "income_approach",
  "cost_approach",
])

const classificationSchema = z
  .object({
    decisionId: digestSchema,
    measurementId: digestSchema,
    evidenceHash: digestSchema,
    rulesetVersion: z.number().int().positive(),
    rulesetHash: digestSchema,
    hierarchy: hierarchySchema,
    basis: z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("rules") }).strict(),
      z
        .object({
          kind: z.literal("override"),
          baseDecisionId: digestSchema,
          overrideId: digestSchema,
        })
        .strict(),
    ]),
    truthTableItemCount: z.number().int().nonnegative(),
    reasonCount: z.number().int().nonnegative(),
  })
  .strict()

const measurementSchema = z
  .object({
    measurementId: digestSchema,
    evidenceHash: digestSchema,
    accountId: uuidSchema,
    instrumentId: uuidSchema,
    amount: amountSchema,
    measurementAt: rfc3339Schema,
    preparedAt: rfc3339Schema,
    preparedBy: principalSchema.shape.principalId,
    method: methodSchema,
    inputCount: z.literal(1),
  })
  .strict()

const measurementResultSchema = z
  .object({
    measurement: measurementSchema,
    classification: classificationSchema,
    measurementReplay: z.boolean(),
    classificationReplay: z.boolean(),
  })
  .strict()

const portfolioEvidenceSchema = z
  .object({
    measurementId: digestSchema,
    evidenceHash: digestSchema,
    inputs: z
      .array(
        z
          .object({
            inputId: digestSchema,
            subjectInstrumentId: uuidSchema,
            referenceInstrumentId: uuidSchema,
            relationship: z.literal("identical"),
            amount: amountSchema,
            significance: z.enum(["significant", "not_significant"]),
            observability: z.literal("observable"),
            adjustment: z.literal("none"),
            marketActivity: z.literal("not_assessed"),
            marketAccess: z.literal("not_assessed"),
            marketAccessAssessment: z.null(),
            dataQuality: z.literal("estimated"),
            useAssessment: z.null(),
            evidence: z
              .object({
                evidenceHash: digestSchema,
                sourceId: boundedTextSchema,
                sourceIdentifier: boundedTextSchema,
                payloadDigest: z
                  .object({
                    algorithm: z.enum(["sha256", "blake3"]),
                    digest: digestSchema,
                  })
                  .strict(),
                origin: z
                  .object({
                    kind: z.literal("portfolio"),
                    revision: digestSchema,
                    accountId: uuidSchema,
                    positionQuantity: exactDecimalSchema,
                    pointInTimeDigest: digestSchema,
                  })
                  .strict(),
                sourceTimestamp: rfc3339Schema.nullable(),
                effectiveAt: rfc3339Schema.nullable(),
                publishedAt: rfc3339Schema.nullable(),
                availableAt: rfc3339Schema.nullable(),
                receivedAt: rfc3339Schema.nullable(),
                qualificationEvaluatedAt: rfc3339Schema.nullable(),
                qualificationValidUntil: rfc3339Schema.nullable(),
                ingestedAt: rfc3339Schema,
                verification: z.literal("verified"),
              })
              .strict(),
          })
          .strict(),
      )
      .length(1),
  })
  .strict()

export type PortfolioMeasurementAccount = z.infer<typeof accountSchema>
export type PortfolioMeasurementHolding = z.infer<typeof holdingSchema>
export type PortfolioMeasurementPrincipal = z.infer<typeof principalSchema>
export type PortfolioMeasurementMethod = z.infer<typeof methodSchema>
export type PortfolioMeasurementAmountBasis = z.infer<typeof amountBasisSchema>
export type PortfolioMeasurementClassification = z.infer<typeof classificationSchema>
export type PortfolioMeasurementResult = z.infer<typeof measurementResultSchema>

export interface PortfolioMeasurementPage {
  accounts: PortfolioMeasurementAccount[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface PortfolioHoldingSnapshot {
  holdings: PortfolioMeasurementHolding[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface PortfolioMeasurementPrincipalPage {
  principals: PortfolioMeasurementPrincipal[]
  nextAfter: string | null
}

export function parsePortfolioMeasurementAccounts(
  result: ApplicationResult,
): PortfolioMeasurementPage {
  const parsed = z.array(accountSchema).max(MAXIMUM_PAGE_ITEMS).safeParse(result.data)
  if (!parsed.success) throw incompatible("portfolio accounts")
  verifyPageMetadata(result, parsed.data.length)
  return {
    accounts: parsed.data,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parsePortfolioMeasurementHoldings(
  result: ApplicationResult,
  expectedAccount: PortfolioMeasurementAccount,
): PortfolioHoldingSnapshot {
  const parsed = z.array(holdingSchema).max(MAXIMUM_PAGE_ITEMS).safeParse(result.data)
  if (!parsed.success) throw incompatible("portfolio holdings")
  if (
    parsed.data.some(
      (holding) =>
        holding.account_id !== expectedAccount.accountId ||
        holding.revisionId !== expectedAccount.currentRevision.revisionId ||
        holding.artifactSha256 !== expectedAccount.currentRevision.artifactSha256,
    )
  ) {
    throw changedPortfolio()
  }
  verifyPageMetadata(result, parsed.data.length)
  return {
    holdings: parsed.data,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parsePortfolioMeasurementPrincipals(
  result: ApplicationResult,
): PortfolioMeasurementPrincipalPage {
  const parsed = z
    .object({
      principals: z.array(principalSchema).max(MAXIMUM_PAGE_ITEMS),
      nextAfter: principalSchema.shape.principalId.nullable(),
    })
    .strict()
    .safeParse(result.data)
  if (!parsed.success) throw incompatible("governance principals")
  verifyPageMetadata(result, parsed.data.principals.length)
  return parsed.data
}

export function parsePortfolioMeasurementResult(
  result: ApplicationResult,
  expected: {
    accountId: string
    instrumentId: string
    amount: string
    currency: string
    scale: number
    amountBasis: PortfolioMeasurementAmountBasis
    method: PortfolioMeasurementMethod
    preparedBy: string
    at: string
  },
): PortfolioMeasurementResult {
  const parsed = measurementResultSchema.safeParse(result.data)
  if (!parsed.success) throw incompatible("measurement result")
  const { measurement, classification } = parsed.data
  if (
    measurement.accountId !== expected.accountId ||
    measurement.instrumentId !== expected.instrumentId ||
    measurement.amount.amount !== expected.amount ||
    measurement.amount.currency !== expected.currency ||
    measurement.amount.scale !== expected.scale ||
    measurement.amount.amountBasis !== expected.amountBasis ||
    measurement.method !== expected.method ||
    measurement.preparedBy !== expected.preparedBy ||
    Date.parse(measurement.measurementAt) !== Date.parse(expected.at) ||
    Date.parse(measurement.preparedAt) !== Date.parse(expected.at) ||
    classification.measurementId !== measurement.measurementId ||
    classification.evidenceHash !== measurement.evidenceHash
  ) {
    throw incompatible("measurement identity")
  }
  verifySingleResult(result)
  return parsed.data
}

export function verifyPortfolioMeasurementEvidence(
  result: ApplicationResult,
  expected: {
    measurementId: string
    evidenceHash: string
    accountId: string
    instrumentId: string
    revisionId: string
    quantity: string
    portfolioAmount: string
    portfolioCurrency: string
    significance: "significant" | "not_significant"
  },
) {
  const parsed = portfolioEvidenceSchema.safeParse(result.data)
  if (!parsed.success) throw incompatible("portfolio valuation evidence")
  const input = parsed.data.inputs[0]
  if (
    parsed.data.measurementId !== expected.measurementId ||
    parsed.data.evidenceHash !== expected.evidenceHash ||
    input?.subjectInstrumentId !== expected.instrumentId ||
    input.referenceInstrumentId !== expected.instrumentId ||
    input.amount.amount !== expected.portfolioAmount ||
    input.amount.currency !== expected.portfolioCurrency ||
    input.amount.scale !== decimalPlaces(expected.portfolioAmount) ||
    input.amount.amountBasis !== "position_total" ||
    input.significance !== expected.significance ||
    input.evidence.sourceId !== "market-squawk.portfolio" ||
    input.evidence.payloadDigest.algorithm !== "sha256" ||
    input.evidence.payloadDigest.digest !== expected.revisionId ||
    input.evidence.origin.accountId !== expected.accountId ||
    input.evidence.origin.revision !== expected.revisionId ||
    input.evidence.origin.positionQuantity !== expected.quantity
  ) {
    throw changedPortfolio()
  }
  verifySingleResult(result)
}

function decimalPlaces(value: string) {
  return value.split(".")[1]?.length ?? 0
}

export function exactAmountError(amount: string, scale: number): string | null {
  if (!Number.isInteger(scale) || scale < 0 || scale > 28) {
    return "Scale must be a whole number from 0 through 28."
  }
  if (!exactDecimalSchema.safeParse(amount).success) {
    return "Enter an exact decimal amount without commas, symbols, or scientific notation."
  }
  const unsigned = amount.startsWith("-") ? amount.slice(1) : amount
  const [whole = "", fraction = ""] = unsigned.split(".")
  if (whole.length + fraction.length > 29) {
    return "The amount may contain at most 29 digits."
  }
  if (fraction.length > scale) {
    return `Scale ${scale} cannot exactly represent ${fraction.length} decimal places.`
  }
  return null
}

export function samePortfolioAccount(
  left: PortfolioMeasurementAccount,
  right: PortfolioMeasurementAccount,
) {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function samePortfolioHolding(
  left: PortfolioMeasurementHolding,
  right: PortfolioMeasurementHolding,
) {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function changedPortfolio() {
  return new Error(
    "The portfolio changed while this measurement was being prepared. Refresh the evidence and review the current holding before trying again.",
  )
}

function verifyPageMetadata(result: ApplicationResult, returned: number) {
  if (
    result.metadata.returnedItems !== returned ||
    result.metadata.availableItems < returned ||
    (result.metadata.completeness === "complete" &&
      result.metadata.availableItems !== returned)
  ) {
    throw incompatible("result bounds")
  }
}

function verifySingleResult(result: ApplicationResult) {
  if (
    result.metadata.completeness !== "complete" ||
    result.metadata.returnedItems !== 1 ||
    result.metadata.availableItems !== 1
  ) {
    throw incompatible("result bounds")
  }
}

function incompatible(section: string) {
  return new Error(
    `The installed service returned ${section} this dashboard cannot safely interpret.`,
  )
}
