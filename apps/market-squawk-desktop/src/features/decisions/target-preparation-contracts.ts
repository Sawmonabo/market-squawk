import { z } from "zod"

import type { MoneyValue } from "@/lib/formatters"
import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z
  .union([z.string().regex(/^-?\d+$/), z.number().int()])
  .transform(String)
const identitySchema = z.string().min(1).max(256)
const uuidSchema = z.string().uuid()
const moneySchema = z
  .object({
    amount: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/),
    currency: z.string().min(3).max(8),
  })
  .strict() satisfies z.ZodType<MoneyValue>
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
const targetMethodSchema = z.enum([
  "comparable_evidence",
  "discounted_cash_flow",
  "residual_income",
  "forecast_distribution",
  "fair_value_measurement",
])
const referenceMarkSchema = z
  .object({
    selector: uuidSchema,
    price: moneySchema,
    observedAt: timestampSchema,
    quality: dataQualitySchema,
    source: identitySchema,
  })
  .strict()
const targetPreparationSchema = z
  .object({
    dossierId: identitySchema,
    instrumentId: uuidSchema,
    assembledAt: timestampSchema,
    forecastOptions: z
      .array(z.object({ index: z.number().int().nonnegative() }).strict())
      .max(4_096),
    fairValueAvailable: z.boolean(),
    portfolioAvailable: z.boolean(),
    referenceMarks: z.array(referenceMarkSchema).max(4_096),
  })
  .strict()
const assumptionEvidenceSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("dossier") }).strict(),
  z
    .object({ kind: z.literal("dossier_reference"), index: z.number().int().nonnegative() })
    .strict(),
  z.object({ kind: z.literal("forecast") }).strict(),
  z.object({ kind: z.literal("fair_value") }).strict(),
  z.object({ kind: z.literal("portfolio") }).strict(),
  z.object({ kind: z.literal("reference_mark") }).strict(),
])
const targetPricesSchema = z
  .object({
    downside: moneySchema,
    add: moneySchema,
    entryLower: moneySchema,
    entryUpper: moneySchema,
    base: moneySchema,
    trimLower: moneySchema,
    trimUpper: moneySchema,
    exitLower: moneySchema,
    exitUpper: moneySchema,
    upside: moneySchema,
  })
  .strict()
const preparedTargetSchema = z
  .object({
    receiptId: uuidSchema,
    receiptExpiresAt: timestampSchema,
    targetId: identitySchema,
    revision: z.number().int().positive(),
    dossierId: identitySchema,
    instrumentId: uuidSchema,
    intent: z.enum(["buy", "sell", "hold"]),
    referenceMark: moneySchema,
    referenceMarkObservedAt: timestampSchema,
    referenceMarkQuality: dataQualitySchema,
    referenceMarkSource: identitySchema,
    prices: targetPricesSchema,
    method: targetMethodSchema,
    assumptions: z
      .array(
        z
          .object({
            text: z.string().min(1).max(4_096),
            evidence: assumptionEvidenceSchema,
          })
          .strict(),
      )
      .max(32),
    thesis: z.string().min(1).max(4_096),
    risks: z.array(z.string().min(1).max(4_096)).max(32),
    invalidationConditions: z.array(z.string().min(1).max(4_096)).max(32),
    createdAt: timestampSchema,
    horizonAt: timestampSchema,
    expiresAt: timestampSchema,
    reviewDueAt: timestampSchema,
    author: identitySchema,
    rulesetVersion: z.number().int().positive(),
    forecastSelected: z.boolean(),
    fairValueSelected: z.boolean(),
    portfolioSelected: z.boolean(),
  })
  .strict()
const targetCommitSchema = z
  .object({ outcome: z.enum(["appended", "already_present"]) })
  .strict()
const notApplicableSchema = z.object({ status: z.literal("not_applicable") }).strict()
const completeMetadataSchema = z
  .object({
    completeness: z.literal("complete"),
    returnedItems: z.number().int().positive(),
    availableItems: z.number().int().positive(),
    sourceCoverage: notApplicableSchema,
    dataQuality: notApplicableSchema,
  })
  .strict()

export type TargetPreparationView = z.infer<typeof targetPreparationSchema>
export type TargetReferenceMarkView = z.infer<typeof referenceMarkSchema>
export type PreparedTargetView = z.infer<typeof preparedTargetSchema>
export type AssumptionEvidenceView = z.infer<typeof assumptionEvidenceSchema>
export type TargetCommitOutcome = z.infer<typeof targetCommitSchema>["outcome"]

export function parseTargetPreparation(result: ApplicationResult): TargetPreparationView {
  const inventory = parseComplete(targetPreparationSchema, result, "target evidence inventory")
  const expectedItems = Math.max(inventory.referenceMarks.length, 1)
  if (result.metadata.returnedItems !== expectedItems) {
    throw new Error("The installed service returned inconsistent target evidence metadata.")
  }
  const selectors = inventory.referenceMarks.map((mark) => mark.selector)
  const forecastIndexes = inventory.forecastOptions.map((option) => option.index)
  const disallowedMark = inventory.referenceMarks.some((mark) =>
    ["modeled", "estimated", "stale", "quarantined"].includes(mark.quality),
  )
  if (
    disallowedMark ||
    new Set(selectors).size !== selectors.length ||
    new Set(forecastIndexes).size !== forecastIndexes.length ||
    forecastIndexes.some(
      (index, position) =>
        index >= 4_096 || (position > 0 && forecastIndexes[position - 1]! >= index),
    )
  ) {
    throw new Error("The installed service returned inconsistent target evidence choices.")
  }
  return inventory
}

export function parsePreparedTarget(result: ApplicationResult): PreparedTargetView {
  return parseComplete(preparedTargetSchema, result, "prepared target preview", 1)
}

export function parseTargetCommit(result: ApplicationResult): TargetCommitOutcome {
  return parseComplete(targetCommitSchema, result, "target commit receipt", 1).outcome
}

function parseComplete<Schema extends z.ZodType>(
  schema: Schema,
  result: ApplicationResult,
  label: string,
  expectedItems?: number,
): z.infer<Schema> {
  const data = schema.safeParse(result.data)
  const metadata = completeMetadataSchema.safeParse(result.metadata)
  if (
    !data.success ||
    !metadata.success ||
    metadata.data.returnedItems !== metadata.data.availableItems ||
    (expectedItems !== undefined && metadata.data.returnedItems !== expectedItems)
  ) {
    throw new Error(`The installed service returned an unsupported ${label}.`)
  }
  return data.data
}
