import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import type { ManualPaperRequest, ProductTransport } from "@/lib/transport"

const RAW_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
const targetTokenSchema = z
  .string()
  .min(16)
  .max(512)
  .refine((value) => !RAW_UUID.test(value), "Expected an opaque product target token.")
const timestampSchema = z.string().datetime({ offset: true })
const moneySchema = z
  .object({
    amount: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
  })
  .strict()
const investmentSchema = z
  .object({
    name: z.string().min(1).max(256),
    symbol: z.string().min(1).max(64).nullable(),
  })
  .strict()
const targetLevelSchema = z.enum([
  "downside",
  "add",
  "entry_lower",
  "entry_upper",
  "base",
  "trim_lower",
  "trim_upper",
  "exit_lower",
  "exit_upper",
  "upside",
])
const timeInForceSchema = z.enum([
  "day",
  "good_til_cancelled",
  "immediate_or_cancel",
  "fill_or_kill",
])
const orderTypeSchema = z.enum(["market", "limit", "stop", "stop_limit"])
const orderSideSchema = z.enum(["buy", "sell"])

const choiceLabelSchema = z.string().min(1).max(160)
const sideChoiceSchema = z
  .object({
    value: orderSideSchema,
    label: choiceLabelSchema,
    explanation: z.string().min(1).max(512),
  })
  .strict()
const timeInForceChoiceSchema = z
  .object({
    value: timeInForceSchema,
    label: choiceLabelSchema,
    explanation: z.string().min(1).max(512),
  })
  .strict()
const orderChoiceSchema = z
  .object({
    value: orderTypeSchema,
    label: choiceLabelSchema,
    explanation: z.string().min(1).max(512),
    requiresLimitLevel: z.boolean(),
    requiresStopLevel: z.boolean(),
    timeInForceChoices: z.array(timeInForceChoiceSchema).min(1).max(4),
  })
  .strict()

const governedTargetSchema = z
  .object({
    targetToken: targetTokenSchema,
    investment: investmentSchema,
    thesis: z.string().min(1).max(4_096),
    expiresAt: timestampSchema,
    reviewDueAt: timestampSchema,
    ladder: z
      .array(
        z
          .object({
            level: targetLevelSchema,
            label: z.string().min(1).max(96),
            value: moneySchema,
          })
          .strict(),
      )
      .length(10),
    sideChoices: z.array(sideChoiceSchema).min(1).max(2),
    orderChoices: z.array(orderChoiceSchema).min(1).max(4),
  })
  .strict()

const targetCatalogSchema = z
  .object({ targets: z.array(governedTargetSchema).max(100) })
  .strict()
const acceptedManualPaperDraftSchema = z
  .object({
    accepted: z.literal(true),
    message: z.string().min(1).max(512),
  })
  .strict()
const manualPaperPreviewSchema = z
  .object({
    confirmationToken: targetTokenSchema,
    expiresAt: timestampSchema,
    investment: investmentSchema,
    direction: z.enum(["Buy", "Sell"]),
    orderApproach: z.enum(["Market", "Limit", "Stop", "Stop limit"]),
    quantity: z.string().min(1).max(128),
    duration: z.enum([
      "Today",
      "Until cancelled",
      "Fill now or cancel",
      "All now or cancel",
    ]),
    limitCondition: z
      .object({ label: z.string().min(1).max(96), value: moneySchema })
      .strict()
      .nullable(),
    stopCondition: z
      .object({ label: z.string().min(1).max(96), value: moneySchema })
      .strict()
      .nullable(),
    safeguards: z
      .object({
        maximumOrderValue: moneySchema,
        maximumSlippage: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?%$/),
        shorting: z.enum(["allowed", "disabled"]),
      })
      .strict(),
    simulationWarning: z.string().min(1).max(1_000),
  })
  .strict()
const completeMetadataSchema = z
  .object({
    completeness: z.literal("complete"),
    returnedItems: z.number().int().nonnegative(),
    availableItems: z.number().int().nonnegative(),
  })
  .passthrough()

export type GovernedPaperTarget = z.infer<typeof governedTargetSchema>
export type TargetLevel = z.infer<typeof targetLevelSchema>
export type ManualPaperOrderType = z.infer<typeof orderTypeSchema>
export type ManualPaperSide = z.infer<typeof orderSideSchema>
export type ManualPaperTimeInForce = z.infer<typeof timeInForceSchema>
export type ManualPaperPrepare = Extract<ManualPaperRequest, { action: "prepareManual" }>
export type ManualPaperPreview = z.infer<typeof manualPaperPreviewSchema>

export type ManualPaperTransport = Pick<ProductTransport, "manualPaper">

export function asManualPaperTransport(value: unknown): ManualPaperTransport | null {
  if (
    typeof value !== "object" ||
    value === null ||
    !("manualPaper" in value) ||
    typeof value.manualPaper !== "function"
  ) {
    return null
  }
  return value as ManualPaperTransport
}

export function parseGovernedPaperTargets(result: ApplicationResult): GovernedPaperTarget[] {
  const catalog = targetCatalogSchema.safeParse(result.data)
  const metadata = completeMetadataSchema.safeParse(result.metadata)
  if (
    !catalog.success ||
    !metadata.success ||
    metadata.data.returnedItems !== catalog.data.targets.length ||
    metadata.data.availableItems !== catalog.data.targets.length ||
    !hasExactLadder(catalog.data.targets) ||
    !hasUniquePreparedChoices(catalog.data.targets)
  ) {
    throw new Error("Prepared paper choices are unavailable.")
  }
  return catalog.data.targets
}

export function parseAcceptedManualPaperDraft(result: ApplicationResult): string {
  const accepted = acceptedManualPaperDraftSchema.safeParse(result.data)
  const metadata = completeMetadataSchema.safeParse(result.metadata)
  if (
    !accepted.success ||
    !metadata.success ||
    metadata.data.returnedItems !== 1 ||
    metadata.data.availableItems !== 1
  ) {
    throw new Error("The paper draft was not accepted.")
  }
  return accepted.data.message
}

export function parseManualPaperPreview(result: ApplicationResult): ManualPaperPreview {
  const preview = manualPaperPreviewSchema.safeParse(result.data)
  const metadata = completeMetadataSchema.safeParse(result.metadata)
  if (
    !preview.success ||
    !metadata.success ||
    metadata.data.returnedItems !== 1 ||
    metadata.data.availableItems !== 1
  ) {
    throw new Error("The paper trade preview is unavailable.")
  }
  return preview.data
}

export function isPositiveLotQuantity(value: string): boolean {
  if (!/^[1-9]\d{0,18}$/.test(value)) return false
  try {
    return BigInt(value) <= 9_223_372_036_854_775_807n
  } catch {
    return false
  }
}

function hasExactLadder(targets: GovernedPaperTarget[]): boolean {
  const expected = new Set<TargetLevel>([
    "downside",
    "add",
    "entry_lower",
    "entry_upper",
    "base",
    "trim_lower",
    "trim_upper",
    "exit_lower",
    "exit_upper",
    "upside",
  ])
  return targets.every((target) => {
    const levels = target.ladder.map((entry) => entry.level)
    return (
      levels.length === expected.size &&
      new Set(levels).size === expected.size &&
      levels.every((level) => expected.has(level))
    )
  })
}

function hasUniquePreparedChoices(targets: GovernedPaperTarget[]): boolean {
  return targets.every(
    (target) =>
      new Set(target.sideChoices.map((choice) => choice.value)).size ===
        target.sideChoices.length &&
      new Set(target.orderChoices.map((choice) => choice.value)).size ===
        target.orderChoices.length &&
      target.orderChoices.every(
        (choice) =>
          new Set(choice.timeInForceChoices.map((entry) => entry.value)).size ===
          choice.timeInForceChoices.length,
      ),
  )
}
