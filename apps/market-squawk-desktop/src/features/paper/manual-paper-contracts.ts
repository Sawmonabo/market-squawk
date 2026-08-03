import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const targetIdSchema = z.string().regex(/^[a-z][a-z0-9._-]{0,127}$/)
const targetRevisionSchema = z.number().int().positive()
const timestampSchema = z.union([z.string().min(1), z.number().int()]).transform(String)
const moneySchema = z
  .object({
    amount: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
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

const governedTargetSchema = z
  .object({
    targetId: targetIdSchema,
    targetRevision: targetRevisionSchema,
    instrumentId: z.string().uuid(),
    status: z.literal("active"),
    thesis: z.string().min(1).max(4_096),
    expiresAt: timestampSchema,
    reviewDueAt: timestampSchema,
    route: z
      .object({
        venueId: z.string().min(1).max(128),
        instrumentId: z.string().uuid(),
      })
      .strict(),
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
  })
  .strict()

const targetCatalogSchema = z
  .object({
    targets: z.array(governedTargetSchema).max(100),
  })
  .strict()

const acceptedManualPaperDraftSchema = z
  .object({
    state: z.literal("accepted"),
    targetId: targetIdSchema,
    targetRevision: targetRevisionSchema,
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

export type ManualPaperSubmit = {
  action: "submit"
  targetId: string
  targetRevision: number
  side: ManualPaperSide
  orderType: ManualPaperOrderType
  quantityLots: string
  limitTargetLevel?: TargetLevel
  stopTargetLevel?: TargetLevel
  timeInForce: ManualPaperTimeInForce
}

export type ManualPaperRequest = { action: "targets" } | ManualPaperSubmit

export interface ManualPaperTransport {
  manualPaper(request: ManualPaperRequest, confirmed?: boolean): Promise<ApplicationResult>
}

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
    !hasExactLadder(catalog.data.targets)
  ) {
    throw new Error("The installed service returned unsupported governed paper targets.")
  }
  return catalog.data.targets
}

export function parseAcceptedManualPaperDraft(
  result: ApplicationResult,
  request: ManualPaperSubmit,
): void {
  const receipt = acceptedManualPaperDraftSchema.safeParse(result.data)
  const metadata = completeMetadataSchema.safeParse(result.metadata)
  if (
    !receipt.success ||
    !metadata.success ||
    metadata.data.returnedItems !== 1 ||
    metadata.data.availableItems !== 1 ||
    receipt.data.targetId !== request.targetId ||
    receipt.data.targetRevision !== request.targetRevision
  ) {
    throw new Error("The installed service returned an unsupported manual paper-order receipt.")
  }
}

export function validTimeInForce(
  orderType: ManualPaperOrderType,
): readonly ManualPaperTimeInForce[] {
  switch (orderType) {
    case "market":
      return ["immediate_or_cancel", "fill_or_kill"]
    case "limit":
      return ["day", "good_til_cancelled", "immediate_or_cancel", "fill_or_kill"]
    case "stop":
    case "stop_limit":
      return ["day", "good_til_cancelled"]
  }
}

export function isPositiveLotQuantity(value: string): boolean {
  if (!/^[1-9]\d{0,18}$/.test(value)) return false
  try {
    return BigInt(value) <= 9_223_372_036_854_775_807n
  } catch {
    return false
  }
}

export function requiresLimitLevel(orderType: ManualPaperOrderType): boolean {
  return orderType === "limit" || orderType === "stop_limit"
}

export function requiresStopLevel(orderType: ManualPaperOrderType): boolean {
  return orderType === "stop" || orderType === "stop_limit"
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
    if (target.route.instrumentId !== target.instrumentId) return false
    const levels = target.ladder.map((entry) => entry.level)
    return (
      levels.length === expected.size &&
      new Set(levels).size === expected.size &&
      levels.every((level) => expected.has(level))
    )
  })
}
