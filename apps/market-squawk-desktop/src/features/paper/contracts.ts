import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const RAW_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

const actionTokenSchema = z
  .string()
  .min(16)
  .max(512)
  .refine((value) => !RAW_UUID.test(value), "Expected an opaque product action token.")
const timestampSchema = z.string().datetime({ offset: true })
const productTextSchema = z.string().min(1).max(4_096)
const moneySchema = z
  .object({
    amount: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
  })
  .strict()
const percentageSchema = z
  .string()
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?%$/)

const investmentSchema = z
  .object({
    name: z.string().min(1).max(256),
    symbol: z.string().min(1).max(64).nullable(),
  })
  .strict()

const boundedRowsSchema = <T extends z.ZodTypeAny>(row: T) =>
  z
    .object({
      rows: z.array(row),
      returnedItems: z.number().int().nonnegative(),
      availableItems: z.number().int().nonnegative(),
    })
    .strict()
    .superRefine((value, context) => {
      if (
        value.returnedItems !== value.rows.length ||
        value.returnedItems > value.availableItems
      ) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Paper result counts do not match the returned rows.",
        })
      }
    })

const paperAccountSchema = z
  .object({
    displayName: z.string().min(1).max(256),
    eligible: z.boolean(),
    settledCapital: moneySchema,
    markedEquity: moneySchema,
    peakMarkedEquity: moneySchema,
    grossExposure: moneySchema,
    unrealizedPnl: moneySchema,
    realizedPnl: moneySchema,
    maximumDrawdown: moneySchema,
  })
  .strict()

const paperPositionSchema = z
  .object({
    accountName: z.string().min(1).max(256),
    investment: investmentSchema,
    quantity: z.string().min(1).max(64),
    costBasis: moneySchema,
  })
  .strict()

const safetySummarySchema = z
  .object({
    maximumOrderValue: moneySchema,
    maximumTotalExposure: moneySchema,
    maximumPosition: z.string().min(1).max(96),
    leverageLimit: percentageSchema,
    minimumCapital: moneySchema,
    maximumLoss: moneySchema,
    maximumDrawdown: moneySchema,
    maximumFees: percentageSchema,
    maximumPriceDeviation: percentageSchema,
    maximumSlippage: percentageSchema,
    orderPace: z.string().min(1).max(160),
    shorting: z.enum(["allowed", "disabled"]),
    emergencyStop: z.enum(["clear", "engaged"]),
    eligibleInvestments: boundedRowsSchema(investmentSchema),
  })
  .strict()

const productRiskReasonSchema = z.enum([
  "The virtual order was declined.",
  "The order result needs review before continuing.",
  "The account needs reconciliation before another order.",
  "The order or account changed before the check completed.",
  "Paper trading is temporarily unavailable. Try again.",
  "Market data is unavailable or too old.",
  "The investment cannot be traded right now.",
  "The order is no longer valid at current conditions.",
  "The order is outside the active price and slippage limits.",
  "Paper trading is paused by the emergency stop.",
  "Available cash or holdings are insufficient.",
  "The investment is not eligible for paper trading.",
  "The order is outside the active safety limits.",
  "The virtual account is not eligible for paper trading.",
])

const auditDecisionSchema = z
  .object({
    outcome: z.enum([
      "declined",
      "approved",
      "accepted",
      "needs_review",
      "cancel_requested",
      "cancelled",
      "reconciled",
    ]),
    investment: investmentSchema,
    marketObservedAt: timestampSchema,
    validUntil: timestampSchema,
    observedAt: timestampSchema,
    reasons: z.array(productRiskReasonSchema).max(14),
  })
  .strict()

const reconciliationSchema = z
  .object({
    state: z.enum(["current", "action_needed", "incomplete"]),
    activeOrders: z.number().int().nonnegative(),
    completedOrders: z.number().int().nonnegative(),
    fills: z.number().int().nonnegative(),
    accounts: z.number().int().nonnegative(),
    positions: z.number().int().nonnegative(),
  })
  .strict()

const paperStatusSchema = z.discriminatedUnion("sessionAvailability", [
  z
    .object({
      sessionAvailability: z.enum(["ready", "unavailable"]),
      safeguards: z.enum(["active", "action_needed"]),
    })
    .strict(),
  z
    .object({
      sessionAvailability: z.literal("active"),
      safeguards: z.enum(["active", "action_needed"]),
      modeLabel: z.string().min(1).max(160),
      accountUpdate: z.enum(["complete", "incomplete"]),
      accounts: boundedRowsSchema(paperAccountSchema),
      positions: boundedRowsSchema(paperPositionSchema),
      safety: safetySummarySchema,
      recentDecisions: boundedRowsSchema(auditDecisionSchema),
      reconciliation: reconciliationSchema,
    })
    .strict(),
])

const paperOrderSchema = z
  .object({
    actionToken: actionTokenSchema,
    state: z.enum([
      "waiting",
      "accepted",
      "partially_filled",
      "filled",
      "cancel_requested",
      "cancelled",
      "declined",
      "expired",
    ]),
    investment: investmentSchema,
    direction: z.enum(["buy", "sell"]),
    requestedQuantity: z.string().min(1).max(64),
    filledQuantity: z.string().min(1).max(64),
    averageFillPrice: moneySchema.nullable(),
    maximumExecutionPrice: moneySchema,
    maximumSlippage: percentageSchema,
    fees: moneySchema,
    acceptedAt: timestampSchema,
    expiresAt: timestampSchema,
    targetLinked: z.boolean(),
    cancellationAvailable: z.boolean(),
  })
  .strict()

const paperFillSchema = z
  .object({
    investment: investmentSchema,
    quantity: z.string().min(1).max(64),
    averagePrice: moneySchema,
    maximumPrice: moneySchema,
    notional: moneySchema,
    fee: moneySchema,
    occurredAt: timestampSchema,
  })
  .strict()

const stopResultSchema = z
  .object({
    sessionAvailability: z.literal("ready"),
    safeguards: z.literal("active"),
    message: productTextSchema,
  })
  .strict()
const productChoiceTokenSchema = actionTokenSchema
const startPreparationSchema = z
  .object({
    virtualCashChoices: z
      .array(
        z
          .object({
            choiceToken: productChoiceTokenSchema,
            label: z.string().min(1).max(96),
            amount: moneySchema,
            explanation: z.string().min(1).max(1_000),
          })
          .strict(),
      )
      .length(3),
    costChoices: z
      .array(
        z
          .object({
            choiceToken: productChoiceTokenSchema,
            label: z.string().min(1).max(96),
            estimatedTradingCost: percentageSchema,
            explanation: z.string().min(1).max(1_000),
          })
          .strict(),
      )
      .length(3),
    modeChoices: z
      .array(
        z
          .object({
            choiceToken: productChoiceTokenSchema,
            label: z.string().min(1).max(96),
            explanation: z.string().min(1).max(1_000),
          })
          .strict(),
      )
      .length(2),
  })
  .strict()
const startPreviewSchema = z
  .object({
    confirmationToken: actionTokenSchema,
    expiresAt: timestampSchema,
    virtualCash: moneySchema,
    estimatedTradingCost: percentageSchema,
    modeLabel: z.string().min(1).max(96),
    safeguards: z.array(z.string().min(1).max(1_000)).length(3),
  })
  .strict()
const startResultSchema = z
  .object({
    sessionAvailability: z.literal("active"),
    safeguards: z.literal("active"),
    modeLabel: z.string().min(1).max(96),
    message: z.string().min(1).max(512),
  })
  .strict()
const cancelResultSchema = z
  .object({
    actionToken: actionTokenSchema,
    state: z.enum(["pending", "cancelled", "already_complete"]),
    observedAt: timestampSchema,
    filledQuantity: z.string().min(1).max(64),
    averageFillPrice: moneySchema.nullable(),
    fees: moneySchema,
  })
  .strict()
export type PaperStatus = z.infer<typeof paperStatusSchema>
export type PaperOrder = z.infer<typeof paperOrderSchema>
export type PaperFill = z.infer<typeof paperFillSchema>
export type PaperAccount = z.infer<typeof paperAccountSchema>
export type PaperPosition = z.infer<typeof paperPositionSchema>
export type PaperAuditDecision = z.infer<typeof auditDecisionSchema>
export type PaperStartPreparation = z.infer<typeof startPreparationSchema>
export type PaperStartPreview = z.infer<typeof startPreviewSchema>
export type PaperControlIntent =
  | { action: "stop" | "triggerKillSwitch"; reason: string }
  | { action: "cancel"; actionToken: string }
export type PaperControlResult =
  | { action: "stop" | "triggerKillSwitch"; value: z.infer<typeof stopResultSchema> }
  | { action: "cancel"; value: z.infer<typeof cancelResultSchema> }

export interface PaperResult<T> {
  value: T
  completeness: string
  returnedItems: number
  availableItems: number
}

export function parsePaperStatus(result: ApplicationResult): PaperResult<PaperStatus> {
  requireSingleResult(result)
  return boundary(result, paperStatusSchema.parse(result.data))
}

export function parsePaperOrders(result: ApplicationResult): PaperResult<PaperOrder[]> {
  return rowBoundary(result, parseNullableRows(result.data, paperOrderSchema))
}

export function parsePaperFills(result: ApplicationResult): PaperResult<PaperFill[]> {
  return rowBoundary(result, parseNullableRows(result.data, paperFillSchema))
}

export function parsePaperStartPreparation(result: ApplicationResult): PaperStartPreparation {
  requireCompleteAction(result)
  return startPreparationSchema.parse(result.data)
}

export function parsePaperStartPreview(result: ApplicationResult): PaperStartPreview {
  requireCompleteAction(result)
  return startPreviewSchema.parse(result.data)
}

export function parsePaperStartResult(result: ApplicationResult): string {
  requireCompleteAction(result)
  return startResultSchema.parse(result.data).message
}

export function parsePaperControlResult(
  result: ApplicationResult,
  request: PaperControlIntent,
): PaperControlResult {
  requireCompleteAction(result)
  switch (request.action) {
    case "stop":
    case "triggerKillSwitch":
      return { action: request.action, value: stopResultSchema.parse(result.data) }
    case "cancel": {
      const value = cancelResultSchema.parse(result.data)
      if (value.actionToken !== request.actionToken) {
        throw new Error("Paper cancellation result does not match the selected order.")
      }
      return { action: request.action, value }
    }
  }
}

function requireCompleteAction(result: ApplicationResult): void {
  if (
    result.metadata.completeness !== "complete" ||
    result.metadata.returnedItems !== 1 ||
    result.metadata.availableItems !== 1
  ) {
    throw new Error("Paper action result is incomplete.")
  }
}

function requireSingleResult(result: ApplicationResult): void {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("Paper status is incomplete.")
  }
}

function parseNullableRows<T>(value: unknown, schema: z.ZodType<T>): T[] {
  if (value === null) return []
  return z.array(schema).parse(value)
}

function boundary<T>(result: ApplicationResult, value: T): PaperResult<T> {
  return {
    value,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

function rowBoundary<T>(result: ApplicationResult, value: T[]): PaperResult<T[]> {
  if (
    result.metadata.returnedItems !== value.length ||
    result.metadata.returnedItems > result.metadata.availableItems
  ) {
    throw new Error("Paper history counts do not match the returned records.")
  }
  return boundary(result, value)
}
