import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import type { PaperControlRequest } from "@/lib/transport"

const timestampSchema = z.union([z.string(), z.number().int()])
const moneySchema = z
  .object({ amount: z.string(), currency: z.string().min(1) })
  .strict()
const boundedRowsSchema = <T extends z.ZodTypeAny>(row: T) =>
  z
    .object({
      rows: z.array(row),
      returnedItems: z.number().int().nonnegative(),
      availableItems: z.number().int().nonnegative(),
    })
    .strict()

const paperAccountSchema = z
  .object({
    accountId: z.string().uuid(),
    eligible: z.boolean(),
    currency: z.string().min(1),
    settledCapital: moneySchema,
    markedEquity: moneySchema,
    peakMarkedEquity: moneySchema,
    grossExposure: moneySchema,
    unrealizedPnl: moneySchema,
    realizedPnl: moneySchema,
    realizedLoss: moneySchema,
    drawdown: moneySchema,
  })
  .strict()
const paperCashSchema = z
  .object({ accountId: z.string().uuid(), balance: moneySchema })
  .strict()
const paperPositionSchema = z
  .object({
    accountId: z.string().uuid(),
    instrumentId: z.string().uuid(),
    lots: z.number().int(),
    costBasis: moneySchema,
  })
  .strict()

const riskLimitsSchema = z
  .object({
    currency: z.string().min(1),
    eligibleInstruments: boundedRowsSchema(z.string().uuid()),
    maximumPositionLots: z.number().int().positive(),
    maximumOrderNotional: moneySchema,
    maximumGrossExposure: moneySchema,
    maximumLeverageBasisPoints: z.number().int().nonnegative(),
    minimumCapital: moneySchema,
    maximumLoss: moneySchema,
    maximumDrawdown: moneySchema,
    maximumFeeBasisPoints: z.number().int().nonnegative(),
    maximumPriceDeviationBasisPoints: z.number().int().nonnegative(),
    maximumSlippageBasisPoints: z.number().int().nonnegative(),
    maximumOrdersPerWindow: z.number().int().positive(),
    orderRateWindowNanos: z.number().int().positive(),
    allowShort: z.boolean(),
    killSwitch: z.boolean(),
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
    orderToken: z.string().uuid(),
    instrumentId: z.string().uuid(),
    maximumPriceTicks: z.number().int().positive().nullable(),
    marketObservedAt: timestampSchema,
    validUntil: timestampSchema,
    observedAt: timestampSchema,
    reasons: z.array(productRiskReasonSchema).max(14),
  })
  .strict()
const riskDecisionsSchema = z
  .object({
    records: z.array(auditDecisionSchema),
    returnedItems: z.number().int().nonnegative(),
    availableItems: z.number().int().nonnegative(),
  })
  .strict()

const simulationSchema = z
  .object({
    minimumLatencyNanos: z.number().int().nonnegative(),
    maximumLatencyNanos: z.number().int().nonnegative(),
    cancelLatencyNanos: z.number().int().nonnegative(),
    maximumMarkAgeNanos: z.number().int().positive(),
    maximumParticipationBasisPoints: z.number().int().positive(),
    impactBasisPointsPerLevel: z.number().int().nonnegative(),
    makerFeeBasisPoints: z.number().int().nonnegative(),
    takerFeeBasisPoints: z.number().int().nonnegative(),
    minimumFee: moneySchema,
    maximumFee: moneySchema.nullable(),
  })
  .strict()
const reconciliationSchema = z
  .object({
    snapshotComplete: z.boolean(),
    reconciliationRequired: z.boolean(),
    financialReconciliationCurrent: z.boolean(),
    activeOrderCount: z.number().int().nonnegative(),
    archivedOrderCount: z.number().int().nonnegative(),
    fillCount: z.number().int().nonnegative(),
    accountCount: z.number().int().nonnegative(),
    cashBalanceCount: z.number().int().nonnegative(),
    positionCount: z.number().int().nonnegative(),
  })
  .strict()

const paperStatusSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("stopped"), lastShutdownComplete: z.boolean().nullable() }).strict(),
  z.object({ state: z.literal("starting") }).strict(),
  z.object({ state: z.literal("stopping") }).strict(),
  z
    .object({
      state: z.literal("failed"),
      recoveryAction: z.literal("stop_before_restart").optional(),
      requiresStop: z.literal(true),
    })
    .strict(),
  z
    .object({
      state: z.literal("running"),
      strategyMode: z.enum(["manual", "book_imbalance"]),
      complete: z.boolean(),
      reconciliationRequired: z.boolean(),
      financialReconciliationCurrent: z.boolean(),
      orders: z.number().int().nonnegative(),
      fills: z.number().int().nonnegative(),
      positions: z.number().int().nonnegative(),
      accounts: boundedRowsSchema(paperAccountSchema),
      cash: boundedRowsSchema(paperCashSchema),
      positionRecords: boundedRowsSchema(paperPositionSchema),
      riskLimits: riskLimitsSchema,
      riskDecisions: riskDecisionsSchema,
      simulation: simulationSchema,
      reconciliation: reconciliationSchema,
    })
    .strict(),
])

const observedOrderSchema = z
  .object({
    firstFillAt: timestampSchema.nullable(),
    firstFillAfterEligibilityNanos: z.number().int().nullable(),
    averageFillSlippageTicks: z.number().int().nullable(),
    averageFillSlippageBasisPoints: z.number().int().nullable(),
  })
  .strict()
const paperOrderSchema = z
  .object({
    orderToken: z.string().uuid(),
    status: z.string().min(1),
    requestedLots: z.number().int().nonnegative(),
    filledLots: z.number().int().nonnegative(),
    averageFillPriceTicks: z.number().int().nullable(),
    maximumFillPriceTicks: z.number().int().nullable(),
    maximumExecutionPriceTicks: z.number().int().positive(),
    side: z.enum(["buy", "sell"]),
    referencePriceTicks: z.number().int().positive(),
    maximumSlippageBasisPoints: z.number().int().nonnegative(),
    observed: observedOrderSchema,
    cumulativeFees: moneySchema,
    acceptedAt: timestampSchema,
    eligibleAt: timestampSchema,
    expiresAt: timestampSchema,
    targetToken: z.string().uuid().nullable(),
  })
  .strict()
const paperFillSchema = z
  .object({
    orderToken: z.string().uuid(),
    quantityLots: z.number().int().nonnegative(),
    eventAt: timestampSchema,
    averagePriceTicks: z.number().int(),
    maximumPriceTicks: z.number().int(),
    notional: moneySchema,
    fee: moneySchema,
    liquidity: z.string().min(1),
  })
  .strict()

const startResultSchema = z
  .object({ state: z.literal("running"), strategyMode: z.enum(["manual", "book_imbalance"]) })
  .strict()
const stopResultSchema = z
  .object({
    state: z.literal("stopped"),
    shutdownComplete: z.boolean(),
    reason: z.string().min(1),
  })
  .strict()
const cancelResultSchema = z
  .object({
    orderToken: z.string().uuid(),
    status: z.enum(["pending", "canceled", "already_terminal"]),
    observedAt: timestampSchema,
    cumulativeFilledLots: z.number().int().nonnegative(),
    averageFillPriceTicks: z.number().int().nullable(),
    maximumFillPriceTicks: z.number().int().nullable(),
    cumulativeFees: moneySchema,
  })
  .strict()
const reconcileResultSchema = z
  .object({
    observedAt: timestampSchema,
    ordersChecked: z.number().int().nonnegative(),
    accountsChecked: z.number().int().nonnegative(),
    marketDataReady: z.boolean(),
    reconciliationRequired: z.boolean(),
  })
  .strict()

export type PaperStatus = z.infer<typeof paperStatusSchema>
export type PaperOrder = z.infer<typeof paperOrderSchema>
export type PaperFill = z.infer<typeof paperFillSchema>
export type PaperAccount = z.infer<typeof paperAccountSchema>
export type PaperPosition = z.infer<typeof paperPositionSchema>
export type PaperRiskLimits = z.infer<typeof riskLimitsSchema>
export type PaperAuditDecision = z.infer<typeof auditDecisionSchema>
export type PaperRiskDecisions = z.infer<typeof riskDecisionsSchema>
export type PaperControlResult =
  | { action: "start"; value: z.infer<typeof startResultSchema> }
  | { action: "stop" | "triggerKillSwitch"; value: z.infer<typeof stopResultSchema> }
  | { action: "cancel"; value: z.infer<typeof cancelResultSchema> }
  | { action: "reconcile"; value: z.infer<typeof reconcileResultSchema> }

export interface PaperResult<T> {
  value: T
  completeness: string
  returnedItems: number
  availableItems: number
}

export function parsePaperStatus(result: ApplicationResult): PaperResult<PaperStatus> {
  return boundary(result, paperStatusSchema.parse(result.data))
}

export function parsePaperOrders(result: ApplicationResult): PaperResult<PaperOrder[]> {
  return boundary(result, parseNullableRows(result.data, paperOrderSchema))
}

export function parsePaperFills(result: ApplicationResult): PaperResult<PaperFill[]> {
  return boundary(result, parseNullableRows(result.data, paperFillSchema))
}

export function parsePaperControlResult(
  result: ApplicationResult,
  request: PaperControlRequest,
): PaperControlResult {
  requireCompleteAction(result)
  switch (request.action) {
    case "start": {
      const value = startResultSchema.parse(result.data)
      if (value.strategyMode !== request.strategyMode) throw new Error("Paper start mismatch.")
      return { action: request.action, value }
    }
    case "stop":
    case "triggerKillSwitch": {
      const value = stopResultSchema.parse(result.data)
      if (value.reason !== request.reason) throw new Error("Paper stop mismatch.")
      return { action: request.action, value }
    }
    case "cancel": {
      const value = cancelResultSchema.parse(result.data)
      if (value.orderToken !== request.orderToken) throw new Error("Paper cancellation mismatch.")
      return { action: request.action, value }
    }
    case "reconcile":
      return { action: request.action, value: reconcileResultSchema.parse(result.data) }
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
