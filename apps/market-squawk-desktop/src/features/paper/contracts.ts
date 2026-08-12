import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import type { PaperControlRequest } from "@/lib/transport"

const timestampSchema = z.union([z.string(), z.number().int()])

const moneySchema = z.object({
  amount: z.string(),
  currency: z.string().min(1),
})

const boundedRowsSchema = <T extends z.ZodTypeAny>(row: T) =>
  z.object({
    rows: z.array(row),
    returnedItems: z.number().int().nonnegative(),
    availableItems: z.number().int().nonnegative(),
  })

const paperAccountSchema = z.object({
  accountId: z.string().min(1),
  revision: z.number().int().positive(),
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
  markDigestSha256: z.string().length(64),
})

const paperCashSchema = z.object({ accountId: z.string().min(1), balance: moneySchema })

const paperPositionSchema = z.object({
  accountId: z.string().min(1),
  instrumentId: z.string().min(1),
  lots: z.number().int(),
  costBasis: moneySchema,
})

const riskLimitsSchema = z.object({
  currency: z.string().min(1),
  eligibleInstruments: boundedRowsSchema(z.string().min(1)),
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
  reservationTtlNanos: z.number().int().positive(),
  allowShort: z.boolean(),
  killSwitch: z.boolean(),
})

const auditDecisionSchema = z.object({
  sequence: z.number().int().positive(),
  kind: z.string().min(1),
  approvalId: z.string().min(1),
  orderId: z.string().min(1),
  accountId: z.string().min(1),
  instrumentId: z.string().min(1),
  strategyId: z.string().min(1),
  modelId: z.string().nullable(),
  intentDigestSha256: z.string().length(64),
  assessmentDigestSha256: z.string().length(64).nullable(),
  evidenceBindingDigestSha256: z.string().length(64).nullable(),
  executionIdentityDigestSha256: z.string().length(64).nullable(),
  portfolioContentDigestSha256: z.string().length(64).nullable(),
  maximumExecutionPriceTicks: z.number().int().positive().nullable(),
  riskPolicyDigestSha256: z.string().length(64),
  riskPolicyRulesetVersion: z.number().int().positive(),
  marketObservedAt: timestampSchema,
  validUntil: timestampSchema,
  observedAt: timestampSchema,
  reasons: z.array(z.unknown()),
})

const riskDecisionsSchema = z.object({
  records: z.array(auditDecisionSchema),
  returnedItems: z.number().int().nonnegative(),
  availableItems: z.number().int().nonnegative(),
  totalPublished: z.number().int().nonnegative(),
  oldestSequence: z.number().int().positive().nullable(),
  latestSequence: z.number().int().positive().nullable(),
  cursorExpired: z.boolean(),
  nextCursor: z.number().int().positive().nullable(),
})

const simulationSchema = z.object({
  configurationVersion: z.number().int().positive(),
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

const reconciliationSchema = z.object({
  snapshotSequence: z.number().int().nonnegative(),
  snapshotComplete: z.boolean(),
  configurationDigestSha256: z.string().length(64),
  reconciliationRequired: z.boolean(),
  financialReconciliationCurrent: z.boolean(),
  activeOrderCount: z.number().int().nonnegative(),
  archivedOrderCount: z.number().int().nonnegative(),
  fillCount: z.number().int().nonnegative(),
  accountCount: z.number().int().nonnegative(),
  cashBalanceCount: z.number().int().nonnegative(),
  positionCount: z.number().int().nonnegative(),
})

const paperStatusSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("stopped"),
    lastShutdownComplete: z.boolean().nullable(),
  }),
  z.object({ state: z.literal("starting") }),
  z.object({ state: z.literal("stopping") }),
  z.object({
    state: z.literal("failed"),
    provider: z.string().min(1).optional(),
    reason: z.string().min(1).optional(),
    requiresStop: z.literal(true),
  }),
  z.object({
    state: z.literal("running"),
    strategyMode: z.enum(["manual", "book_imbalance"]),
    sequence: z.number().int().nonnegative(),
    complete: z.boolean(),
    reconciliationRequired: z.boolean(),
    financialReconciliationCurrent: z.boolean(),
    orders: z.number().int().nonnegative(),
    fills: z.number().int().nonnegative(),
    positions: z.number().int().nonnegative(),
    accounts: boundedRowsSchema(paperAccountSchema).optional(),
    cash: boundedRowsSchema(paperCashSchema).optional(),
    positionEvidence: boundedRowsSchema(paperPositionSchema).optional(),
    configurationDigestSha256: z.string().length(64).optional(),
    riskLimits: riskLimitsSchema.optional(),
    riskDecisions: riskDecisionsSchema.optional(),
    simulation: simulationSchema.optional(),
    reconciliation: reconciliationSchema.optional(),
  }),
])

const paperOrderSchema = z
  .object({
    orderId: z.string().min(1),
    accountId: z.string().min(1).optional(),
    state: z.string().min(1),
    requestedLots: z.number().int().nonnegative(),
    filledLots: z.number().int().nonnegative().optional(),
    averageFillPriceTicks: z.number().int().nullable().optional(),
    maximumFillPriceTicks: z.number().int().nullable().optional(),
    maximumExecutionPriceTicks: z.number().int().nullable().optional(),
    side: z.string().min(1).optional(),
    referencePriceTicks: z.number().int().positive().optional(),
    maximumSlippageBasisPoints: z.number().int().nonnegative().optional(),
    observed: z.object({
      firstFillAt: timestampSchema.nullable(),
      firstFillAfterEligibilityNanos: z.number().int().nullable(),
      averageFillSlippageTicks: z.number().int().nullable(),
      averageFillSlippageBasisPoints: z.number().int().nullable(),
    }).optional(),
    cumulativeFees: moneySchema.optional(),
    acceptedAt: timestampSchema.optional(),
    eligibleAt: timestampSchema.optional(),
    expiresAt: timestampSchema.optional(),
    revision: z.number().int().nonnegative().optional(),
    targetReference: z
      .object({
        targetId: z.string().regex(/^[a-z][a-z0-9._-]{0,127}$/),
        revision: z.number().int().positive(),
        contentSha256: z.string().regex(/^[0-9a-f]{64}$/),
      })
      .strict()
      .nullable()
      .optional(),
  })
  .loose()

const startReceiptSchema = z
  .object({
    state: z.literal("running"),
    provider: z.enum(["coinbase", "coinbase-direct", "kraken"]),
    strategyMode: z.enum(["manual", "book_imbalance"]),
  })
  .strict()

const stopReceiptSchema = z
  .object({
    state: z.literal("stopped"),
    shutdownComplete: z.boolean(),
    reason: z.string().min(1),
  })
  .strict()

const cancelReceiptSchema = z
  .object({
    orderId: z.string().min(1),
    status: z.enum(["pending", "canceled", "already_terminal"]),
    observedAt: timestampSchema,
    cumulativeFilledLots: z.number().int().nonnegative(),
    averageFillPriceTicks: z.number().int().nullable(),
    maximumFillPriceTicks: z.number().int().nullable(),
    cumulativeFees: moneySchema,
  })
  .strict()

const reconcileReceiptSchema = z
  .object({
    observedAt: timestampSchema,
    orderCount: z.number().int().nonnegative(),
    accountCount: z.number().int().nonnegative(),
    sourceBound: z.boolean(),
    reconciliationRequired: z.boolean(),
  })
  .strict()

const paperFillSchema = z
  .object({
    sequence: z.number().int().nonnegative(),
    orderId: z.string().min(1),
    quantityLots: z.number().int().nonnegative(),
    eventAt: timestampSchema.optional(),
    averagePriceTicks: z.number().int().optional(),
    maximumPriceTicks: z.number().int().optional(),
    notional: moneySchema.optional(),
    fee: moneySchema.optional(),
    liquidity: z.string().min(1).optional(),
  })
  .loose()

export type PaperStatus = z.infer<typeof paperStatusSchema>
export type PaperOrder = z.infer<typeof paperOrderSchema>
export type PaperFill = z.infer<typeof paperFillSchema>
export type PaperAccount = z.infer<typeof paperAccountSchema>
export type PaperCash = z.infer<typeof paperCashSchema>
export type PaperPosition = z.infer<typeof paperPositionSchema>
export type PaperRiskLimits = z.infer<typeof riskLimitsSchema>
export type PaperAuditDecision = z.infer<typeof auditDecisionSchema>
export type PaperRiskDecisions = z.infer<typeof riskDecisionsSchema>
export type PaperControlReceipt =
  | { action: "start"; value: z.infer<typeof startReceiptSchema> }
  | { action: "stop" | "triggerKillSwitch"; value: z.infer<typeof stopReceiptSchema> }
  | { action: "cancel"; value: z.infer<typeof cancelReceiptSchema> }
  | { action: "reconcile"; value: z.infer<typeof reconcileReceiptSchema> }

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

export function parsePaperControlReceipt(
  result: ApplicationResult,
  request: PaperControlRequest,
): PaperControlReceipt {
  if (
    result.metadata.completeness !== "complete" ||
    result.metadata.returnedItems !== 1 ||
    result.metadata.availableItems !== 1
  ) {
    throw new Error("The installed service returned incomplete paper-control evidence.")
  }
  switch (request.action) {
    case "start": {
      const value = startReceiptSchema.parse(result.data)
      if (value.provider !== request.provider || value.strategyMode !== request.strategyMode) {
        throw new Error("The paper-start receipt does not match the confirmed request.")
      }
      return { action: request.action, value }
    }
    case "stop":
    case "triggerKillSwitch": {
      const value = stopReceiptSchema.parse(result.data)
      if (value.reason !== request.reason) {
        throw new Error("The paper-stop receipt does not match the confirmed reason.")
      }
      return { action: request.action, value }
    }
    case "cancel": {
      const value = cancelReceiptSchema.parse(result.data)
      if (value.orderId !== request.orderId) {
        throw new Error("The paper-cancel receipt does not match the confirmed order.")
      }
      return { action: request.action, value }
    }
    case "reconcile":
      return { action: request.action, value: reconcileReceiptSchema.parse(result.data) }
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
