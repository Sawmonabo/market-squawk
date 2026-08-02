import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.union([z.string(), z.number().int()])

const moneySchema = z.object({
  amount: z.string(),
  currency: z.string().min(1),
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
    provider: z.string().min(1),
    requiresStop: z.literal(true),
  }),
  z.object({
    state: z.literal("running"),
    sequence: z.number().int().nonnegative(),
    complete: z.boolean(),
    reconciliationRequired: z.boolean(),
    financialReconciliationCurrent: z.boolean(),
    orders: z.number().int().nonnegative(),
    fills: z.number().int().nonnegative(),
    positions: z.number().int().nonnegative(),
  }),
])

const paperOrderSchema = z
  .object({
    orderId: z.string().min(1),
    state: z.string().min(1),
    requestedLots: z.number().int().nonnegative(),
    filledLots: z.number().int().nonnegative().optional(),
    averageFillPriceTicks: z.number().int().nullable().optional(),
    maximumFillPriceTicks: z.number().int().nullable().optional(),
    maximumExecutionPriceTicks: z.number().int().nullable().optional(),
    cumulativeFees: moneySchema.optional(),
    acceptedAt: timestampSchema.optional(),
    eligibleAt: timestampSchema.optional(),
    expiresAt: timestampSchema.optional(),
    revision: z.number().int().nonnegative().optional(),
  })
  .loose()

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
