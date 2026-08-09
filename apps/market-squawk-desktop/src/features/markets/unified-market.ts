import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const quoteSchema = z
  .object({
    bidPrice: z.string().nullable(),
    bidPriceProviderLexeme: z.string().nullable(),
    bidSize: z.string().nullable(),
    bidSizeProviderLexeme: z.string().nullable(),
    askPrice: z.string().nullable(),
    askPriceProviderLexeme: z.string().nullable(),
    askSize: z.string().nullable(),
    askSizeProviderLexeme: z.string().nullable(),
    midPrice: z.string().nullable(),
    midPriceBasis: z.string().nullable(),
    lastPrice: z.string().nullable(),
    lastPriceProviderLexeme: z.string().nullable(),
    lastSize: z.string().nullable(),
    lastSizeProviderLexeme: z.string().nullable(),
    lastSourceTimestamp: z.string().nullable(),
    lastReceivedAt: z.string().nullable(),
    lastAvailableAt: z.string().nullable(),
    lastQuality: z.string().nullable(),
    lastFreshAtSelection: z.boolean().nullable(),
    quoteEvidence: z.record(z.string(), z.unknown()).nullable(),
    tradeEvidence: z.record(z.string(), z.unknown()).nullable(),
  })
  .strict()

const selectedSourceSchema = z
  .object({
    surfaceId: z.string().min(1),
    providerId: z.string().min(1),
    providerSymbol: z.string().min(1).optional(),
    sourceId: z.string().min(1),
    venueId: z.string().nullable(),
    providerProduct: z.string().min(1),
    providerChannel: z.string().min(1),
    timing: z.string().min(1),
    depth: z.string().nullable(),
    depthLabel: z.string().min(1),
    quality: z.string().min(1),
    coverage: z.string().min(1),
    health: z.string().min(1),
    freshness: z
      .object({
        receivedAt: z.string(),
        availableAt: z.string(),
        sourceValidUntil: z.string().nullable(),
        freshAtSelection: z.boolean(),
      })
      .passthrough(),
    integrity: z
      .object({
        state: z.string().min(1),
        phase: z.string().min(1),
        generationCurrent: z.boolean().nullable(),
        snapshotInitialized: z.boolean(),
      })
      .passthrough(),
  })
  .passthrough()

const orderLevelOrderSchema = z
  .object({
    orderId: z.string().min(1),
    side: z.enum(["bid", "ask"]),
    price: z.string().min(1),
    priceTicks: z.string().regex(/^-?[0-9]+$/),
    quantity: z.string().min(1),
    quantityLots: z.string().regex(/^-?[0-9]+$/),
    providerOrderTimestamp: z.string().nullable(),
    providerPriority: z
      .object({
        value: z.string().regex(/^[0-9]+$/),
        rule: z.string().min(1),
      })
      .strict()
      .nullable(),
    firstSeenIn: z.enum(["snapshot", "update"]),
    lastUpdatedIn: z.enum(["snapshot", "update"]),
    lastSourceTimestamp: z.string(),
    lastReceivedAt: z.string(),
    arrivalOrdinal: z.string().regex(/^[0-9]+$/),
  })
  .strict()

const orderBookSchema = z
  .object({
    depth: z.literal("order_level"),
    revision: z.string().regex(/^[0-9]+$/),
    phase: z.enum(["awaiting_snapshot", "healthy", "quarantined"]),
    quarantineReason: z.string().nullable(),
    quality: z.string().min(1),
    freshness: z.enum(["uninitialized", "fresh", "stale"]),
    lastMarketAt: z.string().nullable(),
    usableForSelection: z.boolean(),
    totalOrderCount: z.number().int().nonnegative(),
    returnedOrderCount: z.number().int().nonnegative(),
    sampleTruncated: z.boolean(),
    samplePolicy: z.literal("stable_provider_order_id_prefix"),
    orders: z.array(orderLevelOrderSchema).max(64),
  })
  .strict()

export const unifiedMarketRowSchema = z
  .object({
    instrumentId: z.string().uuid(),
    symbol: z.string().min(1),
    symbolKind: z.string().min(1),
    symbolVenueId: z.string().min(1).nullable(),
    assetClass: z.string().min(1),
    quoteCurrency: z.string().min(1),
    definitionKind: z.string().min(1),
    definitionRevision: z.number().int().positive().nullable(),
    referenceRevision: z.string().min(1).nullable(),
    permanentFigi: z.string().min(1).nullable(),
    displayName: z.string().min(1).nullable(),
    tickSize: z.string().min(1).nullable(),
    lotSize: z.string().min(1).nullable(),
    executionTermsAvailable: z.boolean(),
    referenceEvidence: z.record(z.string(), z.unknown()).nullable(),
    availability: z.string().min(1),
    confidence: z.string().min(1),
    quote: quoteSchema,
    orderBook: orderBookSchema.nullable(),
    selectedSource: selectedSourceSchema.nullable(),
    alternatives: z.array(z.record(z.string(), z.unknown())),
    selectionReceipt: z.record(z.string(), z.unknown()),
  })
  .strict()

const unifiedMarketRowsSchema = z.array(unifiedMarketRowSchema).nullable()

export type UnifiedMarketRow = z.infer<typeof unifiedMarketRowSchema>

export function unifiedMarketRows(result: ApplicationResult | undefined): UnifiedMarketRow[] {
  if (!result) return []
  return unifiedMarketRowsSchema.parse(result.data) ?? []
}
