import { z } from "zod"

export const lookupCategories = [
  "command",
  "dataset",
  "instrument",
  "job",
  "model",
  "portfolio",
  "provider",
  "screen",
  "target",
] as const

export type LookupCategory = (typeof lookupCategories)[number]

export const instrumentLookupDetailSchema = z.object({
  displayName: z.string(),
  companyName: z.string().nullable(),
  assetClass: z.string(),
  tradingStatus: z.string(),
  quoteCurrency: z.string(),
  definitionRevision: z.number(),
  definitionObservedAt: z.unknown(),
  venueMappings: z.array(z.record(z.string(), z.unknown())),
  matchReasons: z.array(
    z.object({
      kind: z.string(),
      label: z.string(),
      value: z.string(),
      venueId: z.string().optional(),
      sourceId: z.string().optional(),
      current: z.boolean(),
      evidence: z.record(z.string(), z.unknown()),
    }),
  ).max(8),
  matchReasonsTruncated: z.boolean(),
})

const lookupDestinationSchema = z.object({
  kind: z.literal("market_instrument"),
  instrumentId: z.string().uuid(),
})

export const lookupMatchSchema = z.object({
  category: z.enum(lookupCategories),
  id: z.string(),
  label: z.string(),
  detail: z.record(z.string(), z.unknown()),
  destination: lookupDestinationSchema.optional(),
})

export const lookupResultSchema = z.object({
  query: z.string(),
  matches: z.array(lookupMatchSchema).max(64),
  categories: z.array(
    z.object({
      category: z.enum(lookupCategories),
      state: z.enum(["available", "unavailable"]),
      reason: z.string().optional(),
    }),
  ),
  truncated: z.boolean(),
})

export type LookupMatch = z.infer<typeof lookupMatchSchema>
export type LookupResult = z.infer<typeof lookupResultSchema>
export type InstrumentLookupDetail = z.infer<typeof instrumentLookupDetailSchema>
