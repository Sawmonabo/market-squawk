import { z } from "zod"

export const lookupCategories = [
  "company",
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

export const productLookupCategories = [
  "company",
  "dataset",
  "instrument",
  "model",
  "portfolio",
  "screen",
  "target",
] as const satisfies readonly LookupCategory[]

export type ProductLookupCategory = (typeof productLookupCategories)[number]

export const instrumentLookupDetailSchema = z.object({
  displayName: z.string(),
  companyName: z.string().nullable(),
  assetClass: z.string(),
  tradingStatus: z.string(),
  quoteCurrency: z.string(),
  matchReasons: z.array(
    z.object({
      label: z.string(),
      value: z.string(),
      venueId: z.string().optional(),
      current: z.boolean(),
    }),
  ).max(8),
  matchReasonsTruncated: z.boolean(),
})

const lookupDestinationSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("market_instrument"),
    instrumentId: z.string().uuid(),
  }),
  z.object({
    kind: z.literal("research_company"),
  }),
])

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
