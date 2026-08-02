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

export const lookupMatchSchema = z.object({
  category: z.enum(lookupCategories),
  id: z.string(),
  label: z.string(),
  detail: z.record(z.string(), z.unknown()),
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
