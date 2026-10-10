import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import { marketSelectionTokenSchema, productInstantSchema } from "./market-product"

const referenceProfileSchema = z.object({
  displayName: z.string().min(1),
  symbol: z.string().min(1),
  assetClass: z.enum(["equity", "fixed_income", "option", "future", "foreign_exchange", "crypto", "commodity", "fund", "index", "cash"]),
  currency: z.string().regex(/^[A-Z]{3}$/),
  listingVenue: z.string().min(1),
  exchangeTradedFund: z.boolean(),
  roundLotSize: z.number().int().nonnegative().max(4_294_967_295),
  effectiveFrom: productInstantSchema,
  effectiveUntil: productInstantSchema.nullable(),
  knownAt: productInstantSchema,
  referenceUpdatedAt: productInstantSchema,
  lifecycle: z.literal("successor_and_delisting_not_established"),
}).strict()

const selectionFields = {
  selectionToken: marketSelectionTokenSchema,
  knowledgeAt: productInstantSchema,
}

export const investmentProfileResultSchema = z.discriminatedUnion("state", [
  z.object({ ...selectionFields, state: z.literal("available"), reason: z.null(), profile: referenceProfileSchema }).strict(),
  z.object({ ...selectionFields, state: z.literal("missing"), reason: z.enum(["canonical_definition", "official_directory", "official_membership"]), profile: z.null() }).strict(),
  z.object({ ...selectionFields, state: z.literal("ambiguous"), reason: z.null(), profile: z.null() }).strict(),
  z.object({ ...selectionFields, state: z.literal("unavailable"), reason: z.enum(["directory_read_bound", "reference_not_configured"]), profile: z.null() }).strict(),
])

export type InvestmentProfileResult = z.infer<typeof investmentProfileResultSchema>
export type InvestmentReferenceProfile = z.infer<typeof referenceProfileSchema>

export function parseInvestmentProfileResult(result: ApplicationResult, selectionToken: string): InvestmentProfileResult {
  const parsed = investmentProfileResultSchema.parse(result.data)
  if (parsed.selectionToken !== selectionToken) {
    throw new Error("The profile does not match the selected investment.")
  }
  if (result.metadata.completeness !== "complete" || result.metadata.returnedItems !== 1) {
    throw new Error("The profile response is incomplete.")
  }
  if (parsed.state === "available" && parsed.profile.knownAt !== parsed.knowledgeAt) {
    throw new Error("The profile does not match the requested information date.")
  }
  return parsed
}
