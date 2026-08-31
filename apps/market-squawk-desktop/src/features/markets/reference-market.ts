import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import { marketSelectionTokenSchema } from "./market-product"

export const investmentSearchRowSchema = z.object({
  selectionToken: marketSelectionTokenSchema,
  symbol: z.string().trim().min(1).max(64).nullable(),
  name: z.string().trim().min(1).max(256).nullable(),
  kind: z.enum(["stock", "fund", "bond", "option", "future", "currency", "crypto", "commodity", "index", "cash"]),
}).strict().refine((row) => row.symbol !== null || row.name !== null, {
  message: "An investment needs a display name or symbol.",
})

const investmentSearchResultSchema = z.object({
  data: z.array(investmentSearchRowSchema).max(100),
  page: z.object({ hasMore: z.boolean(), nextPageToken: z.string().regex(/^page_[A-Za-z0-9_-]{32,86}$/).nullable() }).strict(),
}).strict().superRefine((result, context) => {
  if (result.page.hasMore !== (result.page.nextPageToken !== null)) {
    context.addIssue({ code: "custom", message: "Investment search continuation is inconsistent." })
  }
})

export type InvestmentSearchRow = z.infer<typeof investmentSearchRowSchema>
export type InvestmentSearchResult = z.infer<typeof investmentSearchResultSchema>

export function parseInvestmentSearchPage(result: ApplicationResult): InvestmentSearchResult {
  const parsed = investmentSearchResultSchema.parse(result.data)
  if (parsed.data.length !== result.metadata.returnedItems) {
    throw new Error("Investment search count is inconsistent.")
  }
  return parsed
}

export function parseInvestmentSearchResult(result: ApplicationResult): InvestmentSearchRow[] {
  return parseInvestmentSearchPage(result).data
}
