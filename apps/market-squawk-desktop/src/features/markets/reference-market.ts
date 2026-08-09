import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

export const referenceMarketRowSchema = z
  .object({
    referenceId: z.string().min(1),
    symbol: z.string().min(1),
    name: z.string().min(1),
    venueId: z.string().min(1),
    assetClass: z.enum(["equity", "fund"]),
    referenceOnly: z.literal(true),
    isEtf: z.boolean(),
    roundLotSize: z.number().int().nonnegative(),
    directoryPresence: z.literal("current_directory"),
    quality: z.literal("official_delayed"),
    effectiveAt: z.string().min(1),
    availableAt: z.string().min(1),
    sourceId: z.string().min(1),
    providerId: z.string().min(1),
    sourcePayloadSha256: z.string().regex(/^[0-9a-f]{64}$/),
    matchKind: z.string().min(1),
    quoteAvailability: z.literal("account_required"),
  })
  .strict()

const referenceMarketRowsSchema = z.array(referenceMarketRowSchema).nullable()

export type ReferenceMarketRow = z.infer<typeof referenceMarketRowSchema>

export function referenceMarketRows(
  result: ApplicationResult | undefined,
): ReferenceMarketRow[] {
  if (!result) return []
  return referenceMarketRowsSchema.parse(result.data) ?? []
}
