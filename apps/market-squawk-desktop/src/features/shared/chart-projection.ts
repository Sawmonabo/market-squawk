import { z } from "zod"

// Display metadata describes original evidence retained by backend projections.
const nonnegativeIntegerSchema = z.string().regex(/^(?:0|[1-9]\d*)$/)
const chartTimeSchema = z.string().regex(/^-?(?:0|[1-9]\d*)$/).refine((value) => value !== "-0")

export const chartDisplaySchema = z.strictObject({
  method: z.literal("first_last_min_max"),
  originalPointCount: nonnegativeIntegerSchema,
  visibleOriginalPointCount: nonnegativeIntegerSchema,
  returnedPointCount: z.number().int().nonnegative().max(4_096),
  firstTimeUnixNanos: chartTimeSchema.nullable(),
  lastTimeUnixNanos: chartTimeSchema.nullable(),
  projectionDigest: z.string().regex(/^[0-9a-f]{64}$/),
  reduced: z.boolean(),
}).superRefine((display, context) => {
  if (BigInt(display.visibleOriginalPointCount) > BigInt(display.originalPointCount)
    || BigInt(display.returnedPointCount) > BigInt(display.visibleOriginalPointCount)
    || display.reduced !== (BigInt(display.visibleOriginalPointCount) > BigInt(display.returnedPointCount))
    || (display.firstTimeUnixNanos === null) !== (display.lastTimeUnixNanos === null)
    || display.firstTimeUnixNanos !== null && display.lastTimeUnixNanos !== null
      && BigInt(display.firstTimeUnixNanos) > BigInt(display.lastTimeUnixNanos)) {
    context.addIssue({ code: "custom", message: "The displayed saved-evidence counts or bounds are inconsistent." })
  }
})
export const chartOriginalPointFields = {
  originalOrdinal: nonnegativeIntegerSchema,
  breakBefore: z.array(z.boolean()).min(1).max(3),
}
