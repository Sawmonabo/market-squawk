import { z } from "zod"

const decimalInteger = /^-?\d+$/

/** An exact integer transported without exceeding JavaScript's safe-number range. */
export const losslessIntegerSchema = z.union([
  z.string().regex(decimalInteger),
  z.number().int().safe().transform(String),
])

export type LosslessInteger = z.infer<typeof losslessIntegerSchema>

export function compareLosslessIntegers(
  left: LosslessInteger,
  right: LosslessInteger,
): number {
  const leftValue = BigInt(left)
  const rightValue = BigInt(right)
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0
}
