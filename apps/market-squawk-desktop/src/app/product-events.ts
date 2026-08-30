import type { DesktopEvent } from "@/lib/schemas"

import type { ProductScope } from "./query-client"

const SOURCE_MARKET_AUTHORITY_OPERATIONS = new Set([
  "Source.Start",
  "Source.Stop",
  "Source.Retry",
  "Source.Resynchronize",
  "Source.Verify",
  "Source.Reconfigure",
  "Source.Remove",
])

export function sameProductSession(
  left: ProductScope,
  right: ProductScope,
): boolean {
  return left === right
}

export function rejectsProductEvent(
  scope: ProductScope,
  previousSequence: string,
  event: DesktopEvent,
): boolean {
  if (
    !sameProductSession(scope, event.productSessionToken) ||
    event.body.type !== "authority_changed"
  ) {
    return true
  }
  return BigInt(event.sequence) !== BigInt(previousSequence) + 1n
}

export function affectedDomains(event: DesktopEvent): readonly string[] {
  if (event.body.type !== "authority_changed") return []
  if (
    event.body.domain === "source" &&
    SOURCE_MARKET_AUTHORITY_OPERATIONS.has(event.body.operation)
  ) {
    return ["source", "market"]
  }
  return [event.body.domain]
}
