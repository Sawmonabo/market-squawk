import type {
  DesktopEvent,
  DesktopInvalidationDomain,
} from "@/lib/schemas"

import type { ProductScope } from "./query-client"

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
    event.body.type !== "invalidate"
  ) {
    return true
  }
  return BigInt(event.sequence) !== BigInt(previousSequence) + 1n
}

export function affectedDomains(
  event: DesktopEvent,
): readonly DesktopInvalidationDomain[] {
  return event.body.type === "invalidate" ? event.body.domains : []
}
