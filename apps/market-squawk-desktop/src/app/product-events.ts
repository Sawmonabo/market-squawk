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

export function sameRuntime(
  left: ProductScope,
  right: ProductScope,
): boolean {
  return (
    left.installationId === right.installationId &&
    left.workspaceId === right.workspaceId &&
    left.serviceGeneration === right.serviceGeneration
  )
}

export function requiresResync(
  scope: ProductScope,
  previousSequence: string,
  event: DesktopEvent,
): boolean {
  if (!sameRuntime(scope, event.runtime) || event.body.type === "resync_required") {
    return true
  }
  return event.body.type === "stream_disconnected"
    ? event.sequence !== previousSequence
    : BigInt(event.sequence) !== BigInt(previousSequence) + 1n
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

export function isRetryableDisconnect(event: DesktopEvent): boolean {
  return event.body.type === "stream_disconnected"
}
