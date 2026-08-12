import type { DesktopEvent } from "@/lib/schemas"

import type { ProductScope } from "./query-client"

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

export function affectedDomain(event: DesktopEvent): string | null {
  return event.body.type === "authority_changed" ? event.body.domain : null
}

export function isRetryableDisconnect(event: DesktopEvent): boolean {
  return event.body.type === "stream_disconnected"
}
