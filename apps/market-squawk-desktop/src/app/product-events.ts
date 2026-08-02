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
  previousSequence: number,
  event: DesktopEvent,
): boolean {
  return (
    !sameRuntime(scope, event.runtime) ||
    event.body.type === "resync_required" ||
    event.sequence !== previousSequence + 1
  )
}

export function affectedDomain(event: DesktopEvent): string | null {
  return event.body.type === "authority_changed" ? event.body.domain : null
}
