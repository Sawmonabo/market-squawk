import type { McpClientsStatus as SharedMcpClientsStatus } from "@/lib/schemas"
import type { McpClientControlRequest as SharedMcpClientControlRequest } from "@/lib/transport"

export type McpClientControlRequest = SharedMcpClientControlRequest
export type McpClientAction = McpClientControlRequest["action"]
export type McpClientKind = McpClientControlRequest["client"]
export type McpClientState =
  | "absent"
  | "unsupported"
  | "ready"
  | "owned"
  | "repair_required"
  | "access_revoked"
  | "conflict"

export type McpClientsStatus = SharedMcpClientsStatus
export type McpClientView = McpClientsStatus["clients"][number]
export type McpServiceClientStatus = McpClientView["service"]
export type McpRuntimeStatus = McpClientsStatus["runtime"]

export function availableActions(client: McpClientView): McpClientAction[] {
  switch (client.state) {
    case "ready":
      return ["connect"]
    case "owned":
      return [
        "verify",
        "rotateCredential",
        "revokeCredential",
        "repair",
        "disconnect",
      ]
    case "repair_required":
      return ["repair", "disconnect"]
    case "access_revoked":
      return ["reconnect", "disconnect"]
    case "absent":
    case "unsupported":
    case "conflict":
      return []
  }
}

export function actionLabel(action: McpClientAction) {
  switch (action) {
    case "connect":
      return "Connect"
    case "reconnect":
      return "Reconnect"
    case "verify":
      return "Verify connection"
    case "repair":
      return "Repair owned entry"
    case "rotateCredential":
      return "Rotate credential"
    case "revokeCredential":
      return "Revoke access"
    case "disconnect":
      return "Disconnect"
  }
}

export function actionDescription(
  action: McpClientAction,
  clientLabel: string,
) {
  switch (action) {
    case "connect":
      return `Create one user-level Market Squawk entry in ${clientLabel} through its supported command interface.`
    case "reconnect":
      return `Re-enable ${clientLabel}'s current protected service credential and restore its owned entry.`
    case "verify":
      return `Initialize a real ${clientLabel} relay session, discover capabilities, and perform one bounded safe read.`
    case "repair":
      return `Restore only the ${clientLabel} entry proven by Market Squawk's owned receipt.`
    case "rotateCredential":
      return `Replace ${clientLabel}'s protected service credential, revoke its prior generation, and update the owned receipt.`
    case "revokeCredential":
      return `Revoke ${clientLabel}'s current service access while retaining its owned entry for an explicit reconnect.`
    case "disconnect":
      return `Remove only the ${clientLabel} entry proven by Market Squawk's owned receipt, then refresh its access state.`
  }
}

export function statePresentation(state: McpClientState): {
  label: string
  detail: string
  tone: "ready" | "attention" | "muted"
} {
  switch (state) {
    case "absent":
      return {
        label: "Not detected",
        detail: "This client is not installed in a controlled discovery location.",
        tone: "muted",
      }
    case "unsupported":
      return {
        label: "Update required",
        detail: "The installed client does not support the required official MCP commands.",
        tone: "attention",
      }
    case "ready":
      return {
        label: "Ready to connect",
        detail: "The supported client is installed and has no Market Squawk entry.",
        tone: "ready",
      }
    case "owned":
      return {
        label: "Connected",
        detail: "The exact client entry matches Market Squawk's owned receipt.",
        tone: "ready",
      }
    case "repair_required":
      return {
        label: "Repair required",
        detail: "The owned entry belongs to an earlier service or credential identity.",
        tone: "attention",
      }
    case "access_revoked":
      return {
        label: "Access revoked",
        detail: "The shared service rejects this owned client until reconnect is confirmed.",
        tone: "attention",
      }
    case "conflict":
      return {
        label: "Name conflict",
        detail: "A same-name entry is present but is not proven to be owned by Market Squawk.",
        tone: "attention",
      }
  }
}

export function formatObservedAt(unixSeconds: number) {
  if (!Number.isSafeInteger(unixSeconds) || unixSeconds < 0) return "Invalid timestamp"
  const date = new Date(unixSeconds * 1_000)
  return Number.isNaN(date.valueOf())
    ? "Invalid timestamp"
    : new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "medium",
      }).format(date)
}
