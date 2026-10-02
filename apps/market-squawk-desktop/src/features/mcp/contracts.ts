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
  if (client.service.credentialRotationRecoveryPending) {
    if (client.state === "repair_required") return ["repair"]
    if (client.state === "owned" && !client.service.accessRevoked) {
      return ["verify"]
    }
    return []
  }
  switch (client.state) {
    case "ready":
      return ["connect"]
    case "owned":
      return [
        "verify",
        "rotateCredential",
        "revokeCredential",
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
      return "Check connection"
    case "repair":
      return "Repair connection"
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
      return `Add a Market Squawk connection to ${clientLabel} for your user account.`
    case "reconnect":
      return `Restore access for ${clientLabel} and its Market Squawk connection.`
    case "verify":
      return `Check that ${clientLabel} can connect and read workspace information.`
    case "repair":
      return `Repair the connection created by Market Squawk in ${clientLabel}.`
    case "rotateCredential":
      return `Create a new credential for ${clientLabel}. Its previous credential will stop working.`
    case "revokeCredential":
      return `Stop ${clientLabel} from accessing the workspace. Keep its configuration so you can reconnect later.`
    case "disconnect":
      return `Remove the connection created by Market Squawk in ${clientLabel}, then check its access status.`
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
        detail: "This client was not found in the supported installation locations.",
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
        detail: "This client is installed and ready to connect.",
        tone: "ready",
      }
    case "owned":
      return {
        label: "Connected",
        detail: "The Market Squawk connection is configured.",
        tone: "ready",
      }
    case "repair_required":
      return {
        label: "Repair required",
        detail: "The connection configuration needs updating.",
        tone: "attention",
      }
    case "access_revoked":
      return {
        label: "Access revoked",
        detail: "This client cannot access the workspace until you reconnect it.",
        tone: "attention",
      }
    case "conflict":
      return {
        label: "Name conflict",
        detail: "A connection with this name already exists. Market Squawk cannot replace it because it did not create it.",
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
