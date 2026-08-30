import { parsePaperStatus } from "@/features/paper/contracts"
import type { SettingsSnapshot } from "@/features/settings/contracts"
import type { SourceEvidence } from "@/features/sources/source-evidence"
import type {
  DesktopBootstrap,
  McpClientsStatus,
} from "@/lib/schemas"

import {
  formatUnixSeconds,
  type PlanStep,
  type PlanStepId,
} from "./setup-plan"

export type EvidenceTone =
  | "loading"
  | "ready"
  | "recorded"
  | "unfinished"
  | "degraded"
  | "unavailable"

export interface StepEvidence {
  tone: EvidenceTone
  complete: boolean
  headline: string
  detail: string
}

export type EvidenceMap = Record<PlanStepId, StepEvidence>
export type OwnerRead<T> =
  | { ok: true; value: T }
  | { ok: false; error: string }

export function initialEvidence(bootstrap: DesktopBootstrap, loading = false): EvidenceMap {
  const base: StepEvidence = loading
    ? {
        tone: "loading",
        complete: false,
        headline: "Checking your setup",
        detail: "Current readiness will appear when these checks finish.",
      }
    : {
        tone: "unavailable",
        complete: false,
        headline: "Setup status has not been checked",
        detail: "Refresh to see what is ready and what still needs attention.",
      }
  return {
    goals_and_starter_plan: { ...base },
    storage_retention_time_and_disk: loading
      ? { ...base }
      : readinessEvidence(
          bootstrap.storage.state === "ready" && bootstrap.installation.state === "ready",
          "Workspace and installed release are ready",
          "Your workspace and installed application passed their readiness checks.",
          "Storage or release verification still needs attention",
          "Open Updates & Repair, then run this check again.",
        ),
    public_and_zero_fee_providers: { ...base },
    file_and_portfolio_import: { ...base },
    model_runtime: loading
      ? { ...base }
      : readinessEvidence(
          bootstrap.modelRuntime.state === "ready",
          "Local model runtime is verified",
          "Local forecasting and analysis can use the installed model runtime.",
          "Local model runtime is not verified",
          "Open Models to review the local runtime, then run this check again.",
        ),
    paper_and_risk: { ...base },
    claude_code: { ...base },
    codex: { ...base },
    backup: { ...base },
    review: { ...base },
    first_useful_result: { ...base },
  }
}

export function paperRiskEvidence(
  status: ReturnType<typeof parsePaperStatus>["value"],
  bootstrap: DesktopBootstrap,
): StepEvidence {
  if (status.state === "stopped") {
    const requiredOperations = [
      "Bot.GetStatus",
      "Execution.GetOrders",
      "Risk.TriggerKillSwitch",
    ]
    const missingOperations = requiredOperations.filter(
      (name) => !bootstrap.operations.some((operation) => operation.name === name),
    )
    if (missingOperations.length === 0) {
      return {
        tone: "ready",
        complete: true,
        headline: "Paper execution is stopped behind central risk",
        detail:
          "Paper trading is stopped, and its required safety controls are available.",
      }
    }
    return {
      tone: "degraded",
      complete: false,
      headline: "Some paper-trading safeguards are unavailable",
      detail:
        "Some required paper-trading safety controls are unavailable. Reopen Market Squawk, then check again.",
    }
  }
  if (status.state === "running") {
    return {
      tone: "unfinished",
      complete: false,
      headline: "Paper execution is running, not stopped",
      detail:
        "Stop the paper account in Paper Execution before completing setup.",
    }
  }
  if (status.state === "failed") {
    return {
      tone: "degraded",
      complete: false,
      headline: "Paper execution failed and requires a stop",
      detail: "Paper execution reports a failed state. Open Paper Execution and stop or recover it explicitly.",
    }
  }
  return {
    tone: "unfinished",
    complete: false,
    headline: `Paper execution is ${status.state}`,
    detail: "Wait for the change to finish, then run this check again.",
  }
}

export function mcpClientEvidence(
  label: string,
  client: McpClientsStatus["clients"][number] | undefined,
  read: OwnerRead<McpClientsStatus>,
): StepEvidence {
  if (!read.ok) {
    return unavailableEvidence(
      `${label} status is unavailable`,
      "Try again or review Logs & Diagnostics for details.",
    )
  }
  if (!client) {
    return unavailableEvidence(
      `${label} status is unavailable`,
      `Open MCP and check the ${label} connection.`,
    )
  }
  const verified = client.state === "owned" && client.verification !== null
  return verified
    ? {
        tone: "ready",
        complete: true,
        headline: `${label} is connected and verified`,
        detail: `The connection and a safe read were verified at ${formatUnixSeconds(String(client.verification!.verifiedAtUnixSeconds))}.`,
      }
    : {
        tone: client.state === "repair_required" || client.state === "conflict" ? "degraded" : "unfinished",
        complete: false,
        headline: `${label}: ${client.state.replaceAll("_", " ")}`,
        detail:
          client.blocker ??
          `Open MCP and complete ${label}'s own connect and verification actions. The other client never satisfies this step.`,
      }
}

export function readinessEvidence(
  ready: boolean,
  readyHeadline: string,
  readyDetail: string,
  unfinishedHeadline: string,
  unfinishedDetail = readyDetail,
): StepEvidence {
  return ready
    ? { tone: "ready", complete: true, headline: readyHeadline, detail: readyDetail }
    : {
        tone: "unfinished",
        complete: false,
        headline: unfinishedHeadline,
        detail: unfinishedDetail,
      }
}

export function storageEvidence(
  step: PlanStep | null,
  bootstrap: DesktopBootstrap,
  settingsRead: OwnerRead<SettingsSnapshot>,
): StepEvidence {
  if (bootstrap.storage.state !== "ready" || bootstrap.installation.state !== "ready") {
    return {
      tone: "unfinished",
      complete: false,
      headline: "Storage or installed-release verification needs attention",
      detail: "Open Updates & Repair, resolve the listed issue, then check again.",
    }
  }
  if (!settingsRead.ok) {
    return unavailableEvidence(
      "Workspace settings are unavailable",
      "Try again or review Logs & Diagnostics for details.",
    )
  }
  if (!step || step.choice.kind !== "storage") {
    return unavailableEvidence(
      "The planned storage settings are unavailable",
      "Review your storage choices, save the setup plan again, then check this item.",
    )
  }

  const retention = settingsRead.value.entries.find(
    (entry) => entry.key === "log_retention_days",
  )
  const softLimit = settingsRead.value.entries.find(
    (entry) => entry.key === "storage_soft_limit_bytes",
  )
  const expectedRetention = step.choice.retention_days
  const expectedSoftLimit = step.choice.workspace_soft_limit_bytes
  const expectedTimePolicy = step.choice.time_policy
  const matches =
    retention?.value.kind === "log_retention_days" &&
    softLimit?.value.kind === "storage_soft_limit_bytes" &&
    typeof expectedRetention === "number" &&
    (typeof expectedSoftLimit === "number" || typeof expectedSoftLimit === "string") &&
    expectedTimePolicy === "point_in_time_with_first_observed_locally_provenance" &&
    retention.value.value === expectedRetention &&
    softLimit.value.value === String(expectedSoftLimit)

  return matches
    ? {
        tone: "ready",
        complete: true,
        headline: "Workspace retention, time, and disk policy match the accepted plan",
        detail: `${expectedRetention} days retention · ${expectedSoftLimit} byte soft limit · point-in-time research.`,
      }
    : {
        tone: "degraded",
      complete: false,
      headline: "Workspace settings do not match the accepted storage plan",
      detail:
          "Open Settings to review retention and storage, then run this check again.",
      }
}

interface FirstResultFacts {
  overviewReady: boolean
  marketsReady: boolean
  marketResults: number
  researchReady: boolean
  researchCount: number
  portfolioReady: boolean
  reconciledPortfolioCount: number
  forecastsReady: boolean
  forecastCount: number
  paperReady: boolean
  mcpReady: boolean
}

export function firstResultEvidence(
  step: PlanStep | null,
  facts: FirstResultFacts,
): StepEvidence {
  if (!step || step.choice.kind !== "first_useful_result") {
    return unavailableEvidence(
      "The planned first result cannot be identified",
      "Review and save your first-result choice, then check this item again.",
    )
  }
  if (!facts.overviewReady) {
    return unavailableEvidence(
      "Home information is unavailable",
      "Refresh this check, then open Home. Review Logs & Diagnostics if the problem continues.",
    )
  }

  const result = step.choice.result
  switch (result) {
    case "verified_public_market_snapshot":
      return resultEvidence(
        facts.marketsReady,
        facts.marketResults > 0,
        "Current market information is available",
        `${facts.marketResults} investment${facts.marketResults === 1 ? " has" : "s have"} a current price on Home.`,
        "Current market information is unavailable",
        "No current price is available. Review Connections & Sources, refresh this check, then open Home.",
      )
    case "point_in_time_research_result":
      return resultEvidence(
        facts.researchReady,
        facts.researchCount > 0,
        "A point-in-time research result is available",
        `${facts.researchCount} research dataset${facts.researchCount === 1 ? " is" : "s are"} available.`,
        "The planned research result is unavailable",
        "No research dataset is available. Complete a Research workflow, refresh this check, then open Home.",
      )
    case "reconciled_portfolio_summary":
      return resultEvidence(
        facts.portfolioReady,
        facts.reconciledPortfolioCount > 0,
        "A reconciled portfolio result is available",
        `${facts.reconciledPortfolioCount} portfolio account${facts.reconciledPortfolioCount === 1 ? " has" : "s have"} zero reconciliation discrepancies.`,
        "The planned portfolio result is unavailable",
        "No reconciled portfolio account exists. Complete the controlled Portfolio import and reconciliation workflow, refresh evidence, then open Home.",
      )
    case "admitted_model_forecast":
      return resultEvidence(
        facts.forecastsReady,
        facts.forecastCount > 0,
        "An admitted-model forecast is available",
        `${facts.forecastCount} forecast${facts.forecastCount === 1 ? " is" : "s are"} available.`,
        "The planned forecast result is unavailable",
        "No forecast is available. Verify the model runtime, create a forecast in Models, then refresh this check.",
      )
    case "stopped_paper_and_risk_review":
      return resultEvidence(
        true,
        facts.paperReady,
        "A stopped paper and central-risk review is available",
        "Paper trading is stopped and the required risk controls are available.",
        "The planned paper and risk result is unavailable",
        "Paper trading or its risk controls need attention. Restore the stopped safe state, then refresh this check.",
      )
    case "verified_mcp_safe_read":
      return resultEvidence(
        true,
        facts.mcpReady,
        "A verified MCP safe read is available",
        "At least one AI client is connected and has completed a safe read.",
        "The planned MCP safe-read result is unavailable",
        "Neither Claude Code nor Codex has completed a verified safe read. Connect and verify one client, then refresh this check.",
      )
    default:
      return unavailableEvidence(
        "The planned first-result choice is unsupported",
        "Review your first-result choice and save the setup plan again.",
      )
  }
}

function resultEvidence(
  ownerReadReady: boolean,
  complete: boolean,
  readyHeadline: string,
  readyDetail: string,
  missingHeadline: string,
  missingDetail: string,
): StepEvidence {
  if (!ownerReadReady) {
    return unavailableEvidence(
      missingHeadline,
      `The required information is unavailable. ${missingDetail}`,
    )
  }
  return complete
    ? {
        tone: "ready",
        complete: true,
        headline: readyHeadline,
        detail: readyDetail,
      }
    : {
        tone: "unfinished",
        complete: false,
        headline: missingHeadline,
        detail: missingDetail,
      }
}

export function mcpClientIsVerified(
  client: McpClientsStatus["clients"][number] | undefined,
) {
  return client?.state === "owned" && client.verification !== null
}

const providerOutcomes = {
  coinbase_public_market_snapshot: {
    label: "Coinbase public market snapshot",
    surfaces: ["coinbase.public-market-data"],
  },
  kraken_public_market_snapshot: {
    label: "Kraken public market snapshot",
    surfaces: ["kraken.spot-public-market-data"],
  },
  sec_filing_research: {
    label: "SEC filing research",
    surfaces: ["sec.edgar-public"],
  },
  bls_macro_research: {
    label: "BLS macro research",
    surfaces: ["bls.v1-unregistered", "bls.v2-registered"],
  },
  treasury_rates_research: {
    label: "U.S. Treasury rates research",
    surfaces: ["treasury.daily-rates-xml", "treasury.fiscal-data"],
  },
  fred_alfred_authorized_research: {
    label: "authorized FRED/ALFRED research",
    surfaces: ["fred-alfred.api-v1-v2"],
  },
} as const

export function providerEvidence(
  step: PlanStep | null,
  sources: SourceEvidence[],
  readFailures: number,
  totalReads: number,
): StepEvidence {
  if (!step || step.choice.kind !== "providers" || !Array.isArray(step.choice.outcomes)) {
    return unavailableEvidence(
      "Connection choices are unavailable",
      "Review and save your connection choices, then check this item again.",
    )
  }
  if (step.disposition === "available_to_finish_later") {
    return {
      tone: "unfinished",
      complete: false,
      headline: "Connections were skipped in this setup run",
      detail:
        "You can return to Connections & Sources later. This item remains unfinished until every selected connection is ready.",
    }
  }

  const outcomes = step.choice.outcomes.filter(
    (outcome): outcome is keyof typeof providerOutcomes =>
      typeof outcome === "string" && outcome in providerOutcomes,
  )
  if (outcomes.length !== step.choice.outcomes.length || outcomes.length === 0) {
    return unavailableEvidence(
      "Connection choices cannot be checked",
      "Review and save your connection choices, then try again.",
    )
  }

  const activeSurfaceIds = new Set(
    sources
      .filter((source) => source.runtimeState === "active")
      .map((source) => source.id),
  )
  const missing = outcomes.filter((outcome) =>
    providerOutcomes[outcome].surfaces.every(
      (surface) => !activeSurfaceIds.has(surface),
    ),
  )
  const completed = outcomes.length - missing.length
  if (missing.length === 0 && readFailures === 0) {
    return {
      tone: "ready",
      complete: true,
      headline: `All ${outcomes.length} selected connections are ready`,
      detail:
        "Coverage, freshness, and data quality are available in Connections & Sources.",
    }
  }

  const missingLabels = missing.map((outcome) => providerOutcomes[outcome].label)
  return {
    tone:
      readFailures >= totalReads && totalReads > 0 ? "unavailable" : "degraded",
    complete: false,
    headline: `${completed} of ${outcomes.length} selected connections are ready`,
    detail: `${
      missingLabels.length > 0
        ? `Still missing: ${missingLabels.join(", ")}. `
        : ""
    }${
      readFailures > 0
        ? `${readFailures} connection check${readFailures === 1 ? "" : "s"} could not be completed. `
        : ""
    }Complete the remaining connection setup, then check again.`,
  }
}

export function unavailableEvidence(headline: string, detail: string): StepEvidence {
  return { tone: "unavailable", complete: false, headline, detail }
}
