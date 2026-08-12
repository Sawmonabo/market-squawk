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
        headline: "Reading owner evidence",
        detail: "No completion is assumed while the owner query is in progress.",
      }
    : {
        tone: "unavailable",
        complete: false,
        headline: "Owner evidence has not been refreshed",
        detail: "Open the accepted checklist or refresh to read the current owner state.",
      }
  return {
    goals_and_starter_plan: { ...base },
    storage_retention_time_and_disk: loading
      ? { ...base }
      : readinessEvidence(
          bootstrap.storage.state === "ready" && bootstrap.installation.state === "ready",
          "Workspace and installed release are ready",
          `${bootstrap.storage.detail} ${bootstrap.installation.detail}`,
          "Storage or release verification still needs attention",
        ),
    public_and_zero_fee_providers: { ...base },
    file_and_portfolio_import: { ...base },
    model_runtime: loading
      ? { ...base }
      : readinessEvidence(
          bootstrap.modelRuntime.state === "ready",
          "Local model runtime is verified",
          bootstrap.modelRuntime.detail,
          "Local model runtime is not verified",
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
          "Bot.GetStatus proves the stopped lifecycle state, and the installed surface exposes the closed Bot, Execution, and central Risk operations.",
      }
    }
    return {
      tone: "degraded",
      complete: false,
      headline: "Paper execution is stopped, but the installed safety surface is incomplete",
      detail: `Bot.GetStatus proves the stopped state. Missing installed operations: ${missingOperations.join(", ")}.`,
    }
  }
  if (status.state === "running") {
    return {
      tone: "unfinished",
      complete: false,
      headline: "Paper execution is running, not stopped",
      detail:
        "Stop the paper account in Paper Execution. Central risk remains authoritative while it runs, but the approved setup outcome is stopped-by-default.",
    }
  }
  if (status.state === "failed") {
    return {
      tone: "degraded",
      complete: false,
      headline: "Paper execution failed and requires a stop",
      detail: `Provider ${status.provider} reports a failed state. Open Paper Execution and stop or recover it explicitly.`,
    }
  }
  return {
    tone: "unfinished",
    complete: false,
    headline: `Paper execution is ${status.state}`,
    detail: "Wait for the lifecycle transition to settle, then refresh owner evidence.",
  }
}

export function mcpClientEvidence(
  label: string,
  client: McpClientsStatus["clients"][number] | undefined,
  read: OwnerRead<McpClientsStatus>,
): StepEvidence {
  if (!read.ok) return unavailableEvidence(`${label} evidence is unavailable`, read.error)
  if (!client) {
    return unavailableEvidence(
      `${label} has no distinct owner state`,
      "The MCP owner did not return the required separate client record.",
    )
  }
  const verified = client.state === "owned" && client.verification !== null
  return verified
    ? {
        tone: "ready",
        complete: true,
        headline: `${label} is owned and verified`,
        detail: `A real ${label} handshake, discovery, and safe read were recorded at ${formatUnixSeconds(String(client.verification!.verifiedAtUnixSeconds))}.`,
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
      detail: `${bootstrap.storage.detail} ${bootstrap.installation.detail}`,
    }
  }
  if (!settingsRead.ok) {
    return unavailableEvidence(
      "Workspace settings evidence is unavailable",
      `${settingsRead.error} The ready storage bootstrap alone does not prove the selected retention and disk budget.`,
    )
  }
  if (!step || step.choice.kind !== "storage") {
    return unavailableEvidence(
      "The planned storage policy cannot be interpreted",
      "The accepted plan did not expose its closed retention, disk, and time-policy choice.",
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
        detail: `${expectedRetention} days retention · ${expectedSoftLimit} byte soft limit · point-in-time research with first-observed-locally provenance.`,
      }
    : {
        tone: "degraded",
        complete: false,
        headline: "Workspace settings do not match the accepted storage plan",
        detail:
          "Open Settings to review the typed retention and storage values, then refresh owner evidence. No value is inferred from plan acceptance.",
      }
}

interface FirstResultFacts {
  overviewReady: boolean
  overviewError: string | null
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
      "The accepted plan did not expose its closed first-result choice. Refresh the setup authority; no substitute result is inferred.",
    )
  }
  if (!facts.overviewReady) {
    return unavailableEvidence(
      "Home owner data is unavailable",
      `${facts.overviewError ?? "The Home read failed."} No first result is inferred. Refresh owner evidence, then open Home.`,
    )
  }

  const result = step.choice.result
  switch (result) {
    case "verified_public_market_snapshot":
      return resultEvidence(
        facts.marketsReady,
        facts.marketResults > 0,
        "A verified public market result is available",
        `${facts.marketResults} observed market trade result${facts.marketResults === 1 ? " is" : "s are"} available to Home with source, time, and quality evidence.`,
        "The planned public market result is unavailable",
        "No observed market trade is available. Finish every included provider outcome in Sources, refresh evidence, then open Home.",
      )
    case "point_in_time_research_result":
      return resultEvidence(
        facts.researchReady,
        facts.researchCount > 0,
        "A point-in-time research result is available",
        `${facts.researchCount} durable research dataset${facts.researchCount === 1 ? " is" : "s are"} available with lineage evidence.`,
        "The planned research result is unavailable",
        "No durable research dataset exists. Complete a controlled Research workflow, refresh evidence, then open Home.",
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
        `${facts.forecastCount} durable forecast${facts.forecastCount === 1 ? " is" : "s are"} available from the forecast owner.`,
        "The planned forecast result is unavailable",
        "No admitted-model forecast exists. Verify the model runtime and complete a Models forecast workflow, refresh evidence, then open Home.",
      )
    case "stopped_paper_and_risk_review":
      return resultEvidence(
        true,
        facts.paperReady,
        "A stopped paper and central-risk review is available",
        "The paper lifecycle is stopped and the installed Bot, Execution, and central Risk surface is closed and available.",
        "The planned paper and risk result is unavailable",
        "Paper/risk owner evidence is not ready. Open Paper Execution and Risk, restore the stopped safe state, then refresh and open Home.",
      )
    case "verified_mcp_safe_read":
      return resultEvidence(
        true,
        facts.mcpReady,
        "A verified MCP safe-read result is available",
        "At least one separately owned AI client has a real handshake, discovery, and bounded safe-read result.",
        "The planned MCP safe-read result is unavailable",
        "Neither Claude Code nor Codex has a verified safe read. Complete one client's own MCP connect and verify workflow, then refresh and open Home.",
      )
    default:
      return unavailableEvidence(
        "The planned first-result choice is unsupported",
        `The setup authority returned ${typeof result === "string" ? result : "a non-text result"}; no substitute result is inferred.`,
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
      `The required owner read failed. ${missingDetail}`,
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
      "Provider plan outcomes are unavailable",
      "The accepted provider step did not expose its closed provider outcome list, so no completion is inferred.",
    )
  }
  if (step.disposition === "available_to_finish_later") {
    return {
      tone: "unfinished",
      complete: false,
      headline: "Provider setup was skipped in this setup run",
      detail:
        "The providers remain installed and available when setup resumes. A skipped setup outcome stays incomplete until every included provider has real owner evidence.",
    }
  }

  const outcomes = step.choice.outcomes.filter(
    (outcome): outcome is keyof typeof providerOutcomes =>
      typeof outcome === "string" && outcome in providerOutcomes,
  )
  if (outcomes.length !== step.choice.outcomes.length || outcomes.length === 0) {
    return unavailableEvidence(
      "Provider plan outcomes cannot be interpreted",
      "The accepted plan contains an unknown or empty provider outcome. Refresh the setup authority before continuing.",
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
      headline: `All ${outcomes.length} included provider outcomes are active`,
      detail:
        "Coinbase, Kraken, SEC, BLS, Treasury, and authority-gated FRED/ALFRED each have real active owner evidence. Coverage, freshness, and quality remain separately visible in Sources.",
    }
  }

  const missingLabels = missing.map((outcome) => providerOutcomes[outcome].label)
  return {
    tone:
      readFailures >= totalReads && totalReads > 0 ? "unavailable" : "degraded",
    complete: false,
    headline: `${completed} of ${outcomes.length} included provider outcomes are active`,
    detail: `${
      missingLabels.length > 0
        ? `Still missing: ${missingLabels.join(", ")}. `
        : ""
    }${
      readFailures > 0
        ? `${readFailures} owner evidence reads failed. `
        : ""
    }A profile, onboarding session, or successful status read is not an active provider result.`,
  }
}

export function unavailableEvidence(headline: string, detail: string): StepEvidence {
  return { tone: "unavailable", complete: false, headline, detail }
}
