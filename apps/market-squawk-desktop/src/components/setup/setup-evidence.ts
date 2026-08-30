import * as React from "react"
import { z } from "zod"

import { parseBackupInventory } from "@/features/backup/contracts"
import {
  parseForecasts,
  parseModelBundles,
} from "@/features/models/models-contracts"
import { marketOverviewRows } from "@/features/markets/market-product"
import { decisionOverviewSchema } from "@/features/overview/schemas"
import { parsePaperStatus } from "@/features/paper/contracts"
import {
  parsePortfolioResult,
  portfolioAccountSchema,
} from "@/features/portfolio/portfolio-contracts"
import { parseResearchDatasetPage } from "@/features/research/research-contracts"
import { parseSettingsSnapshot } from "@/features/settings/contracts"
import {
  parseSourceStatusResult,
  sourceEvidence,
} from "@/features/sources/source-evidence"
import type {
  ApplicationResult,
  DesktopBootstrap,
} from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  firstResultEvidence,
  initialEvidence,
  mcpClientEvidence,
  mcpClientIsVerified,
  paperRiskEvidence,
  providerEvidence,
  readinessEvidence,
  storageEvidence,
  unavailableEvidence,
  type EvidenceMap,
  type OwnerRead,
} from "./setup-evidence-model"
import { plainToken } from "./setup-copy"
import type { PlanStep } from "./setup-plan"

export function useOwnerEvidence(
  bootstrap: DesktopBootstrap,
  transport: ProductTransport,
  enabled: boolean,
  planSteps: PlanStep[] | null,
) {
  const [map, setMap] = React.useState<EvidenceMap>(() => initialEvidence(bootstrap))
  const [refreshing, setRefreshing] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const requestId = React.useRef(0)

  const refresh = React.useCallback(async () => {
    const activeRequest = ++requestId.current
    setRefreshing(true)
    setError(null)
    setMap(initialEvidence(bootstrap, true))

    const sourceStatusReadsPromise = Promise.all(
      bootstrap.providerProfiles.map((profile) =>
        settle(
          transport
            .query({ query: "sourceStatus", sourceIds: [profile.id] })
            .then((result) => parseSourceStatusResult(result, [profile.id])),
        ),
      ),
    )
    const [
      sourceStatusReads,
      coverageRead,
      healthRead,
      researchRead,
      portfolioRead,
      bundlesRead,
      forecastsRead,
      paperRead,
      mcpRead,
      backupsRead,
      overviewRead,
      marketsRead,
      settingsRead,
    ] = await Promise.all([
      sourceStatusReadsPromise,
      settle(transport.query({ query: "sourceCoverage" })),
      settle(transport.query({ query: "sourceHealth" })),
      settle(
        transport
          .query({ query: "researchDatasets" })
          .then(parseResearchDatasetPage),
      ),
      settle(
        transport
          .query({ query: "portfolioAccounts" })
          .then((result) => parsePortfolioResult(result, z.array(portfolioAccountSchema))),
      ),
      settle(transport.query({ query: "modelBundles" }).then(parseModelBundles)),
      settle(transport.query({ query: "forecasts" }).then(parseForecasts)),
      settle(transport.query({ query: "paperStatus" }).then(parsePaperStatus)),
      settle(transport.mcpClients()),
      settle(
        transport
          .query({ query: "operationBackups", limit: 64 })
          .then(parseBackupInventory),
      ),
      settle(
        transport
          .query({ query: "overview" })
          .then((result) => decisionOverviewSchema.parse(result.data)),
      ),
      settle(
        transport
          .query({ query: "marketOverview" })
          .then(marketOverviewRows),
      ),
      settle(
        transport
          .query({ query: "operationSettings" })
          .then(parseSettingsSnapshot),
      ),
    ])

    if (activeRequest !== requestId.current) return

    const successfulStatuses = sourceStatusReads.flatMap((read) =>
      read.ok ? read.value : [],
    )
    const sources = sourceEvidence(
      bootstrap.providerProfiles,
      bootstrap.providerSessions,
      successfulStatuses,
      readValue(coverageRead),
      readValue(healthRead),
    )
    const sourceReadFailures =
      sourceStatusReads.filter((read) => !read.ok).length +
      Number(!coverageRead.ok) +
      Number(!healthRead.ok)

    const researchCount = researchRead.ok ? researchRead.value.items.length : 0
    const portfolioCount = portfolioRead.ok ? portfolioRead.value.value.length : 0
    const bundleCount = bundlesRead.ok ? bundlesRead.value.bundles.length : 0
    const forecastCount = forecastsRead.ok ? forecastsRead.value.forecasts.length : 0
    const marketResults = marketsRead.ok
      ? marketsRead.value.filter((row) => row.currentPrice !== null).length
      : 0
    const claude = mcpRead.ok
      ? mcpRead.value.clients.find((client) => client.client === "claude_code")
      : undefined
    const codex = mcpRead.ok
      ? mcpRead.value.clients.find((client) => client.client === "codex")
      : undefined
    const backupCount = backupsRead.ok ? backupsRead.value.manifests.length : 0
    const paperOwnerEvidence = paperRead.ok
      ? paperRiskEvidence(paperRead.value.value, bootstrap)
      : unavailableEvidence(
          "Paper trading and risk status are unavailable",
          "Try again or review Logs & Diagnostics for details.",
        )
    const providerStep =
      planSteps?.find((step) => step.id === "public_and_zero_fee_providers") ?? null
    const firstResultStep =
      planSteps?.find((step) => step.id === "first_useful_result") ?? null
    const storageStep =
      planSteps?.find(
        (step) => step.id === "storage_retention_time_and_disk",
      ) ?? null

    const next: EvidenceMap = {
      goals_and_starter_plan: {
        tone: "recorded",
        complete: true,
        headline: "Your goals and starter plan are saved",
        detail:
          "The remaining setup items are checked separately so unfinished work stays visible.",
      },
      storage_retention_time_and_disk: storageEvidence(
        storageStep,
        bootstrap,
        settingsRead,
      ),
      public_and_zero_fee_providers: providerEvidence(
        providerStep,
        sources,
        sourceReadFailures,
        bootstrap.providerProfiles.length + 2,
      ),
      file_and_portfolio_import:
        researchRead.ok && portfolioRead.ok
          ? readinessEvidence(
              researchCount + portfolioCount > 0,
              `${researchCount} research dataset${researchCount === 1 ? "" : "s"} and ${portfolioCount} portfolio account${portfolioCount === 1 ? "" : "s"} are recorded`,
              "Your imported research and portfolio information is available.",
              "No owned-file dataset or portfolio account is recorded",
              "Import a research file or add a portfolio account to continue.",
            )
          : unavailableEvidence(
              "Import evidence is unavailable",
              "Try again or review Logs & Diagnostics for details.",
            ),
      model_runtime: readinessEvidence(
        bootstrap.modelRuntime.state === "ready",
        "Local model runtime is verified",
        `${bootstrap.modelRuntime.detail} ${bundleCount} admitted bundle${bundleCount === 1 ? " is" : "s are"} currently listed.`,
        "Local model runtime is not verified",
        bootstrap.modelRuntime.detail,
      ),
      paper_and_risk: paperOwnerEvidence,
      claude_code: mcpClientEvidence("Claude Code", claude, mcpRead),
      codex: mcpClientEvidence("Codex", codex, mcpRead),
      backup: backupsRead.ok
        ? backupCount > 0
          ? {
              tone: "ready",
              complete: true,
              headline: `${backupCount} verified backup${backupCount === 1 ? " is" : "s are"} available`,
              detail:
                "Your verified backup inventory is ready for recovery use.",
            }
          : {
              tone: "unfinished",
              complete: false,
              headline: "No backup is inventoried",
              detail: "Create and verify a backup through Backup & Recovery, then refresh this check.",
            }
        : unavailableEvidence(
            "Backup information is unavailable",
            "Try again or review Logs & Diagnostics for details.",
          ),
      review: {
        tone: "recorded",
        complete: false,
        headline: "Checking your setup",
        detail: "This status updates when the checks finish.",
      },
      first_useful_result: firstResultEvidence(firstResultStep, {
        overviewReady: overviewRead.ok,
        marketsReady: marketsRead.ok,
        marketResults,
        researchReady: researchRead.ok,
        researchCount,
        portfolioReady: portfolioRead.ok,
        reconciledPortfolioCount: portfolioRead.ok
          ? portfolioRead.value.value.filter(
              (account) => account.reconciliationDiscrepancies === 0,
            ).length
          : 0,
        forecastsReady: forecastsRead.ok,
        forecastCount,
        paperReady: paperOwnerEvidence.complete,
        mcpReady:
          mcpClientIsVerified(claude) || mcpClientIsVerified(codex),
      }),
    }

    for (const step of planSteps ?? []) {
      if (
        step.disposition === "available_to_finish_later" &&
        step.id !== "review"
      ) {
        next[step.id] = {
          tone: "unfinished",
          complete: false,
          headline: `${plainToken(step.id)} was skipped in this setup run`,
          detail:
            "You can return to this item later. It remains unfinished until its setup checks pass.",
        }
      }
    }

    const reviewGapCount = Object.entries(next).filter(
      ([step, item]) => step !== "review" && !item.complete,
    ).length
    next.review = {
      tone: "ready",
      complete: true,
      headline: `Capability review completed with ${reviewGapCount} visible gap${reviewGapCount === 1 ? "" : "s"}`,
      detail:
        reviewGapCount === 0
          ? "Every setup check is complete."
          : "Items that still need attention remain visible in this checklist.",
    }

    const errors = collectErrors([
      ...sourceStatusReads,
      coverageRead,
      healthRead,
      researchRead,
      portfolioRead,
      bundlesRead,
      forecastsRead,
      paperRead,
      mcpRead,
      backupsRead,
      overviewRead,
      marketsRead,
      settingsRead,
    ])
    setMap(next)
    setError(
      errors.length
        ? "Some setup information could not be checked. Try again or review Logs & Diagnostics."
        : null,
    )
    setRefreshing(false)
  }, [bootstrap, planSteps, transport])

  React.useEffect(() => {
    if (enabled) void refresh()
  }, [enabled, refresh])

  return { map, refreshing, error, refresh }
}

async function settle<T>(promise: Promise<T>): Promise<OwnerRead<T>> {
  try {
    return { ok: true, value: await promise }
  } catch {
    return { ok: false, error: "This setup information is unavailable right now." }
  }
}

function readValue(read: OwnerRead<ApplicationResult>): ApplicationResult | undefined {
  return read.ok ? read.value : undefined
}

function collectErrors(reads: OwnerRead<unknown>[]) {
  return reads.flatMap((read) => (read.ok ? [] : [read.error]))
}
