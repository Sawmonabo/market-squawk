import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import {
  parsePortfolioCandidateImpact, parsePortfolioRebalanceReport, parsePortfolioResult,
  parsePortfolioScenarioReport, portfolioAccountSummarySchema, portfolioCandidateImpactInputSchema,
  portfolioRebalanceInputSchema, portfolioScenarioInputSchema,
} from "./portfolio-contracts"
import type { PortfolioCandidateImpact, PortfolioRebalanceReport, PortfolioScenarioReport } from "./portfolio-contracts"

const unixNanosSchema = z.string().regex(/^-?\d+$/)
export const savedPlanningSummarySchema = z.strictObject({
  savedResultToken: z.string().uuid(),
  calculationToken: z.string().uuid(),
  accountToken: portfolioAccountSummarySchema.shape.accountToken,
  kind: z.enum(["scenario", "scenario_batch", "rebalance", "position_comparison"]),
  snapshotToken: z.string().uuid(),
  portfolioEffectiveAtUnixNanos: unixNanosSchema,
  portfolioAvailableAtUnixNanos: unixNanosSchema.nullable(),
  calculatedAtUnixNanos: unixNanosSchema,
  savedAtUnixNanos: unixNanosSchema,
})
export type SavedPlanningSummary = z.infer<typeof savedPlanningSummarySchema>
export type PlanningCalculationIdentity = Pick<SavedPlanningSummary,
  "calculationToken" | "calculatedAtUnixNanos" | "snapshotToken">

const savedPlanningPageSchema = z.strictObject({
  results: z.array(savedPlanningSummarySchema),
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
})
const savedPlanningDetailSchema = z.strictObject({
  summary: savedPlanningSummarySchema,
  request: z.strictObject({ operation: z.string().min(1), arguments: z.record(z.string(), z.unknown()) }),
  result: z.unknown(),
})
export type SavedPlanningCalculation =
  | { kind: "scenario" | "scenario_batch"; report: PortfolioScenarioReport }
  | { kind: "rebalance"; report: PortfolioRebalanceReport }
  | { kind: "position_comparison"; report: PortfolioCandidateImpact }

export function parseSavedPlanningPage(result: ApplicationResult, accountToken: string) {
  const parsed = savedPlanningPageSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Saved planning results could not be displayed safely.")
  const page = parsed.data
  const { completeness, returnedItems, availableItems } = result.metadata
  const validCompletePage = completeness === "complete"
    && returnedItems === availableItems && page.nextCursor === null
  const validTruncatedPage = completeness === "truncated"
    && returnedItems > 0 && returnedItems < availableItems
    && page.nextCursor !== null && page.nextCursor !== page.pageCursor
  if ((!validCompletePage && !validTruncatedPage)
    || returnedItems !== page.results.length
    || page.results.some((summary) => summary.accountToken !== accountToken)
    || new Set(page.results.map((summary) => summary.savedResultToken)).size !== page.results.length) {
    throw new Error("Saved planning results do not match the selected portfolio.")
  }
  return page
}

export function parsePlanningSave(result: ApplicationResult, accountToken: string,
  calculation: PlanningCalculationIdentity, kind: SavedPlanningSummary["kind"]) {
  const { summary } = parsePortfolioResult(result, z.strictObject({ summary: savedPlanningSummarySchema }))
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1
    || summary.accountToken !== accountToken || summary.kind !== kind
    || summary.calculationToken !== calculation.calculationToken
    || summary.calculatedAtUnixNanos !== calculation.calculatedAtUnixNanos
    || summary.snapshotToken !== calculation.snapshotToken) {
    throw new Error("The saved result could not be confirmed for this calculation.")
  }
  return summary
}

export function parseSavedPlanningDetail(result: ApplicationResult, selected: SavedPlanningSummary) {
  const detail = parsePortfolioResult(result, savedPlanningDetailSchema)
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1
    || Object.keys(savedPlanningSummarySchema.shape).some((key) => {
      const field = key as keyof SavedPlanningSummary
      return detail.summary[field] !== selected[field]
    })) {
    throw new Error("The saved planning result does not match your selection.")
  }
  const { summary, request } = detail
  const selection = { snapshotToken: summary.snapshotToken,
    effectiveAtUnixNanos: summary.portfolioEffectiveAtUnixNanos,
    availableAtUnixNanos: summary.portfolioAvailableAtUnixNanos }
  const originalResult = { ...result, data: detail.result }
  const originalAccount = portfolioAccountSummarySchema.shape.accountToken.parse(request.arguments.accountToken)
  if (originalAccount !== summary.accountToken) throw new Error("The saved calculation belongs to a different portfolio.")
  let calculation: SavedPlanningCalculation
  switch (summary.kind) {
    case "scenario":
    case "scenario_batch": {
      const batch = summary.kind === "scenario_batch"
      if (request.operation !== (batch ? "Portfolio.EvaluateScenarioBatch" : "Portfolio.EvaluateScenario")
        || request.arguments.snapshotToken !== summary.snapshotToken) {
        throw new Error("The saved stress assumptions do not match the original calculation.")
      }
      const scenarios = batch ? z.array(portfolioScenarioInputSchema).min(1).parse(request.arguments.scenarios)
        : [portfolioScenarioInputSchema.parse(request.arguments.scenario)]
      calculation = { kind: summary.kind, report: parsePortfolioScenarioReport(originalResult, selection, scenarios, batch) }
      break
    }
    case "rebalance": {
      if (request.operation !== "Portfolio.ProposeRebalance" || request.arguments.snapshotToken !== summary.snapshotToken) {
        throw new Error("The saved rebalance assumptions do not match the original calculation.")
      }
      const proposal = portfolioRebalanceInputSchema.parse(request.arguments.proposal)
      calculation = { kind: "rebalance", report: parsePortfolioRebalanceReport(originalResult, selection, proposal) }
      break
    }
    case "position_comparison": {
      if (request.operation !== "Portfolio.EvaluateCandidateImpact") {
        throw new Error("The saved position assumptions do not match the original calculation.")
      }
      const input = portfolioCandidateImpactInputSchema.parse({ instrumentId: request.arguments.instrumentId,
        proposedQuantity: request.arguments.proposedQuantity, scenarioShockPercent: request.arguments.scenarioShockPercent })
      const report = parsePortfolioCandidateImpact(originalResult, summary.accountToken, input)
      if (report.snapshotToken !== summary.snapshotToken
        || report.portfolioEffectiveAtUnixNanos !== summary.portfolioEffectiveAtUnixNanos
        || report.portfolioAvailableAtUnixNanos !== summary.portfolioAvailableAtUnixNanos) {
        throw new Error("The saved position comparison does not match its original portfolio observation.")
      }
      calculation = { kind: "position_comparison", report }
      break
    }
  }
  if (calculation.report.calculationToken !== summary.calculationToken
    || calculation.report.calculatedAtUnixNanos !== summary.calculatedAtUnixNanos) {
    throw new Error("The saved calculation identity is inconsistent.")
  }
  return { summary, calculation }
}
