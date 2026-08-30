import type { ApplicationResult } from "@/lib/schemas"
import {
  parsePortfolioResult,
  portfolioAccountSchema,
  riskSchema,
  type PortfolioAccount,
  type PortfolioResult,
  type PortfolioRisk,
} from "@/features/portfolio/portfolio-contracts"
import { z } from "zod"

export type PortfolioAccountRiskSummary = PortfolioAccount
export type PortfolioRiskReport = PortfolioRisk

export interface RiskResult<T> {
  value: T
  completeness: "complete" | "partial"
  returnedItems: number
  availableItems: number
}

export function parseRiskAccounts(
  result: ApplicationResult,
): RiskResult<PortfolioAccountRiskSummary[]> {
  return boundary(parsePortfolioResult(result, z.array(portfolioAccountSchema), []))
}

export function parseRiskReport(
  result: ApplicationResult,
): RiskResult<PortfolioRiskReport> {
  return boundary(parsePortfolioResult(result, riskSchema))
}

function boundary<T>(result: PortfolioResult<T>): RiskResult<T> {
  return {
    value: result.value,
    completeness: result.evidence.state,
    returnedItems: result.evidence.returnedItems,
    availableItems: result.evidence.availableItems,
  }
}
