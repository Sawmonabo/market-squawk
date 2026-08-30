import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import {
  holdingSchema,
  portfolioAccountSchema,
  type PortfolioAccount,
  type PortfolioHolding,
} from "../portfolio/portfolio-contracts"
import { fairValueMeasurementSchema } from "./fair-value-contracts"

const MAXIMUM_PAGE_ITEMS = 250
const exactDecimalSchema = z
  .string()
  .min(1)
  .max(96)
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)
const amountBasisSchema = z.enum([
  "per_instrument_unit",
  "reporting_entity_total",
  "position_total",
])
const methodSchema = z.enum([
  "quoted_market_price",
  "market_approach",
  "income_approach",
  "cost_approach",
])
const principalSchema = z
  .object({
    principalId: z.string().uuid(),
    displayName: z.string().min(1).max(256),
    roles: z.array(z.string().min(1).max(256)).min(1).max(32),
  })
  .strict()
const measurementResultSchema = z
  .object({
    measurement: fairValueMeasurementSchema,
    created: z.boolean(),
    classified: z.boolean(),
  })
  .strict()

export type PortfolioMeasurementAccount = PortfolioAccount
export type PortfolioMeasurementHolding = PortfolioHolding
export type PortfolioMeasurementPrincipal = z.infer<typeof principalSchema>
export type PortfolioMeasurementMethod = z.infer<typeof methodSchema>
export type PortfolioMeasurementAmountBasis = z.infer<typeof amountBasisSchema>
export type PortfolioMeasurementResult = z.infer<typeof measurementResultSchema>

export interface PortfolioMeasurementPage {
  accounts: PortfolioMeasurementAccount[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface PortfolioHoldingSnapshot {
  holdings: PortfolioMeasurementHolding[]
  completeness: string
  returnedItems: number
  availableItems: number
}

export interface PortfolioMeasurementPrincipalPage {
  principals: PortfolioMeasurementPrincipal[]
  nextAfter: string | null
}

export function parsePortfolioMeasurementAccounts(
  result: ApplicationResult,
): PortfolioMeasurementPage {
  const parsed = z.array(portfolioAccountSchema).max(MAXIMUM_PAGE_ITEMS).safeParse(result.data)
  if (!parsed.success) throw incompatible("portfolio accounts")
  verifyPageMetadata(result, parsed.data.length)
  return {
    accounts: parsed.data,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parsePortfolioMeasurementHoldings(
  result: ApplicationResult,
  expectedAccount: PortfolioMeasurementAccount,
): PortfolioHoldingSnapshot {
  const parsed = z.array(holdingSchema).max(MAXIMUM_PAGE_ITEMS).safeParse(result.data)
  if (!parsed.success) throw incompatible("portfolio holdings")
  if (
    parsed.data.some(
      (holding) =>
        holding.accountId !== expectedAccount.accountId ||
        holding.snapshotToken !== expectedAccount.currentSnapshot.snapshotToken,
    )
  ) {
    throw changedPortfolio()
  }
  verifyPageMetadata(result, parsed.data.length)
  return {
    holdings: parsed.data,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}

export function parsePortfolioMeasurementPrincipals(
  result: ApplicationResult,
): PortfolioMeasurementPrincipalPage {
  const parsed = z
    .object({
      principals: z.array(principalSchema).max(MAXIMUM_PAGE_ITEMS),
      nextAfter: principalSchema.shape.principalId.nullable(),
    })
    .strict()
    .safeParse(result.data)
  if (!parsed.success) throw incompatible("governance principals")
  verifyPageMetadata(result, parsed.data.principals.length)
  return parsed.data
}

export function parsePortfolioMeasurementResult(
  result: ApplicationResult,
  expected: {
    accountId: string
    instrumentId: string
    amount: string
    currency: string
    scale: number
    amountBasis: PortfolioMeasurementAmountBasis
    method: PortfolioMeasurementMethod
    preparedBy: string
    at: string
    portfolioAmount: string
    portfolioCurrency: string
    significance: "significant" | "not_significant"
  },
): PortfolioMeasurementResult {
  const parsed = measurementResultSchema.safeParse(result.data)
  if (!parsed.success) throw incompatible("measurement result")
  const { measurement } = parsed.data
  const input = measurement.inputs[0]
  if (
    measurement.accountId !== expected.accountId ||
    measurement.instrumentId !== expected.instrumentId ||
    measurement.amount.amount !== expected.amount ||
    measurement.amount.currency !== expected.currency ||
    measurement.amount.scale !== expected.scale ||
    measurement.amount.amountBasis !== expected.amountBasis ||
    measurement.method !== expected.method ||
    measurement.preparedBy !== expected.preparedBy ||
    Date.parse(measurement.measurementAt) !== Date.parse(expected.at) ||
    Date.parse(measurement.preparedAt) !== Date.parse(expected.at) ||
    measurement.classification === null ||
    measurement.inputCount !== 1 ||
    input?.referenceInstrumentId !== expected.instrumentId ||
    input.amount.amount !== expected.portfolioAmount ||
    input.amount.currency !== expected.portfolioCurrency ||
    input.amount.amountBasis !== "position_total" ||
    input.significance !== expected.significance ||
    input.evidence.kind !== "portfolio" ||
    input.evidence.verification !== "verified"
  ) {
    throw changedPortfolio()
  }
  verifySingleResult(result)
  return parsed.data
}

export function exactAmountError(amount: string, scale: number): string | null {
  if (!Number.isInteger(scale) || scale < 0 || scale > 28) {
    return "Scale must be a whole number from 0 through 28."
  }
  if (!exactDecimalSchema.safeParse(amount).success) {
    return "Enter an exact decimal amount without commas, symbols, or scientific notation."
  }
  const unsigned = amount.startsWith("-") ? amount.slice(1) : amount
  const [whole = "", fraction = ""] = unsigned.split(".")
  if (whole.length + fraction.length > 29) {
    return "The amount may contain at most 29 digits."
  }
  if (fraction.length > scale) {
    return `Scale ${scale} cannot exactly represent ${fraction.length} decimal places.`
  }
  return null
}

export function samePortfolioAccount(
  left: PortfolioMeasurementAccount,
  right: PortfolioMeasurementAccount,
) {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function samePortfolioHolding(
  left: PortfolioMeasurementHolding,
  right: PortfolioMeasurementHolding,
) {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function changedPortfolio() {
  return new Error(
    "The portfolio changed while this measurement was being prepared. Refresh the evidence and review the current holding before trying again.",
  )
}

function verifyPageMetadata(result: ApplicationResult, length: number) {
  if (
    result.metadata.returnedItems !== length ||
    result.metadata.availableItems < length ||
    (result.metadata.completeness === "complete" && result.metadata.availableItems !== length)
  ) {
    throw incompatible("result bounds")
  }
}

function verifySingleResult(result: ApplicationResult) {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw incompatible("single result bounds")
  }
}

function incompatible(subject: string) {
  return new Error(`The installed service returned unsupported ${subject}.`)
}
