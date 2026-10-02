import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

import { marketSelectionTokenSchema } from "./market-product"

const decimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const timestampSchema = losslessIntegerSchema.refine((value) => {
  const nanos = BigInt(value)
  return nanos >= -9_223_372_036_854_775_808n && nanos <= 9_223_372_036_854_775_807n
})
const calendarDateSchema = z.object({
  year: z.number().int().min(1).max(65_535),
  month: z.number().int().min(1).max(12),
  day: z.number().int().min(1).max(31),
}).strict().refine(({ year, month, day }) => {
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
  return day <= days[month - 1]!
}, { message: "The reporting date is invalid." })
const productTimeSchema = z.discriminatedUnion("precision", [
  z.object({ precision: z.literal("timestamp"), value: timestampSchema }).strict(),
  z.object({ precision: z.literal("calendar_date"), value: calendarDateSchema }).strict(),
])
const periodSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("instant"), instant: calendarDateSchema }).strict(),
  z.object({ kind: z.literal("duration"), start: calendarDateSchema, end: calendarDateSchema }).strict(),
])
const revisionSchema = z.enum(["current", "superseded", "incomparable_history"])
const fiscalContextSchema = z.object({
  fiscalYear: z.number().int().min(0).max(65_535).nullable(),
  fiscalPeriod: z.enum(["fiscal_year", "calendar_year", "first_quarter", "second_quarter", "third_quarter", "fourth_quarter", "unavailable"]),
  cadence: z.enum(["annual", "quarterly", "other", "unavailable"]),
}).strict()
const reportingContextSchema = z.object({
  dimensionality: z.enum(["unavailable", "no_dimensions"]),
  consolidation: z.enum(["reported_consolidated", "reported_non_consolidated", "unavailable"]),
  amendment: z.enum(["original", "amendment", "unavailable"]),
  restatement: z.enum(["reported_restated", "reported_not_restated", "unavailable"]),
  occurrence: z.number().int().min(1).max(4_294_967_295),
}).strict()
const envelopeFields = {
  period: periodSchema,
  fiscalContext: fiscalContextSchema,
  reportingContext: reportingContextSchema,
  filedOn: calendarDateSchema.nullable(),
  effective: productTimeSchema,
  knownAt: timestampSchema,
}
const reportingEnvelopeSchema = z.object(envelopeFields).strict()
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const factSchema = z.object({
  ...envelopeFields,
  scope: z.enum(["company_wide", "filing_detail"]),
  revision: revisionSchema,
  metric: z.enum([
    "cash_and_cash_equivalents", "accounts_receivable_net_current", "inventory_net", "current_assets", "total_assets",
    "current_liabilities", "total_liabilities", "current_long_term_debt", "noncurrent_long_term_debt", "shareholders_equity",
    "total_equity_including_noncontrolling_interests", "revenue", "net_sales", "customer_revenue_excluding_assessed_tax",
    "cost_of_revenue", "gross_profit", "operating_expenses", "operating_income", "net_income", "common_net_income",
    "preferred_dividends_and_adjustments", "profit_or_loss_including_noncontrolling_interests", "basic_earnings_per_share",
    "diluted_earnings_per_share", "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
    "property_plant_and_equipment_purchases", "long_term_borrowing_proceeds", "long_term_debt_repayments",
    "preferred_dividends_paid", "preferred_stock_issued_value", "entity_common_shares_outstanding",
    "common_stock_shares_outstanding", "weighted_average_basic_shares", "weighted_average_diluted_shares",
  ]),
  displayName: z.string().min(1),
  value: decimalSchema,
  unit: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("currency"), currency: currencySchema }).strict(),
    z.object({ kind: z.literal("shares") }).strict(),
    z.object({ kind: z.literal("currency_per_share"), currency: currencySchema }).strict(),
  ]),
}).strict()
const statementSchema = z.object({
  statement: z.enum(["financial_position", "operations", "cash_flows", "share_data"]),
  envelope: reportingEnvelopeSchema,
  items: z.array(factSchema).min(1),
}).strict()
const ratioSchema = z.object({
  metric: z.enum(["current_ratio", "gross_margin", "operating_margin", "net_margin"]),
  displayName: z.string().min(1),
  state: z.enum(["reported", "missing_input", "conflicting_input", "incompatible_units", "zero_denominator", "unavailable"]),
  value: decimalSchema.nullable(),
  unit: z.literal("ratio"),
  envelope: reportingEnvelopeSchema.nullable(),
  inputs: z.array(z.object({ role: z.enum(["numerator", "denominator"]), fact: factSchema }).strict()),
}).strict().refine((ratio) => (ratio.state === "reported") === (ratio.value !== null), {
  message: "The ratio value does not match its availability.",
})
const filingSchema = z.object({
  revision: revisionSchema,
  form: z.string().min(1).max(64),
  effective: productTimeSchema,
  published: productTimeSchema.nullable(),
  knownAt: timestampSchema,
}).strict()
const pageFields = {
  selectionToken: marketSelectionTokenSchema,
  knowledgeAt: z.iso.datetime({ offset: true }).nullable(),
  effectiveOn: z.iso.date().nullable(),
  revisionPolicy: z.literal("latestKnown"),
  state: z.enum(["reported", "missing", "conflict", "unavailable", "expired"]),
  families: z.array(z.object({
    family: z.enum(["company_facts", "filing_details", "filings"]),
    state: z.enum(["reported", "missing", "conflict", "unavailable"]),
    reason: z.enum(["identity_missing", "identity_ambiguous", "identity_stale", "identity_revoked", "revision_conflict", "no_records", "evidence_unavailable", "rights_unavailable"]).nullable(),
  }).strict()).max(3),
  currentCursor: z.string().min(1).nullable(),
  nextCursor: z.string().min(1).nullable(),
  readToken: z.string().uuid().nullable(),
  omittedItems: z.number().int().nonnegative(),
  limitations: z.array(z.enum(["some_reported_facts_not_supported", "item_exceeds_response_limit", "read_expired"])),
}
export const investmentFinancialsResultSchema = z.discriminatedUnion("section", [
  z.object({ ...pageFields, section: z.literal("facts"), items: z.array(factSchema) }).strict(),
  z.object({ ...pageFields, section: z.literal("statements"), items: z.array(statementSchema) }).strict(),
  z.object({ ...pageFields, section: z.literal("ratios"), items: z.array(ratioSchema) }).strict(),
  z.object({ ...pageFields, section: z.literal("filings"), items: z.array(filingSchema) }).strict(),
]).superRefine((page, context) => {
  if (page.state === "expired") {
    if (page.readToken !== null || page.currentCursor !== null || page.nextCursor !== null || page.items.length !== 0
      || page.knowledgeAt !== null || page.effectiveOn !== null) {
      context.addIssue({ code: "custom", message: "Expired financial information cannot contain a retained page." })
    }
  } else if (page.readToken === null || page.currentCursor === null || page.knowledgeAt === null || page.effectiveOn === null) {
    context.addIssue({ code: "custom", message: "Financial information requires its retained page identity." })
  }
  if (new Set(page.families.map((family) => family.family)).size !== page.families.length) {
    context.addIssue({ code: "custom", message: "Financial coverage contains duplicate families." })
  }
  if (page.nextCursor !== null && page.nextCursor === page.currentCursor) {
    context.addIssue({ code: "custom", message: "The financial page repeats its navigation identity." })
  }
})

export type InvestmentFinancialsResult = z.infer<typeof investmentFinancialsResultSchema>
export type InvestmentFinancialSection = InvestmentFinancialsResult["section"]
export type InvestmentFinancialFact = z.infer<typeof factSchema>
export type InvestmentFinancialStatement = z.infer<typeof statementSchema>
export type InvestmentFinancialRatio = z.infer<typeof ratioSchema>
export type InvestmentFinancialFiling = z.infer<typeof filingSchema>
export type InvestmentFinancialEnvelope = z.infer<typeof reportingEnvelopeSchema>
export type InvestmentFinancialTime = z.infer<typeof productTimeSchema>
export type InvestmentFinancialDate = z.infer<typeof calendarDateSchema>
export type InvestmentFinancialSnapshot = Pick<InvestmentFinancialsResult, "readToken" | "knowledgeAt" | "effectiveOn">

export function parseInvestmentFinancialsResult(result: ApplicationResult, expected: {
  selectionToken: string
  section: InvestmentFinancialSection
  cursor?: string
  snapshot?: InvestmentFinancialSnapshot
}): InvestmentFinancialsResult {
  const page = investmentFinancialsResultSchema.parse(result.data)
  if (page.selectionToken !== expected.selectionToken || page.section !== expected.section) {
    throw new Error("The financial information does not match this investment section.")
  }
  if (result.metadata.completeness !== "complete" || result.metadata.returnedItems !== page.items.length) {
    throw new Error("The financial page is incomplete.")
  }
  if (page.state !== "expired" && expected.cursor !== undefined && page.currentCursor !== expected.cursor) {
    throw new Error("The financial information does not match the requested page.")
  }
  if (page.state !== "expired" && expected.snapshot && (
    page.readToken !== expected.snapshot.readToken || page.knowledgeAt !== expected.snapshot.knowledgeAt
    || page.effectiveOn !== expected.snapshot.effectiveOn
  )) {
    throw new Error("The financial information does not match the open information date.")
  }
  return page
}
