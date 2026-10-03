import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import type { RecommendationSetupRequest } from "@/lib/transport"
import { moneySchema, parsePortfolioResult } from "./portfolio-contracts"

const digest = z.string().regex(/^[0-9a-f]{64}$/)
const revision = z.number().int().nonnegative().safe()
const time = z.string().regex(/^-?(?:0|[1-9]\d*)$/).refine((value) =>
  BigInt(value) >= -(1n << 63n) && BigInt(value) < (1n << 63n),
)
const currency = z.string().regex(/^[A-Z]{3}$/)
const weight = z.number().int().min(1).max(10_000)
const duration = z.string().regex(/^[1-9]\d*$/).refine((value) =>
  BigInt(value) <= 3_650n * 86_400_000_000_000n,
)
const allocation = z.object({
  preferredPositionWeightLowerBps: weight,
  preferredPositionWeightUpperBps: weight,
  minimumCashReserve: moneySchema.refine((value) => !value.amount.startsWith("-")),
  maximumDownsideLossBpsOfMarkedEquity: weight,
  availableInvestmentHorizonNanos: duration,
}).strict()
const account = z.object({
  accountId: z.string().uuid(),
  portfolioRevisionSha256: digest,
  reportingCurrency: currency,
}).strict()

const statusSchema = z.object({
  workspaceId: z.string().uuid(),
  state: z.enum(["ready", "setup_required"]),
  setupRequiredReason: z.enum([
    "no_default_account", "ambiguous_accounts", "portfolio_evidence_unavailable", "profile_review_required",
  ]).nullable(),
  authority: z.object({
    revision,
    digest,
    transitionAtUnixNanos: time.nullable(),
    configurationDigest: digest.nullable(),
  }).strict(),
  accountSelection: z.object({
    setupRevision: revision.refine((value) => value > 0),
    accountId: z.string().uuid(),
    reportingCurrency: currency,
    confirmedPortfolioRevisionSha256: digest,
    confirmedCatalogDigestSha256: digest,
    confirmedAtUnixNanos: time,
    digest,
  }).strict().nullable(),
  allocationProfile: allocation.extend({
    setupRevision: revision.refine((value) => value > 0),
    accountId: z.string().uuid(),
    reportingCurrency: currency,
    acceptedAtUnixNanos: time,
    reviewDueAtUnixNanos: time,
    digest,
  }).strict().nullable(),
  portfolioCatalog: z.object({
    digest,
    accountCount: z.number().int().min(0).max(256),
    accounts: z.array(account.extend({
      displayName: z.string().trim().min(1).max(160),
      effectiveAtUnixNanos: time,
      availableAtUnixNanos: time.nullable(),
      sourceId: z.string().min(1).max(256),
      sourceCoverage: z.array(z.string().min(1).max(256)).max(4096),
      artifactSha256: digest,
    }).strict()).max(256),
  }).strict(),
}).strict()

const previewSchema = z.object({
  workspaceId: z.string().uuid(),
  previewId: z.string().uuid(),
  previewDigest: digest,
  currentRevision: revision,
  resultingRevision: revision.refine((value) => value > 0),
  currentAuthorityDigest: digest,
  catalogDigest: digest,
  kind: z.literal("configure"),
  accountSelection: account,
  allocationProfile: allocation,
  issuedAtUnixNanos: time,
  expiresAtUnixNanos: time,
}).strict()

const receiptSchema = z.object({
  workspaceId: z.string().uuid(),
  revision: revision.refine((value) => value > 0),
  authorityDigest: digest,
  configured: z.literal(true),
  acceptedAtUnixNanos: time,
}).strict()

export type RecommendationSetupStatus = z.infer<typeof statusSchema>
export type RecommendationSetupPreview = z.infer<typeof previewSchema>
export type RecommendationAllocation = z.infer<typeof allocation>

export function parseRecommendationSetup(result: ApplicationResult): RecommendationSetupStatus {
  const status = parsePortfolioResult(result, statusSchema)
  const accounts = status.portfolioCatalog.accounts
  if (status.portfolioCatalog.accountCount !== accounts.length
    || new Set(accounts.map((value) => value.accountId)).size !== accounts.length
    || (status.state === "ready" && (!status.accountSelection || !status.allocationProfile || status.setupRequiredReason !== null))) {
    throw new Error("Recommendation preferences could not be displayed safely.")
  }
  return status
}

export function parseRecommendationPreview(
  result: ApplicationResult,
  status: RecommendationSetupStatus,
  request: Extract<RecommendationSetupRequest, { action: "preview" }>,
): RecommendationSetupPreview {
  const preview = parsePortfolioResult(result, previewSchema)
  const selected = status.portfolioCatalog.accounts.find((value) => value.accountId === request.accountId)
  const profile = preview.allocationProfile
  const input = request.allocationProfile
  if (!selected || preview.workspaceId !== status.workspaceId
    || preview.currentRevision !== request.expectedRevision
    || preview.resultingRevision !== preview.currentRevision + 1
    || preview.currentAuthorityDigest !== status.authority.digest
    || preview.catalogDigest !== status.portfolioCatalog.digest
    || preview.accountSelection.accountId !== selected.accountId
    || preview.accountSelection.reportingCurrency !== selected.reportingCurrency
    || preview.accountSelection.portfolioRevisionSha256 !== selected.portfolioRevisionSha256
    || profile.minimumCashReserve.currency !== selected.reportingCurrency
    || normalizeAmount(profile.minimumCashReserve.amount) !== normalizeAmount(input.minimumCashReserve.amount)
    || profile.preferredPositionWeightLowerBps !== input.preferredPositionWeightLowerBps
    || profile.preferredPositionWeightUpperBps !== input.preferredPositionWeightUpperBps
    || profile.maximumDownsideLossBpsOfMarkedEquity !== input.maximumDownsideLossBpsOfMarkedEquity
    || BigInt(profile.availableInvestmentHorizonNanos) !== BigInt(input.availableInvestmentHorizonDays) * 86_400_000_000_000n
    || BigInt(preview.expiresAtUnixNanos) <= BigInt(preview.issuedAtUnixNanos)) {
    throw new Error("The returned preview does not match your selected account and preferences. Refresh and review again.")
  }
  return preview
}

export function parseRecommendationCommit(result: ApplicationResult, preview: RecommendationSetupPreview) {
  const receipt = parsePortfolioResult(result, receiptSchema)
  if (receipt.workspaceId !== preview.workspaceId || receipt.revision !== preview.resultingRevision) {
    throw new Error("The saved preferences could not be confirmed. Refresh to check their current state.")
  }
  return receipt
}

/** Percent inputs become small exact integers; money remains decimal text throughout. */
export function parsePreferencePercent(value: string): number {
  if (!/^(?:0|[1-9]\d{0,2})(?:\.\d{1,2})?$/.test(value)) {
    throw new Error("Enter percentages from 0.01 to 100 with at most two decimal places.")
  }
  const [whole = "", fraction = ""] = value.split(".")
  const exact = Number(whole) * 100 + Number(fraction.padEnd(2, "0"))
  if (exact < 1 || exact > 10_000) throw new Error("Enter percentages from 0.01 to 100.")
  return exact
}

export function normalizeAmount(value: string): string {
  if (value.length > 128 || !/^\d+(?:\.\d+)?$/.test(value)) {
    throw new Error("Enter a nonnegative cash amount using digits and a decimal point.")
  }
  const [whole = "", fraction = ""] = value.split(".")
  const integer = whole.replace(/^0+(?=\d)/, "")
  const decimals = fraction.replace(/0+$/, "")
  return decimals ? `${integer}.${decimals}` : integer
}

export function formatPreferencePercent(value: number): string {
  return `${Math.floor(value / 100)}${value % 100 ? `.${String(value % 100).padStart(2, "0").replace(/0$/, "")}` : ""}%`
}

export function preferenceTime(value: string): string {
  return new Date(Number(BigInt(value) / 1_000_000n)).toLocaleString()
}
