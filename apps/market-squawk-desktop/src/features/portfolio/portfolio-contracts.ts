import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

export const moneySchema = z.object({
  amount: exactDecimalSchema,
  currency: z.string().min(1),
})

export const portfolioRevisionSchema = z.object({
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  sourceCoverage: z.array(z.string()),
  artifactSha256: digestSchema,
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  reconciliationDiscrepancies: z.number().int().nonnegative(),
})

export const portfolioAccountSchema = z.object({
  accountId: z.string().min(1),
  currency: z.string().min(1),
  currentRevision: portfolioRevisionSchema,
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  reconciliationDiscrepancies: z.number().int().nonnegative(),
})

const basisSchema = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("resolved"),
    observation: z.object({
      amount: moneySchema,
      lot_method: z.string(),
      source_reference: z.string(),
    }).loose(),
  }),
  z.object({ status: z.literal("missing") }),
  z.object({
    status: z.literal("ambiguous"),
    candidates: z.array(moneySchema),
    lot_method: z.string(),
  }),
])

const holdingMarkEvidenceSchema = z.object({
  sourceReference: z.string().min(1),
  observedAtUnixNanos: losslessIntegerSchema,
  venue: z.string().min(1).nullable(),
  venueStatus: z.string().min(1),
  state: z.string().min(1),
  quality: z.string().min(1),
  executionEligible: z.boolean(),
  freshness: z.object({
    status: z.string().min(1),
    reason: z.string().min(1),
  }),
  fallback: z.object({
    status: z.string().min(1),
    reason: z.string().min(1),
  }),
})

export const holdingSchema = z.object({
  account_id: z.string().min(1),
  instrument_id: z.string().min(1),
  currency: z.string().min(1),
  quantity: exactDecimalSchema,
  lot_size: exactDecimalSchema,
  market_value: moneySchema,
  as_of: losslessIntegerSchema,
  basis: basisSchema,
  source_reference: z.string().min(1),
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  artifactSha256: digestSchema,
  markEvidence: holdingMarkEvidenceSchema,
})

export const portfolioTransactionSchema = z.object({
  broker_transaction_id: z.string().min(1),
  account_id: z.string().min(1),
  instrument_id: z.string().min(1).nullable(),
  kind: z.enum(["trade", "cash_transfer", "income", "fee", "corporate_action"]),
  amount: moneySchema,
  quantity: exactDecimalSchema.nullable(),
  occurred_at: losslessIntegerSchema,
  lot_method: z.string().nullable(),
  source_reference: z.string().min(1),
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  artifactSha256: digestSchema,
})

const reportBase = z.object({
  accountId: z.string().min(1),
  revisionId: digestSchema,
  policy: z.string().min(1),
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
})

const markEvidenceSchema = z.object({
  sourceId: z.string().min(1),
  sourceCoverage: z.array(z.string()),
  artifactSha256: digestSchema,
  quality: z.string().min(1),
  executionEligible: z.boolean(),
})

const advancedReportBase = reportBase.extend({
  markEvidence: markEvidenceSchema,
})

const contributionSchema = z.object({
  instrumentId: z.string().min(1),
  amount: moneySchema,
})

const evaluatedScenarioSchema = z.object({
  id: z.string().min(1),
  composition: z.enum(["additive", "compounded"]),
  contributions: z.array(contributionSchema),
  total: moneySchema,
})

export const portfolioAttributionSchema = advancedReportBase.extend({
  baselineRevisionId: digestSchema,
  baselineEffectiveAtUnixNanos: z.string(),
  baselineAvailableAtUnixNanos: z.string().nullable(),
  contributions: z.array(contributionSchema),
  total: moneySchema,
  methodDisclosure: z.literal(
    "source_mark_change_without_cash_flow_or_corporate_action_adjustment",
  ),
})

export const portfolioScenarioResultSchema = advancedReportBase.extend({
  scenario: evaluatedScenarioSchema,
})

export const portfolioScenarioBatchResultSchema = advancedReportBase.extend({
  scenarios: z.array(evaluatedScenarioSchema),
})

export const portfolioRebalanceSchema = advancedReportBase.extend({
  trades: z.array(
    z.object({
      instrumentId: z.string().min(1),
      valueChange: moneySchema,
    }),
  ),
  projectedCash: moneySchema,
  turnover: exactDecimalSchema,
  constrained: z.boolean(),
  authority: z.object({
    proposalOnly: z.literal(true),
    executionAuthority: z.literal(false),
    riskApprovalRequiredBeforeAnyOrder: z.literal(true),
  }),
})

const candidateDecimalSchema = z.string().regex(
  /^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/,
)
const integerTextSchema = z.string().regex(/^-?\d+$/)
const unsignedIntegerTextSchema = z.string().regex(/^\d+$/)
const positiveU64TextSchema = z
  .string()
  .regex(/^[1-9]\d*$/)
  .refine((value) => BigInt(value) <= 18_446_744_073_709_551_615n)
const uuidSchema = z.string().uuid()
const candidateCurrencySchema = z.string().regex(/^[A-Z]{3}$/)
const candidateMoneySchema = z
  .object({
    amount: candidateDecimalSchema,
    currency: candidateCurrencySchema,
  })
  .strict()
const evidenceDigestSchema = z
  .object({
    algorithm: z.enum(["sha256", "blake3"]),
    bytes: digestSchema,
  })
  .strict()
const sha256EvidenceDigestSchema = z
  .object({
    algorithm: z.literal("sha256"),
    bytes: digestSchema,
  })
  .strict()
const unavailableEvidenceSchema = <Reason extends string>(reason: Reason) =>
  z
    .object({
      status: z.literal("unavailable"),
      reason: z.literal(reason),
    })
    .strict()
const candidateCostSchema = z.discriminatedUnion("status", [
  z
    .object({
      status: z.literal("available"),
      amount: candidateMoneySchema,
      evidenceDigest: evidenceDigestSchema,
    })
    .strict(),
  z
    .object({
      status: z.literal("unavailable"),
      reason: z.enum(["exact_fees", "exact_slippage"]),
    })
    .strict(),
])
const riskCheckSchema = z.enum([
  "selected_account",
  "current_portfolio_revision",
  "fresh_selected_mark",
  "instrument_terms",
  "position_lot_alignment",
  "portfolio_wide_selected_marks",
  "liquidity",
  "settlement_backed_sizing",
  "fees",
  "slippage",
])

export const portfolioCandidateImpactSchema = z
  .object({
    accountId: uuidSchema,
    revisionId: digestSchema,
    setupEvidence: z
      .object({
        setupRevision: positiveU64TextSchema,
        setupDigest: digestSchema,
        configurationDigest: digestSchema,
        profileDigest: digestSchema,
        catalogDigest: digestSchema,
      })
      .strict(),
    policy: z.literal("selected_market_candidate_impact_v3"),
    evidenceSchemaVersion: z.literal(1),
    evidenceDigest: sha256EvidenceDigestSchema,
    portfolioEvidence: z
      .object({
        revisionId: digestSchema,
        effectiveAtUnixNanos: integerTextSchema,
        availableAtUnixNanos: integerTextSchema,
        sourceId: z.string().min(1).max(256),
        sourceCoverage: z.array(z.string().min(1).max(256)).max(4_096),
        artifactSha256: digestSchema,
      })
      .strict(),
    instrumentId: uuidSchema,
    positionState: z.enum(["zero_position", "existing_holding"]),
    currentQuantity: candidateDecimalSchema,
    proposedQuantity: candidateDecimalSchema,
    currentMarketValue: candidateMoneySchema,
    proposedMarketValue: candidateMoneySchema,
    capitalChange: candidateMoneySchema,
    portfolioValue: candidateMoneySchema,
    portfolioValueBasis: z.literal(
      "source_reported_holdings_with_selected_candidate_revalued",
    ),
    instrumentTerms: z
      .object({
        definitionRevision: positiveU64TextSchema,
        priceTick: candidateDecimalSchema,
        lotSize: candidateDecimalSchema,
        quoteCurrency: candidateCurrencySchema,
        settlementDenomination: z.union([
          z
            .object({
              kind: z.literal("currency"),
              currency: candidateCurrencySchema,
            })
            .strict(),
          z
            .object({
              kind: z.literal("asset"),
              instrumentId: uuidSchema,
            })
            .strict(),
        ]),
        contractMultiplier: candidateDecimalSchema,
      })
      .strict(),
    costEvidence: z
      .object({
        fees: candidateCostSchema,
        slippage: candidateCostSchema,
      })
      .strict(),
    concentration: z
      .object({
        current: candidateDecimalSchema,
        proposed: candidateDecimalSchema,
        change: candidateDecimalSchema,
      })
      .strict(),
    scenario: z
      .object({
        scope: z.literal("candidate_position_only"),
        shock: candidateDecimalSchema,
        currentImpact: candidateMoneySchema,
        proposedImpact: candidateMoneySchema,
        marginalImpact: candidateMoneySchema,
      })
      .strict(),
    markEvidence: z
      .object({
        status: z.literal("fresh_selected_market_observation"),
        instrumentId: uuidSchema,
        unitMark: candidateMoneySchema,
        markKind: z.enum(["last_trade", "midpoint"]),
        quality: z.enum([
          "direct_verified",
          "direct_unverified",
          "official_delayed",
          "aggregated",
          "indicative",
          "modeled",
          "estimated",
        ]),
        sourceId: z.string().min(1).max(256),
        observationDigest: evidenceDigestSchema,
        observedAtUnixNanos: integerTextSchema,
        availableAtUnixNanos: integerTextSchema,
        freshUntilUnixNanosExclusive: integerTextSchema,
        evaluatedAtUnixNanos: integerTextSchema,
        portfolioRevisionId: digestSchema,
        selection: z
          .object({
            instrumentId: uuidSchema,
            sourceId: z.string().min(1).max(256),
            policyRevision: z.number().int().positive().max(4_294_967_295),
            policyDigest: evidenceDigestSchema,
            receiptDigest: evidenceDigestSchema,
            sourceStateRevision: unsignedIntegerTextSchema.nullable(),
            selectedAtUnixNanos: integerTextSchema,
          })
          .strict(),
      })
      .strict(),
    availability: z
      .object({
        portfolioWideSelectedMarks: unavailableEvidenceSchema(
          "portfolio_wide_selected_market_marks",
        ),
        liquidity: unavailableEvidenceSchema("exact_selected_source_liquidity"),
        settlementBackedSizing: unavailableEvidenceSchema(
          "settlement_backed_sizing",
        ),
        factorClassification: unavailableEvidenceSchema(
          "exact_factor_classification",
        ),
      })
      .strict(),
    riskAdvisory: z
      .object({
        outcome: z.literal("indeterminate_at_evaluation"),
        evaluatedAtUnixNanos: integerTextSchema,
        checksEvaluated: z.array(riskCheckSchema).max(10),
        checksUnavailable: z.array(riskCheckSchema).max(10),
        evidenceDigest: evidenceDigestSchema,
        authority: z.literal("analysis_only"),
        reservation: z.literal(false),
        order: z.literal(false),
      })
      .strict(),
    authority: z
      .object({
        analysisOnly: z.literal(true),
        portfolioMutation: z.literal(false),
        executionAuthority: z.literal(false),
        riskAuthority: z.literal("analysis_only"),
        reservation: z.literal(false),
        order: z.literal(false),
        riskApprovalRequiredBeforeAnyOrder: z.literal(true),
      })
      .strict(),
  })
  .strict()
  .superRefine((value, context) => {
    const identitiesMatch =
      value.revisionId === value.portfolioEvidence.revisionId &&
      value.revisionId === value.markEvidence.portfolioRevisionId &&
      value.instrumentId === value.markEvidence.instrumentId &&
      value.instrumentId === value.markEvidence.selection.instrumentId &&
      value.markEvidence.sourceId === value.markEvidence.selection.sourceId
    const currenciesMatch = [
      value.currentMarketValue.currency,
      value.proposedMarketValue.currency,
      value.capitalChange.currency,
      value.portfolioValue.currency,
      value.scenario.currentImpact.currency,
      value.scenario.proposedImpact.currency,
      value.scenario.marginalImpact.currency,
      value.markEvidence.unitMark.currency,
      ...(value.costEvidence.fees.status === "available"
        ? [value.costEvidence.fees.amount.currency]
        : []),
      ...(value.costEvidence.slippage.status === "available"
        ? [value.costEvidence.slippage.amount.currency]
        : []),
    ].every((currency) => currency === value.instrumentTerms.quoteCurrency)
    if (!identitiesMatch || !currenciesMatch) {
      context.addIssue({
        code: "custom",
        message: "Candidate impact evidence identities do not match.",
      })
    }
  })

const reconciliationDetailSchema = z.object({
  field: z.enum(["cash", "market_value", "cost_basis"]),
  supplied: moneySchema,
  calculated: moneySchema,
  currency: z.string().min(1),
  tolerance: z.object({
    kind: z.literal("absolute"),
    amount: moneySchema,
  }),
  sourceReference: z.string().min(1),
})

const measuredAccountingSchema = z.object({
  status: z.string().min(1),
  amount: moneySchema.optional(),
  reason: z.string().min(1).optional(),
})

const accountingEvidenceSchema = z.object({
  cash: z.object({
    amount: moneySchema,
    observedAtUnixNanos: losslessIntegerSchema,
    sourceReference: z.string().min(1),
    status: z.literal("source_reported_snapshot"),
  }),
  reportedMarketValue: moneySchema,
  unrealizedGain: measuredAccountingSchema,
  realizedGain: measuredAccountingSchema,
  income: measuredAccountingSchema,
  fees: measuredAccountingSchema,
  reconciliation: z.object({
    status: z.string().min(1),
    discrepancies: z.array(reconciliationDetailSchema),
  }),
})

export const performanceSchema = reportBase.extend({
  currentValue: moneySchema,
  historyStatus: z.string().optional(),
  timeWeightedReturn: exactDecimalSchema.optional(),
  moneyWeightedReturn: exactDecimalSchema.optional(),
  periods: z.number().int().nonnegative().optional(),
  analyticsEvidenceDigest: digestSchema.optional(),
  accountingEvidence: accountingEvidenceSchema.optional(),
})

const exposureRowSchema = z.object({
  amount: moneySchema,
})

export const exposureSchema = reportBase.extend({
  instrument: z.array(
    exposureRowSchema.extend({ instrumentId: z.string().min(1) }),
  ),
  currency: z.array(
    exposureRowSchema.extend({ currency: z.string().min(1) }),
  ),
  sector: z.array(
    exposureRowSchema.extend({ classification: z.string().min(1) }),
  ),
  factor: z.array(
    exposureRowSchema.extend({ classification: z.string().min(1) }),
  ),
  net: moneySchema.optional(),
  gross: moneySchema.optional(),
  calculationStatus: z.string().optional(),
  classificationStatus: z.string().optional(),
})

const scenarioSchema = z.object({
  id: z.string().min(1),
  status: z.string().optional(),
  impact: moneySchema.optional(),
})

export const riskSchema = reportBase.extend({
  confidence: z.number().min(0).max(1),
  scenario: scenarioSchema,
  historyStatus: z.string().optional(),
  valueAtRisk: z.number().nonnegative().optional(),
  expectedShortfall: z.number().nonnegative().optional(),
  observations: z.number().int().nonnegative().optional(),
  annualizedVolatility: z.number().nonnegative().optional(),
  volatilityStatus: z.string().optional(),
  trackingErrorStatus: z.string().optional(),
})

const qualitySchema = z
  .object({
    class: z.string().optional(),
    executionEligible: z.boolean().optional(),
    reconciliationDiscrepancies: z.number().int().nonnegative().optional(),
    rawEvidenceRetained: z.boolean().optional(),
  })
  .loose()

export interface ResultEvidence {
  completeness: string
  returnedItems: number
  availableItems: number
  sourceCoverage: unknown
  dataQuality: z.infer<typeof qualitySchema> | null
}

export interface PortfolioResult<T> {
  value: T
  evidence: ResultEvidence
}

export type PortfolioAccount = z.infer<typeof portfolioAccountSchema>
export type PortfolioRevision = z.infer<typeof portfolioRevisionSchema>
export type PortfolioHolding = z.infer<typeof holdingSchema>
export type PortfolioTransaction = z.infer<typeof portfolioTransactionSchema>
export type PortfolioPerformance = z.infer<typeof performanceSchema>
export type PortfolioExposure = z.infer<typeof exposureSchema>
export type PortfolioRisk = z.infer<typeof riskSchema>
export type PortfolioAttribution = z.infer<typeof portfolioAttributionSchema>
export type PortfolioScenarioResult = z.infer<typeof portfolioScenarioResultSchema>
export type PortfolioScenarioBatchResult = z.infer<
  typeof portfolioScenarioBatchResultSchema
>
export type PortfolioRebalance = z.infer<typeof portfolioRebalanceSchema>
export type PortfolioCandidateImpact = z.infer<typeof portfolioCandidateImpactSchema>
export type Money = z.infer<typeof moneySchema>

const portfolioImportTransactionSchema = z.object({
  recordId: z.string().min(1),
  brokerTransactionId: z.string().min(1),
  sourceReference: z.string().min(1),
  rawPayloadDigest: z.object({
    algorithm: z.enum(["sha256", "blake3"]),
    value: digestSchema,
  }),
  sourceRevision: z.string().min(1),
  supersedesSourceRevision: z.string().min(1).nullable(),
  classification: z.enum([
    "trade",
    "cash_transfer",
    "income",
    "fee",
    "corporate_action",
  ]),
  amount: z.object({
    value: exactDecimalSchema,
    currency: z.string().min(1),
  }),
  quantity: exactDecimalSchema.nullable(),
  occurredAtUnixNanos: losslessIntegerSchema,
  lotMethod: z.string().min(1).nullable(),
  allowedInterpretations: z.array(z.string().min(1)),
  eligibleOpeningLotIds: z.array(z.string().min(1)),
})

const portfolioImportPreviewSchema = z.object({
  previewId: digestSchema,
  digest: digestSchema,
  preview: z.object({
    accountId: z.string().min(1),
    disposition: z.enum(["applied", "replay"]),
    rawRecords: z.array(
      z.object({
        sourceReference: z.string().min(1),
        payloadDigest: z.object({
          algorithm: z.enum(["sha256", "blake3"]),
          value: digestSchema,
        }),
      }),
    ),
    transactions: z.array(portfolioImportTransactionSchema),
    reconciliationDiscrepancies: z.array(z.unknown()),
    corporateActionRequirements: z.array(
      z.object({
        recordId: z.string().min(1),
        sourceReference: z.string().min(1),
        instrumentId: z.string().min(1).nullable(),
      }),
    ),
    resolutionRequirements: z.object({
      requiresGovernedInterpretation: z.boolean(),
      requiresServerHeldCorporateActionPlan: z.boolean(),
      specificIdentificationUsesOnlyServerEnumeratedLots: z.literal(true),
    }),
  }),
})

const portfolioImportCommitSchema = z.object({
  approvalId: z.string().uuid(),
  previewId: digestSchema,
  previewDigest: digestSchema,
  receipt: z.record(z.string(), z.unknown()),
  status: z.literal("committed"),
})

export type PortfolioImportPreview = z.infer<typeof portfolioImportPreviewSchema>
export type PortfolioImportTransaction = z.infer<typeof portfolioImportTransactionSchema>
export type PortfolioImportCommit = z.infer<typeof portfolioImportCommitSchema>

export function parsePortfolioImportPreview(value: unknown): PortfolioImportPreview {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportPreviewSchema })
    .safeParse(value)
  if (!parsed.success || parsed.data.data.previewId !== parsed.data.data.digest) {
    throw new Error(
      "The installed service returned an import preview this dashboard cannot safely interpret.",
    )
  }
  return parsed.data.data
}

export function parsePortfolioImportCommit(value: unknown): PortfolioImportCommit {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportCommitSchema })
    .safeParse(value)
  if (
    !parsed.success ||
    parsed.data.data.previewId !== parsed.data.data.previewDigest
  ) {
    throw new Error(
      "The installed service returned an import receipt this dashboard cannot safely interpret.",
    )
  }
  return parsed.data.data
}

export function parsePortfolioResult<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
  emptyValue?: z.input<Schema>,
): PortfolioResult<z.infer<Schema>> {
  const parsed = schema.safeParse(
    result.data === null && emptyValue !== undefined ? emptyValue : result.data,
  )
  if (!parsed.success) {
    throw new Error(
      "The installed service returned portfolio data this dashboard cannot safely interpret.",
    )
  }
  const quality = qualitySchema.safeParse(result.metadata.dataQuality)
  return {
    value: parsed.data,
    evidence: {
      completeness: result.metadata.completeness,
      returnedItems: result.metadata.returnedItems,
      availableItems: result.metadata.availableItems,
      sourceCoverage: result.metadata.sourceCoverage,
      dataQuality: quality.success ? quality.data : null,
    },
  }
}
