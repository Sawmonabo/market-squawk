import { z } from "zod"

import type { MoneyValue } from "@/lib/formatters"
import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z
  .union([z.string().regex(/^-?\d+$/), z.number().int()])
  .transform(String)
const digestSchema = z.array(z.number().int().min(0).max(255)).length(32)
const evidenceDigestSchema = z
  .object({
    algorithm: z.enum(["sha256", "blake3"]),
    bytes: digestSchema,
  })
  .strict()
  .transform((digest) => digest.bytes)
const revisionTokenSchema = digestSchema.nullable()
const moneySchema = z.object({
  amount: z.string(),
  currency: z.string().min(1),
}) satisfies z.ZodType<MoneyValue>

const bindingSchema = z.object({
  name: z.string().min(1),
  version: z.number().int().positive(),
  semanticDigest: digestSchema,
})

const screenSchema = z.object({
  id: z.string().min(1),
  revision: z.number().int().positive(),
  universeIdentity: evidenceDigestSchema,
  asOfSemantics: z.literal("available_at_or_before_cutoff"),
  predicates: z.array(
    z.object({
      binding: bindingSchema,
      operator: z.enum([
        "less_than",
        "less_than_or_equal",
        "equal",
        "greater_than_or_equal",
        "greater_than",
      ]),
      threshold: z.number(),
      nullPolicy: z.enum(["exclude", "include"]),
    }),
  ),
  ranking: z.object({
    binding: bindingSchema,
    direction: z.enum(["ascending", "descending"]),
  }),
  maximumResults: z.number().int().positive(),
  constraints: z.object({
    minimumCoverage: z.number(),
    minimumLiquidity: z.number(),
    admittedDataQualities: z.array(z.string()),
  }),
})

const candidateSchema = z.object({
  id: z.string().min(1),
  screenRunId: z.string().min(1),
  screenId: z.string().min(1),
  screenRevision: z.number().int().positive(),
  instrumentId: z.string().min(1),
  rank: z.number().int().positive(),
  score: z.number(),
  selectedAt: timestampSchema,
  scoreContributions: z.array(
    z.object({
      binding: bindingSchema,
      observed: z.number().nullable(),
      contribution: z.number(),
    }),
  ),
  coverage: z.number(),
  liquidity: z.number(),
  dataQuality: z.string(),
  portfolioRevision: revisionTokenSchema,
  flags: z.array(z.string()),
  evidenceIdentity: evidenceDigestSchema,
})

const screenRunIndexSchema = z.object({
  id: z.string().min(1),
  screenId: z.string().min(1),
  screenRevision: z.number().int().positive(),
  asOf: timestampSchema,
  datasetIdentity: evidenceDigestSchema,
  universeIdentity: evidenceDigestSchema,
  candidateCount: z.number().int().nonnegative(),
})

const dossierSchema = z.object({
  id: z.string().min(1),
  candidateId: z.string().min(1),
  instrumentId: z.string().min(1),
  assembledAt: timestampSchema,
  evidence: z.object({
    modelBundle: z.string().nullable(),
    portfolioRevision: revisionTokenSchema,
    fairValueDecision: z.string().nullable(),
    contentIdentity: evidenceDigestSchema,
  }),
  references: z.array(
    z.object({
      section: z.string().min(1),
      contentIdentity: evidenceDigestSchema,
    }),
  ),
})

const reviewSchema = z.object({
  id: z.string().min(1),
  targetId: z.string().min(1),
  targetRevision: z.number().int().positive(),
  reviewer: z.string().min(1),
  reviewedAt: timestampSchema,
  disposition: z.enum(["activate", "reject", "needs_changes"]),
  contentIdentity: evidenceDigestSchema,
})

const invalidationSchema = z.object({
  id: z.string().min(1),
  targetId: z.string().min(1),
  targetRevision: z.number().int().positive(),
  kind: z.enum([
    "corporate_action",
    "model",
    "data",
    "reference_mark",
    "assumption",
  ]),
  actor: z.string().min(1).nullable(),
  observedAt: timestampSchema,
  contentIdentity: evidenceDigestSchema,
})

const targetSchema = z.object({
  id: z.string().min(1),
  revision: z.number().int().positive(),
  dossierId: z.string().min(1),
  instrumentId: z.string().min(1),
  referencePrice: moneySchema,
  referenceObservedAt: timestampSchema,
  referenceIdentity: evidenceDigestSchema,
  downside: moneySchema,
  base: moneySchema,
  upside: moneySchema,
  entryLower: moneySchema,
  entryUpper: moneySchema,
  trimLower: moneySchema,
  trimUpper: moneySchema,
  exitLower: moneySchema,
  exitUpper: moneySchema,
  createdAt: timestampSchema,
  horizonAt: timestampSchema,
  expiresAt: timestampSchema,
  targetIdentity: evidenceDigestSchema,
  addCase: moneySchema,
  method: z.string().min(1),
  assumptions: z.array(
    z.object({
      text: z.string().min(1),
      evidenceIdentity: evidenceDigestSchema,
    }),
  ),
  portfolioRevision: revisionTokenSchema,
  effectiveAt: timestampSchema,
  reviewDueAt: timestampSchema,
  supersedes: z
    .object({ revision: z.number().int().positive(), supersededAt: timestampSchema })
    .nullable(),
  thesis: z.string().min(1),
  risks: z.array(z.string().min(1)),
  invalidationConditions: z.array(z.string().min(1)),
  forecast: evidenceDigestSchema.nullable(),
  fairValue: z.string().nullable(),
  markQuality: z.string(),
  author: z.string().min(1),
  rulesetVersion: z.number().int().positive(),
})

const targetStateSchema = z.object({
  target: targetSchema,
  status: z.enum([
    "pending_review",
    "active",
    "rejected",
    "needs_changes",
    "needs_review",
    "superseded",
  ]),
  latestReview: reviewSchema.nullable(),
  latestInvalidation: invalidationSchema.nullable(),
})

const targetIndexSchema = z.object({
  id: z.string().min(1),
  revision: z.number().int().positive(),
  instrumentId: z.string().min(1),
  status: z.enum([
    "pending_review",
    "active",
    "rejected",
    "needs_changes",
    "needs_review",
    "superseded",
  ]),
})

const screenListSchema = z.object({ screens: z.array(screenSchema) })
const screenRunListSchema = z.object({
  runs: z.array(screenRunIndexSchema),
  nextAfter: z.string().min(1).nullable().optional(),
})
const candidateListSchema = z.object({ candidates: z.array(candidateSchema) })
const candidateDossierListSchema = z.object({
  dossiers: z.array(dossierSchema),
  nextAfter: z.string().min(1).nullable().optional(),
})
const targetListSchema = z.object({ targets: z.array(targetStateSchema) })
const targetIndexListSchema = z.object({
  targets: z.array(targetIndexSchema),
  nextAfter: z.string().min(1).nullable().optional(),
})
const governancePrincipalSchema = z.object({
  principalId: z.string().min(1),
  displayName: z.string().min(1),
  roles: z.array(z.string().min(1)).min(1),
})
const governancePrincipalPageSchema = z.object({
  principals: z.array(governancePrincipalSchema),
  nextAfter: z.string().nullable().optional(),
})
const governancePreviewSchema = z.object({
  preview: z.object({
    previewId: z.string().min(1),
    digest: z.string().min(1),
    requiredRoles: z.array(z.string().min(1)).min(1),
    distinctPrincipalCount: z.number().int().positive(),
    eligiblePrincipalIds: z.array(z.string().min(1)),
    expiresAt: z.string().min(1),
    effects: z.array(z.object({ kind: z.string().min(1) })).min(1),
  }),
})
const governanceAuthorizationSchema = z.object({
  authorization: z.object({
    authorizationHandle: z.string().min(1),
    previewId: z.string().min(1),
    principalId: z.string().min(1),
    expiresAt: z.string().min(1),
  }),
})
const governanceReceiptSchema = z.object({
  receipt: z.object({
    receiptId: z.string().min(1),
    previewId: z.string().min(1),
    digest: z.string().min(1),
    committedAt: z.string().min(1),
    authorizedPrincipals: z.array(
      z.object({
        principalId: z.string().min(1),
        roles: z.array(z.string().min(1)),
      }),
    ),
    effects: z.array(z.object({ kind: z.string().min(1) })),
  }),
})

const digestHexSchema = z.string().regex(/^[0-9a-f]{64}$/)
const boundedIdentitySchema = z.string().min(1).max(128)
const boundedFeatureNameSchema = z.string().min(1).max(96)
const dataQualitySchema = z.enum([
  "direct_verified",
  "direct_unverified",
  "official_delayed",
  "aggregated",
  "indicative",
  "modeled",
  "estimated",
  "stale",
  "quarantined",
])
const featureDataTypeSchema = z.enum([
  "price_ticks",
  "quantity_lots",
  "basis_points",
  "timestamp",
  "aggressor_side",
  "order_side",
  "exact_ratio",
  "instrument_id",
  "venue_id",
  "signed_integer",
  "unsigned_integer",
  "boolean",
  "statistical_f64",
  "decimal",
  "money",
  "canonical_identifier",
  "exact_rate",
  "decimal_measurement",
  "monetary_value",
  "statistical_location",
  "statistical_dispersion",
])
const featureUnitSchema = z.enum([
  "price_ticks",
  "quantity_lots",
  "basis_points",
  "ratio",
  "return",
  "volatility",
  "lots_per_second",
  "count",
  "nanoseconds",
  "unitless",
  "rate",
  "currency_amount",
])
const featureOutputTypeSchema = z.enum([
  "price_ticks",
  "half_tick_price",
  "quantity_lots",
  "basis_points",
  "signed_integer",
  "unsigned_integer",
  "exact_ratio",
  "statistical_f64",
  "decimal",
  "money",
])
const featureParameterSchema = z
  .object({
    name: z.string().min(1).max(64),
    value: z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("signed_integer"), value: z.number().int() }).strict(),
      z.object({ kind: z.literal("unsigned_integer"), value: z.number().int().nonnegative() }).strict(),
      z.object({ kind: z.literal("boolean"), value: z.boolean() }).strict(),
      z.object({ kind: z.literal("duration_nanos"), value: z.number().int().positive() }).strict(),
      z.object({ kind: z.literal("variance_convention"), value: z.enum(["population", "sample"]) }).strict(),
      z.object({ kind: z.literal("missing_value_policy"), value: z.enum(["reject", "drop"]) }).strict(),
      z.object({ kind: z.literal("weight_policy"), value: z.enum(["equal", "positive_normalized"]) }).strict(),
      z.object({ kind: z.literal("rounding_policy"), value: z.string().min(1).max(64) }).strict(),
      z.object({ kind: z.literal("shock_composition"), value: z.enum(["additive", "compounded"]) }).strict(),
    ]),
  })
  .strict()
const featureTimeSemanticsSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("event_time") }).strict(),
  z.object({ kind: z.literal("trailing_window"), durationNanos: z.number().int().positive() }).strict(),
  z.object({ kind: z.literal("cross_venue"), maximumSkewNanos: z.number().int().positive() }).strict(),
])
const featureWarmUpSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("none") }).strict(),
  z.object({ kind: z.literal("observations"), observations: z.number().int().positive() }).strict(),
  z.object({ kind: z.literal("duration_nanos"), durationNanos: z.number().int().positive() }).strict(),
  z
    .object({
      kind: z.literal("observations_and_duration"),
      observations: z.number().int().positive(),
      durationNanos: z.number().int().positive(),
    })
    .strict(),
])
const featureContractSchema = z
  .object({
    kind: z.literal("feature_contract"),
    name: boundedFeatureNameSchema,
    version: z.number().int().positive(),
    inputs: z
      .array(
        z
          .object({
            name: z.string().min(1).max(64),
            dataType: featureDataTypeSchema,
            unit: featureUnitSchema,
            nullable: z.boolean(),
          })
          .strict(),
      )
      .min(1)
      .max(32),
    inputSchemaDigest: digestHexSchema,
    parameters: z.array(featureParameterSchema).max(32),
    timeSemantics: featureTimeSemanticsSchema,
    warmUp: featureWarmUpSchema,
    nullPolicy: z.enum(["unavailable", "warming_up", "ignore_nullable"]),
    outputType: featureOutputTypeSchema,
    outputUnit: featureUnitSchema,
    liveCompatible: z.boolean(),
    pointInTimeCompatible: z.boolean(),
    implementationRevision: z.string().min(1).max(128),
    implementationDigest: digestHexSchema,
    semanticDigest: digestHexSchema,
  })
  .strict()
const featureManifestSchema = z
  .object({
    dataset: boundedIdentitySchema,
    manifestVersion: z.number().int().nonnegative(),
    schema: z
      .object({
        name: z.string().min(1).max(128),
        version: z.number().int().positive(),
        fingerprint: digestHexSchema,
      })
      .strict(),
    contentHash: digestHexSchema,
  })
  .strict()
const splitCountsSchema = z
  .object({
    train: z.number().int().nonnegative(),
    validation: z.number().int().nonnegative(),
    test: z.number().int().nonnegative(),
  })
  .strict()
const legacyFeatureDatasetSchema = z
  .object({
    kind: z.literal("feature_dataset"),
    manifest: featureManifestSchema,
    buildSpecDigest: digestHexSchema,
    policyDigest: digestHexSchema,
    universeDigest: digestHexSchema,
    splitCounts: splitCountsSchema,
  })
  .strict()
const durableFeatureDatasetSchema = z
  .object({
    kind: z.literal("feature_dataset"),
    manifest: featureManifestSchema,
    buildSpecDigest: digestHexSchema.nullable(),
    policyDigest: digestHexSchema,
    universeId: boundedIdentitySchema,
    universeDigest: digestHexSchema,
    pythonExportSha256: digestHexSchema,
    splitCounts: splitCountsSchema,
  })
  .strict()
const featureDatasetSchema = z.union([
  durableFeatureDatasetSchema,
  legacyFeatureDatasetSchema,
])
const featureDatasetPageSchema = z
  .object({
    items: z.array(z.union([featureContractSchema, featureDatasetSchema])).max(4_096),
    hasMore: z.boolean(),
    nextAfterDataset: boundedIdentitySchema.nullable(),
  })
  .strict()
const featureDatasetResultMetadataSchema = z
  .object({
    completeness: z.enum(["complete", "truncated"]),
    returnedItems: z.number().int().nonnegative(),
    availableItems: z.number().int().nonnegative(),
    sourceCoverage: z
      .object({
        sources: z.array(boundedIdentitySchema).max(4_096),
        datasetCount: z.number().int().nonnegative(),
        pointInTime: z.literal(true),
      })
      .strict(),
    dataQuality: z
      .object({
        classes: z.array(dataQualitySchema).max(9),
        executionEligible: z.literal(false),
      })
      .strict(),
  })
  .strict()
const savedScreenReceiptSchema = z
  .object({ outcome: z.enum(["appended", "already_present"]) })
  .strict()
const notApplicableSchema = z.object({ status: z.literal("not_applicable") }).strict()
const savedScreenResultMetadataSchema = z
  .object({
    completeness: z.literal("complete"),
    returnedItems: z.literal(1),
    availableItems: z.literal(1),
    sourceCoverage: notApplicableSchema,
    dataQuality: notApplicableSchema,
  })
  .strict()

export type CandidateView = z.infer<typeof candidateSchema>
export type DecisionDossierView = z.infer<typeof dossierSchema>
export type SavedScreenView = z.infer<typeof screenSchema>
export type ScreenRunIndexView = z.infer<typeof screenRunIndexSchema>
export type TargetStateView = z.infer<typeof targetStateSchema>
export type TargetIndexView = z.infer<typeof targetIndexSchema>
export type GovernancePrincipalView = z.infer<typeof governancePrincipalSchema>
export type GovernancePreviewView = z.infer<typeof governancePreviewSchema>["preview"]
export type GovernanceAuthorizationView = z.infer<typeof governanceAuthorizationSchema>["authorization"]
export type GovernanceReceiptView = z.infer<typeof governanceReceiptSchema>["receipt"]
export type FeatureContractView = z.infer<typeof featureContractSchema>
export type FeatureDatasetView = z.infer<typeof featureDatasetSchema>
export type SavedScreenOutcome = z.infer<typeof savedScreenReceiptSchema>["outcome"]
export interface DecisionDiscoveryPage<Item> {
  items: Item[]
  nextAfter: string | null
}

export interface FeatureDatasetPage {
  contracts: FeatureContractView[]
  datasets: FeatureDatasetView[]
  hasMore: boolean
  nextAfterDataset: string | null
  returnedItems: number
  availableItems: number
}

export function parseDecisionScreens(result: ApplicationResult): SavedScreenView[] {
  return parseResult(screenListSchema, result, "saved-screen list").screens
}

export function parseDecisionCandidates(
  result: ApplicationResult,
): CandidateView[] {
  return parseResult(candidateListSchema, result, "candidate funnel").candidates
}

export function parseDecisionScreenRunPage(
  result: ApplicationResult,
): DecisionDiscoveryPage<ScreenRunIndexView> {
  const page = parseResult(screenRunListSchema, result, "saved-screen run discovery")
  return { items: page.runs, nextAfter: page.nextAfter ?? null }
}

export function parseDecisionDossier(
  result: ApplicationResult,
): DecisionDossierView {
  return parseResult(dossierSchema, result, "decision dossier")
}

export function parseDecisionCandidateDossierPage(
  result: ApplicationResult,
): DecisionDiscoveryPage<DecisionDossierView> {
  const page = parseResult(
    candidateDossierListSchema,
    result,
    "candidate-to-dossier discovery",
  )
  return { items: page.dossiers, nextAfter: page.nextAfter ?? null }
}

export function parseDecisionTargets(result: ApplicationResult): TargetStateView[] {
  return parseResult(targetListSchema, result, "target history").targets
}

export function parseDecisionTargetIndexPage(
  result: ApplicationResult,
): DecisionDiscoveryPage<TargetIndexView> {
  const page = parseResult(targetIndexListSchema, result, "target discovery")
  return { items: page.targets, nextAfter: page.nextAfter ?? null }
}

export function parseGovernancePrincipals(
  result: ApplicationResult,
): GovernancePrincipalView[] {
  return parseResult(governancePrincipalPageSchema, result, "governance principals").principals
}

export function parseGovernancePreview(result: ApplicationResult): GovernancePreviewView {
  return parseResult(governancePreviewSchema, result, "governance action preview").preview
}

export function parseGovernanceAuthorization(
  result: ApplicationResult,
): GovernanceAuthorizationView {
  return parseResult(
    governanceAuthorizationSchema,
    result,
    "governance authorization",
  ).authorization
}

export function parseGovernanceReceipt(result: ApplicationResult): GovernanceReceiptView {
  return parseResult(governanceReceiptSchema, result, "governance commit receipt").receipt
}

export function parseFeatureDatasetPage(
  result: ApplicationResult,
  afterDataset?: string,
): FeatureDatasetPage {
  const metadata = featureDatasetResultMetadataSchema.safeParse(result.metadata)
  if (!metadata.success) {
    throw new Error(
      "The installed service returned unsupported feature-dataset evidence.",
    )
  }
  if (result.data === null) {
    if (
      metadata.data.returnedItems !== 0 ||
      metadata.data.availableItems !== 0 ||
      metadata.data.completeness !== "complete" ||
      metadata.data.sourceCoverage.datasetCount !== 0
    ) {
      throw new Error(
        "The installed service returned inconsistent feature-dataset evidence.",
      )
    }
    return {
      contracts: [],
      datasets: [],
      hasMore: false,
      nextAfterDataset: null,
      returnedItems: 0,
      availableItems: 0,
    }
  }
  const page = featureDatasetPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported feature-dataset page.",
    )
  }
  const contracts = page.data.items.filter(
    (item): item is FeatureContractView => item.kind === "feature_contract",
  )
  const datasets = page.data.items.filter(
    (item): item is FeatureDatasetView => item.kind === "feature_dataset",
  )
  const datasetIds = datasets.map((dataset) => dataset.manifest.dataset)
  const contractKeys = contracts.map(
    (contract) => `${contract.name}:${contract.version}:${contract.semanticDigest}`,
  )
  const finalDataset = datasetIds.at(-1) ?? null
  const metadataValue = metadata.data
  const complete = metadataValue.completeness === "complete"
  if (
    page.data.items.length !== metadataValue.returnedItems ||
    metadataValue.returnedItems > metadataValue.availableItems ||
    metadataValue.sourceCoverage.datasetCount !== datasets.length ||
    new Set(datasetIds).size !== datasetIds.length ||
    datasetIds.some(
      (datasetId, index) =>
        (index > 0 && datasetIds[index - 1]! >= datasetId) ||
        (index === 0 && afterDataset !== undefined && afterDataset >= datasetId),
    ) ||
    new Set(contractKeys).size !== contractKeys.length ||
    (afterDataset !== undefined && contracts.length !== 0) ||
    complete === page.data.hasMore ||
    (page.data.hasMore && page.data.nextAfterDataset !== finalDataset) ||
    (!page.data.hasMore && page.data.nextAfterDataset !== null)
  ) {
    throw new Error(
      "The installed service returned inconsistent feature-dataset pagination.",
    )
  }
  return {
    contracts,
    datasets,
    hasMore: page.data.hasMore,
    nextAfterDataset: page.data.nextAfterDataset,
    returnedItems: metadataValue.returnedItems,
    availableItems: metadataValue.availableItems,
  }
}

export function parseSavedScreenOutcome(result: ApplicationResult): SavedScreenOutcome {
  const metadata = savedScreenResultMetadataSchema.safeParse(result.metadata)
  const receipt = savedScreenReceiptSchema.safeParse(result.data)
  if (!metadata.success || !receipt.success) {
    throw new Error(
      "The installed service returned an unsupported saved-screen receipt.",
    )
  }
  return receipt.data.outcome
}

export function digestEvidence(hex: string): {
  algorithm: "sha256"
  bytes: number[]
} {
  if (!/^[0-9a-f]{64}$/.test(hex)) {
    throw new Error("The selected evidence identity is invalid.")
  }
  const bytes: number[] = []
  for (let offset = 0; offset < hex.length; offset += 2) {
    bytes.push(Number.parseInt(hex.slice(offset, offset + 2), 16))
  }
  return { algorithm: "sha256", bytes }
}

export function featureSemanticBytes(hex: string): number[] {
  return digestEvidence(hex).bytes
}

export function digestHex(value: readonly number[] | null): string {
  if (!value) return "Not bound"
  return value.map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function parseResult<Schema extends z.ZodType>(
  schema: Schema,
  result: ApplicationResult,
  label: string,
): z.infer<Schema> {
  const parsed = schema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      `The installed service returned an unsupported ${label} response.`,
    )
  }
  return parsed.data
}
