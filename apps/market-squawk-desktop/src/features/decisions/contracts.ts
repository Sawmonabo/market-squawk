import { z } from "zod"

import type { MoneyValue } from "@/lib/formatters"
import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z
  .union([z.string().regex(/^-?\d+$/), z.number().int()])
  .transform(String)
const digestSchema = z.array(z.number().int().min(0).max(255)).length(32)
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
  universeIdentity: digestSchema,
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
  evidenceIdentity: digestSchema,
})

const screenRunIndexSchema = z.object({
  id: z.string().min(1),
  screenId: z.string().min(1),
  screenRevision: z.number().int().positive(),
  asOf: timestampSchema,
  datasetIdentity: digestSchema,
  universeIdentity: digestSchema,
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
    contentIdentity: digestSchema,
  }),
  references: z.array(
    z.object({
      section: z.string().min(1),
      contentIdentity: digestSchema,
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
  contentIdentity: digestSchema,
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
  contentIdentity: digestSchema,
})

const targetSchema = z.object({
  id: z.string().min(1),
  revision: z.number().int().positive(),
  dossierId: z.string().min(1),
  instrumentId: z.string().min(1),
  referencePrice: moneySchema,
  referenceObservedAt: timestampSchema,
  referenceIdentity: digestSchema,
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
  targetIdentity: digestSchema,
  addCase: moneySchema,
  method: z.string().min(1),
  assumptions: z.array(
    z.object({
      text: z.string().min(1),
      evidenceIdentity: digestSchema,
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
  forecast: digestSchema.nullable(),
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
export interface DecisionDiscoveryPage<Item> {
  items: Item[]
  nextAfter: string | null
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
