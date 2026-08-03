import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const resultCompletenessSchema = z.enum(["complete", "truncated"])

export const researchManifestSchema = z.strictObject({
  datasetId: z.string().min(1),
  manifestVersion: z.number().int().nonnegative(),
  schema: z.strictObject({
    name: z.string().min(1),
    version: z.number().int().positive(),
    fingerprint: digestSchema,
  }),
  contentHash: digestSchema,
})

export const researchDatasetSchema = z.strictObject({
  manifest: researchManifestSchema,
  sourceId: z.string().min(1),
  generationKind: z.enum(["ingest", "compaction", "derived"]),
  buildSpecDigest: digestSchema.nullable(),
  pythonExportSha256: digestSchema.nullable(),
  parents: z.array(
    z.strictObject({
      relation: z.enum([
        "append_predecessor",
        "compaction_predecessor",
        "derived_input",
      ]),
      manifest: researchManifestSchema,
    }),
  ),
  rowCount: z.number().int().nonnegative(),
  totalBytes: z.number().int().nonnegative(),
  lineageDigest: digestSchema,
  objectCount: z.number().int().nonnegative(),
})

const researchDatasetPageSchema = z.strictObject({
  items: z.array(researchDatasetSchema),
  hasMore: z.boolean(),
  nextAfterDataset: z.string().nullable(),
})

export const researchJobSchema = z
  .object({
    jobId: z.string().min(1),
    generation: z.number().int().positive(),
    sequence: z.number().int().nonnegative(),
    kind: z.string().min(1),
    state: z.enum([
      "queued",
      "preparing",
      "running",
      "awaiting_confirmation",
      "cancelling",
      "completed",
      "failed",
      "cancelled",
      "interrupted",
      "recovering",
    ]),
    phase: z.string().nullable(),
    completedUnits: z.number().int().nonnegative().nullable(),
    totalUnits: z.number().int().nonnegative().nullable(),
    cancellationRequested: z.boolean(),
    result: z.record(z.string(), z.unknown()).nullable(),
    failure: z.record(z.string(), z.unknown()).nullable(),
    updatedAt: losslessIntegerSchema,
    recovery: z.string().nullable(),
  })
  .strict()

const researchJobPageSchema = z.strictObject({
  jobs: z.array(researchJobSchema),
  next: z.unknown().nullable(),
})

const observationArtifactSchema = z.strictObject({
  artifactId: z.string().min(1),
  sha256: digestSchema,
  byteCount: z.number().int().nonnegative(),
  mediaType: z.string().min(1),
  rowCount: z.number().int().nonnegative(),
})

const inlineObservationResultSchema = z.strictObject({
  manifest: researchManifestSchema,
  arrowIpcBytes: z.number().int().nonnegative(),
  rows: z.array(z.record(z.string(), z.unknown())),
})

const artifactObservationResultSchema = z.strictObject({
  manifest: researchManifestSchema,
  artifact: observationArtifactSchema,
})

const researchJobReceiptSchema = z.strictObject({
  jobId: z.string().min(1),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  state: z.literal("queued"),
})

const researchSourceInputSchema = z.strictObject({
  provider: z.string().min(1),
  dataset: z.string().min(1),
  label: z.string().min(1),
})

const evidenceDigestWireSchema = z
  .object({
    algorithm: z.enum(["sha256", "blake3"]),
    bytes: z.array(z.number().int().min(0).max(255)).length(32),
  })
  .strict()

const discoveryRequestSchema = z
  .object({
    dataset: z.string().min(1),
    effective_at: losslessIntegerSchema.nullable(),
    max_results: z.number().int().positive(),
    deadline: losslessIntegerSchema,
    request_id: evidenceDigestWireSchema,
  })
  .strict()

const effectiveIntervalSchema = z
  .object({
    starts_at: losslessIntegerSchema,
    ends_at: losslessIntegerSchema.nullable(),
  })
  .strict()

const availabilityEvidenceSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("observed"), available_at: losslessIntegerSchema, evidence: z.string().min(1) }).strict(),
  z.object({ kind: z.literal("local_first_observed"), observed_at: losslessIntegerSchema }).strict(),
  z.object({ kind: z.literal("inferred"), inferred_at: losslessIntegerSchema, method: z.string().min(1) }).strict(),
  z.object({ kind: z.literal("unknown") }).strict(),
])

const exactPayloadEvidenceSchema = z
  .object({
    content_digest: evidenceDigestWireSchema,
    version_pinned_locator: z
      .object({ reference: z.string().min(1), version: z.string().min(1) })
      .strict()
      .optional(),
  })
  .strict()

const sourceObjectSchema = z
  .object({
    source_id: z.string().min(1),
    metadata_revision: z.string().min(1),
    dataset: z.string().min(1),
    discovery_request_id: evidenceDigestWireSchema,
    object_id: z.string().min(1),
    media_type: z.string().min(1),
    evidence: exactPayloadEvidenceSchema,
    effective: effectiveIntervalSchema,
    expected_bytes: losslessIntegerSchema.nullable(),
    published_at: losslessIntegerSchema.nullable(),
    availability: availabilityEvidenceSchema.optional(),
  })
  .strict()

const providerProfileForResearchSchema = z
  .object({
    id: z.string().min(1),
    display_name: z.string().min(1),
    capability_revision: z.unknown(),
    capability_digest: z.unknown(),
    selected_setup_mode: z.unknown(),
    setup_modes: z.unknown(),
    human_boundary: z.unknown(),
    credential_kind: z.unknown(),
    minimum_authority: z.unknown(),
    maximum_authority: z.unknown(),
    verifier_revision: z.unknown(),
    rate_policy: z.unknown(),
    rights_state: z.unknown(),
    lifecycle_support: z.unknown(),
    capability_evidence: z.unknown(),
    refresh_trigger: z.unknown(),
    zero_fee: z.unknown(),
    account_requirement: z.unknown(),
    credential_requirement: z.unknown(),
    administrative_contact_requirement: z.unknown(),
    release_state: z.unknown(),
    official_handoff_url: z.unknown(),
    handoff_instruction: z.unknown(),
    permissions: z.unknown(),
    coverage: z.unknown(),
    quality_ceiling: z.unknown(),
    rights: z.unknown(),
    rights_duties: z.unknown(),
    rights_decision_digest: z.unknown(),
    persistence_evidence: z.unknown(),
    rotation: z.unknown(),
    revocation: z.unknown(),
    recovery: z.unknown(),
    evidence: z.unknown(),
  })
  .strict()

const researchSourceStatusSchema = z
  .object({
    profile: providerProfileForResearchSchema,
    currentSession: z.record(z.string(), z.unknown()).nullable(),
    providerDatasetIdentifier: z.string().min(1).nullable(),
    runtime: z.record(z.string(), z.unknown()),
  })
  .strict()

const sourceObjectListingSchema = z
  .object({
    profile: z.string().min(1),
    metadata: z.record(z.string(), z.unknown()),
    request: discoveryRequestSchema,
    objects: z.array(sourceObjectSchema),
  })
  .strict()

const discoveredSourceObjectSchema = sourceObjectSchema.extend({
  discovery_receipt: z.string().uuid(),
  discovery_receipt_expires_at: losslessIntegerSchema,
})

const sourceDiscoverySchema = z
  .object({
    profile: z.string().min(1),
    metadata: z.record(z.string(), z.unknown()),
    rights: z.record(z.string(), z.unknown()),
    request: discoveryRequestSchema,
    objects: z.array(discoveredSourceObjectSchema),
    receipts_survive_restart: z.literal(false),
  })
  .strict()

export type ResearchDataset = z.infer<typeof researchDatasetSchema>
export type ResearchJob = z.infer<typeof researchJobSchema>
export type ResearchManifest = z.infer<typeof researchManifestSchema>
export type ResearchJobReceipt = z.infer<typeof researchJobReceiptSchema>
export type ResearchSourceInput = z.infer<typeof researchSourceInputSchema>
export type ResearchSourceObject = z.infer<typeof sourceObjectSchema>

export type ResearchObservationResult =
  | {
      kind: "empty"
      returnedItems: number
      completeness: string
    }
  | {
      kind: "inline"
      manifest: ResearchManifest
      rows: Record<string, unknown>[]
      arrowIpcBytes: number
      returnedItems: number
      completeness: string
    }
  | {
      kind: "artifact"
      manifest: ResearchManifest
      artifact: z.infer<typeof observationArtifactSchema>
      returnedItems: number
      completeness: string
    }

export interface ResearchDatasetPage {
  items: ResearchDataset[]
  hasMore: boolean
  nextAfterDataset: string | null
  completeness: string
}

export function parseResearchDatasetPage(
  result: ApplicationResult,
): ResearchDatasetPage {
  if (result.data === null) {
    validateReturnedItems(result, 0, "research dataset")
    return {
      items: [],
      hasMore: false,
      nextAfterDataset: null,
      completeness: result.metadata.completeness,
    }
  }
  const page = researchDatasetPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported research dataset response.",
    )
  }
  if (page.data.hasMore && page.data.nextAfterDataset === null) {
    throw new Error("The research dataset continuation is incomplete.")
  }
  validateReturnedItems(result, page.data.items.length, "research dataset")
  return { ...page.data, completeness: result.metadata.completeness }
}

export function parseResearchJobs(result: ApplicationResult): ResearchJob[] {
  const page = researchJobPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported background-work response.",
    )
  }
  validateReturnedItems(result, page.data.jobs.length, "background-work")
  return page.data.jobs.filter(
    (job) =>
      job.kind.startsWith("research.") ||
      job.kind === "analysis.feature-dataset-build.v1",
  )
}

export function parseResearchManifest(
  result: ApplicationResult,
  expectedDataset: string,
): ResearchDataset {
  const generation = researchDatasetSchema.safeParse(result.data)
  if (
    !generation.success ||
    generation.data.manifest.datasetId !== expectedDataset
  ) {
    throw new Error(
      "The installed service returned an unsupported research manifest response.",
    )
  }
  validateReturnedItems(result, 1, "research manifest")
  return generation.data
}

export function parseResearchObservations(
  result: ApplicationResult,
  expectedDataset: string,
): ResearchObservationResult {
  const common = {
    returnedItems: result.metadata.returnedItems,
    completeness: result.metadata.completeness,
  }
  if (result.data === null) {
    validateReturnedItems(result, 0, "research history")
    return { kind: "empty", ...common }
  }

  const inline = inlineObservationResultSchema.safeParse(result.data)
  if (
    inline.success &&
    inline.data.manifest.datasetId === expectedDataset &&
    inline.data.rows.length === result.metadata.returnedItems
  ) {
    validateReturnedItems(result, inline.data.rows.length, "research history")
    return { kind: "inline", ...inline.data, ...common }
  }

  const artifact = artifactObservationResultSchema.safeParse(result.data)
  if (
    artifact.success &&
    artifact.data.manifest.datasetId === expectedDataset &&
    artifact.data.artifact.rowCount === result.metadata.returnedItems
  ) {
    validateReturnedItems(result, artifact.data.artifact.rowCount, "research history")
    return { kind: "artifact", ...artifact.data, ...common }
  }

  throw new Error(
    "The installed service returned an unsupported research-history response.",
  )
}

export function parseResearchJobReceipt(
  result: ApplicationResult,
): ResearchJobReceipt {
  const receipt = researchJobReceiptSchema.safeParse(result.data)
  if (!receipt.success) {
    throw new Error(
      "The installed service returned an unsupported research-job receipt.",
    )
  }
  validateReturnedItems(result, 1, "research-job receipt")
  return receipt.data
}

export function parseResearchSourceInputs(
  result: ApplicationResult,
): ResearchSourceInput[] {
  if (result.data === null) {
    validateReturnedItems(result, 0, "research source")
    return []
  }
  const statuses = z.array(researchSourceStatusSchema).safeParse(result.data)
  if (!statuses.success) {
    throw new Error(
      "The installed service returned an unsupported research-source response.",
    )
  }
  validateReturnedItems(result, statuses.data.length, "research source")
  const inputs = statuses.data.flatMap((row) =>
    row.providerDatasetIdentifier === null
      ? []
      : [
          researchSourceInputSchema.parse({
            provider: row.profile.id,
            dataset: row.providerDatasetIdentifier,
            label: row.profile.display_name,
          }),
        ],
  )
  const unique = new Map<string, ResearchSourceInput>()
  for (const input of inputs) {
    const key = `${input.provider}\u0000${input.dataset}`
    if (unique.has(key)) {
      throw new Error("The installed service returned a duplicate research-source identity.")
    }
    unique.set(key, input)
  }
  return [...unique.values()].sort((left, right) =>
    left.label.localeCompare(right.label),
  )
}

export function parseResearchSourceObjects(
  result: ApplicationResult,
  expected: ResearchSourceInput,
): ResearchSourceObject[] {
  const listing = sourceObjectListingSchema.safeParse(result.data)
  if (
    !listing.success ||
    listing.data.profile !== expected.provider ||
    listing.data.request.dataset !== expected.dataset ||
    listing.data.objects.some((object) => object.dataset !== expected.dataset)
  ) {
    throw new Error(
      "The installed service returned an unsupported source-object listing.",
    )
  }
  validateReturnedItems(result, listing.data.objects.length, "source-object listing")
  validateSourceObjectIdentities(listing.data)
  return listing.data.objects
}

export function receiptForDiscoveredObject(
  result: ApplicationResult,
  expected: ResearchSourceInput,
  objectId: string,
): string {
  const discovery = sourceDiscoverySchema.safeParse(result.data)
  if (
    !discovery.success ||
    discovery.data.profile !== expected.provider ||
    discovery.data.request.dataset !== expected.dataset
  ) {
    throw new Error(
      "The installed service returned unsupported discovery evidence.",
    )
  }
  validateReturnedItems(result, discovery.data.objects.length, "source discovery")
  validateSourceObjectIdentities(discovery.data)
  if (
    new Set(discovery.data.objects.map((object) => object.discovery_receipt)).size !==
    discovery.data.objects.length
  ) {
    throw new Error("The source discovery returned a duplicate selection receipt.")
  }
  const selected = discovery.data.objects.find(
    (object) =>
      object.object_id === objectId && object.dataset === expected.dataset,
  )
  if (!selected) {
    throw new Error(
      "The selected source object was not present in the confirmed discovery.",
    )
  }
  return selected.discovery_receipt
}

function validateReturnedItems(
  result: ApplicationResult,
  actual: number,
  label: string,
) {
  const { availableItems, returnedItems } = result.metadata
  const completeness = resultCompletenessSchema.parse(result.metadata.completeness)
  if (actual === 0 && availableItems > 0) {
    throw new Error(
      `The ${label} result reports available rows, but none were returned within its bounds.`,
    )
  }
  if (
    returnedItems !== actual ||
    returnedItems > availableItems ||
    (completeness === "complete" && returnedItems !== availableItems) ||
    (completeness === "truncated" && returnedItems >= availableItems)
  ) {
    throw new Error(`The ${label} result counts are inconsistent.`)
  }
}

function validateSourceObjectIdentities(envelope: {
  metadata: Record<string, unknown>
  request: z.infer<typeof discoveryRequestSchema>
  objects: ResearchSourceObject[]
}) {
  const metadataIdentity = z
    .object({
      source_id: z.string().min(1),
      revision_evidence: z.object({ metadata_revision: z.string().min(1) }),
    })
    .safeParse(envelope.metadata)
  const requestIdentity = JSON.stringify(envelope.request.request_id)
  if (
    !metadataIdentity.success ||
    envelope.objects.length > envelope.request.max_results ||
    envelope.objects.some(
      (object) =>
        object.source_id !== metadataIdentity.data.source_id ||
        object.metadata_revision !==
          metadataIdentity.data.revision_evidence.metadata_revision ||
        JSON.stringify(object.discovery_request_id) !== requestIdentity,
    ) ||
    new Set(envelope.objects.map((object) => object.object_id)).size !==
      envelope.objects.length
  ) {
    throw new Error("The source-object identities are inconsistent.")
  }
}
