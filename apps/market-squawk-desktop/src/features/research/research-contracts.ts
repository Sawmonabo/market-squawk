import { z } from "zod"

import {
  applicationResultSchema,
  type ApplicationResult,
} from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const resultCompletenessSchema = z.enum(["complete", "truncated"])
const U64_MAX = 18_446_744_073_709_551_615n
const canonicalUnsignedU64Schema = z.string().refine(
  (value) =>
    value.length <= 20 &&
    /^(?:0|[1-9]\d*)$/.test(value) &&
    BigInt(value) <= U64_MAX,
  { message: "Expected a canonical unsigned 64-bit decimal" },
)
const canonicalPositiveU64Schema = canonicalUnsignedU64Schema.refine(
  (value) => value !== "0",
  { message: "Expected a positive canonical 64-bit decimal" },
)

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

const researchGenerationCommonShape = {
  manifest: researchManifestSchema,
  sourceId: z.string().min(1),
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
}

export const researchDatasetSchema = z.discriminatedUnion("generationKind", [
  z.strictObject({
    ...researchGenerationCommonShape,
    generationKind: z.literal("ingest"),
    buildSpecDigest: z.null(),
  }),
  z.strictObject({
    ...researchGenerationCommonShape,
    generationKind: z.literal("compaction"),
    buildSpecDigest: z.null(),
  }),
  z.strictObject({
    ...researchGenerationCommonShape,
    generationKind: z.literal("derived"),
    buildSpecDigest: digestSchema,
    publicationStage: z.literal("phase_one_derived_generation"),
    phaseOneDescriptorSha256: digestSchema.nullable(),
    productAdmission: z.literal("not_established_on_this_surface"),
  }),
])

export const researchCollectionSchema = z.strictObject({
  collectionToken: z.string().uuid(),
  title: z.string().min(1),
  rowCount: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
})

const researchCollectionPageSchema = z.strictObject({
  items: z.array(researchCollectionSchema),
  hasMore: z.boolean(),
  nextCollection: z.string().uuid().nullable(),
})

export const researchJobSchema = z
  .object({
    jobId: z.string().min(1),
    generation: canonicalPositiveU64Schema,
    sequence: canonicalUnsignedU64Schema,
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

const researchActivitySchema = z
  .strictObject({
    activityToken: z.string().uuid(),
    label: z.string().min(1).max(128),
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
    completedUnits: z.number().int().nonnegative().nullable(),
    totalUnits: z.number().int().nonnegative().nullable(),
    cancellationRequested: z.boolean(),
    updatedAt: losslessIntegerSchema,
    canCancel: z.boolean(),
    canRetry: z.boolean(),
  })
  .superRefine((activity, context) => {
    if (
      (activity.completedUnits === null) !== (activity.totalUnits === null) ||
      (activity.completedUnits !== null &&
        activity.totalUnits !== null &&
        activity.completedUnits > activity.totalUnits)
    ) {
      context.addIssue({
        code: "custom",
        message: "The research activity progress is inconsistent.",
      })
    }
    if (activity.canCancel && activity.canRetry) {
      context.addIssue({
        code: "custom",
        message: "The research activity actions are inconsistent.",
      })
    }
  })

const researchActivityPageSchema = z.strictObject({
  activities: z.array(researchActivitySchema).max(25),
})

const researchActionAcceptedSchema = z.strictObject({
  accepted: z.literal(true),
})

const researchObservationScalarSchema = z.union([
  z.string(),
  z.number().finite(),
  z.null(),
])

export const researchObservationSchema = z.strictObject({
  revision: researchObservationScalarSchema.optional(),
  quality: researchObservationScalarSchema.optional(),
  effectiveAt: researchObservationScalarSchema.optional(),
  publishedAt: researchObservationScalarSchema.optional(),
  availableAt: researchObservationScalarSchema.optional(),
  supersededAt: researchObservationScalarSchema.optional(),
})

const inlineObservationResultSchema = z.strictObject({
  kind: z.literal("inline"),
  rows: z.array(researchObservationSchema),
})

const artifactObservationResultSchema = z.strictObject({
  kind: z.literal("artifact"),
  rowCount: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
})

const researchJobReceiptSchema = z.strictObject({
  jobId: z.string().min(1),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  state: z.literal("queued"),
})

const researchFilePreviewCellSchema = z.strictObject({
  kind: z.enum(["text", "null", "unsupported", "missing"]),
  value: z.string().max(256).nullable(),
  truncated: z.boolean(),
})

const researchFilePreviewSchema = z.strictObject({
  previewId: digestSchema,
  sha256: digestSchema,
  format: z.enum(["csv", "json", "ndjson", "parquet"]),
  rowCount: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  columns: z
    .array(
      z.strictObject({
        name: z.string().min(1).max(256),
        kind: z.enum([
          "exact_decimal",
          "text",
          "mixed",
          "unsupported",
          "null",
        ]),
        nullable: z.boolean(),
      }),
    )
    .min(1)
    .max(256),
  sampleRows: z
    .array(z.array(researchFilePreviewCellSchema).max(256))
    .max(20),
})

const researchFileDiscardSchema = z.strictObject({
  previewId: digestSchema,
  status: z.literal("discarded"),
})

export type ResearchDataset = z.infer<typeof researchDatasetSchema>
export type ResearchCollection = z.infer<typeof researchCollectionSchema>
export type ResearchObservation = z.infer<typeof researchObservationSchema>
export type ResearchJob = z.infer<typeof researchJobSchema>
export type ResearchActivity = z.infer<typeof researchActivitySchema>
export type ResearchManifest = z.infer<typeof researchManifestSchema>
export type ResearchJobReceipt = z.infer<typeof researchJobReceiptSchema>
export type ResearchFilePreview = z.infer<typeof researchFilePreviewSchema>

export type ResearchObservationResult =
  | {
      kind: "empty"
      returnedItems: number
      completeness: string
    }
  | {
      kind: "inline"
      rows: ResearchObservation[]
      returnedItems: number
      completeness: string
    }
  | {
      kind: "artifact"
      rowCount: number
      returnedItems: number
      completeness: string
    }

export interface ResearchCollectionPage {
  items: ResearchCollection[]
  hasMore: boolean
  nextCollection: string | null
  completeness: string
}

export function parseResearchCollectionPage(
  result: ApplicationResult,
): ResearchCollectionPage {
  if (result.data === null) {
    validateReturnedItems(result, 0, "research collection")
    return {
      items: [],
      hasMore: false,
      nextCollection: null,
      completeness: result.metadata.completeness,
    }
  }
  const page = researchCollectionPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported research collection response.",
    )
  }
  if (page.data.hasMore && page.data.nextCollection === null) {
    throw new Error("The research collection continuation is incomplete.")
  }
  validateReturnedItems(result, page.data.items.length, "research collection")
  return { ...page.data, completeness: result.metadata.completeness }
}

export function parseResearchCollection(
  result: ApplicationResult,
  expectedCollection: string,
): ResearchCollection {
  const collection = researchCollectionSchema.safeParse(result.data)
  if (
    !collection.success ||
    collection.data.collectionToken !== expectedCollection
  ) {
    throw new Error(
      "The installed service returned an unsupported research collection response.",
    )
  }
  validateReturnedItems(result, 1, "research collection")
  return collection.data
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
      job.kind === "analysis.phase-one-feature-derived-generation-job.v1",
  )
}

export function parseResearchActivities(
  result: ApplicationResult,
): ResearchActivity[] {
  const page = researchActivityPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned unsupported research activity.",
    )
  }
  validateReturnedItems(result, page.data.activities.length, "research activity")
  return page.data.activities
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
  if (inline.success && inline.data.rows.length === result.metadata.returnedItems) {
    validateReturnedItems(result, inline.data.rows.length, "research history")
    return { ...inline.data, ...common }
  }

  const artifact = artifactObservationResultSchema.safeParse(result.data)
  if (artifact.success && artifact.data.rowCount === result.metadata.returnedItems) {
    validateReturnedItems(result, artifact.data.rowCount, "research history")
    return { ...artifact.data, ...common }
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

export function parseResearchActionAccepted(result: ApplicationResult): void {
  const accepted = researchActionAcceptedSchema.safeParse(result.data)
  if (!accepted.success) {
    throw new Error(
      "The installed service did not accept the requested research action.",
    )
  }
  validateReturnedItems(result, 1, "research action")
}

export function parseResearchFilePreview(value: unknown): ResearchFilePreview {
  const parsed = applicationResultSchema
    .extend({ data: researchFilePreviewSchema })
    .safeParse(value)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned a research-file preview this dashboard cannot safely interpret.",
    )
  }
  const preview = parsed.data.data
  const names = new Set(preview.columns.map((column) => column.name))
  const invalidCell = preview.sampleRows.some(
    (row) =>
      row.length !== preview.columns.length ||
      row.some(
        (cell) =>
          (cell.kind === "text" && cell.value === null) ||
          (cell.kind !== "text" && cell.value !== null) ||
          (cell.kind !== "text" && cell.truncated),
      ),
  )
  if (
    names.size !== preview.columns.length ||
    preview.sampleRows.length > preview.rowCount ||
    invalidCell
  ) {
    throw new Error("The research-file preview is internally inconsistent.")
  }
  validateReturnedItems(parsed.data, 1, "research-file preview")
  return preview
}

export function parseResearchFileCommit(value: unknown): ResearchJobReceipt {
  const parsed = applicationResultSchema.safeParse(value)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported research-file job receipt.",
    )
  }
  return parseResearchJobReceipt(parsed.data)
}

export function parseResearchFileDiscard(
  value: unknown,
  expectedPreviewId: string,
): void {
  const parsed = applicationResultSchema
    .extend({ data: researchFileDiscardSchema })
    .safeParse(value)
  if (!parsed.success || parsed.data.data.previewId !== expectedPreviewId) {
    throw new Error(
      "The installed service returned a discard receipt for a different research-file preview.",
    )
  }
  validateReturnedItems(parsed.data, 1, "research-file discard receipt")
}

export function validateReturnedItems(
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
