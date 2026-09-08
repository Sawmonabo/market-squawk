import { z } from "zod"

import { validateReturnedItems } from "@/features/research/research-contracts"
import {
  applicationResultSchema,
  type ApplicationResult,
} from "@/lib/schemas"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

const researchManifestSchema = z.strictObject({
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

const researchDatasetSchema = z.discriminatedUnion("generationKind", [
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
  sampleRows: z.array(z.array(researchFilePreviewCellSchema).max(256)).max(20),
})

const researchFileDiscardSchema = z.strictObject({
  previewId: digestSchema,
  status: z.literal("discarded"),
})

export type ResearchDataset = z.infer<typeof researchDatasetSchema>
export type ResearchJobReceipt = z.infer<typeof researchJobReceiptSchema>
export type ResearchFilePreview = z.infer<typeof researchFilePreviewSchema>

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
