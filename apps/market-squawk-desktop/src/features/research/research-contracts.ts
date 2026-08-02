import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

export const researchManifestSchema = z.object({
  datasetId: z.string().min(1),
  manifestVersion: z.number().int().nonnegative(),
  schema: z.object({
    name: z.string().min(1),
    version: z.number().int().positive(),
    fingerprint: digestSchema,
  }),
  contentHash: digestSchema,
})

export const researchDatasetSchema = z.object({
  manifest: researchManifestSchema,
  sourceId: z.string().min(1),
  generationKind: z.enum(["ingest", "compaction", "derived"]),
  buildSpecDigest: digestSchema.nullable(),
  pythonExportSha256: digestSchema.nullable(),
  parents: z.array(
    z.object({
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

const researchDatasetPageSchema = z.object({
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
    updatedAt: losslessIntegerSchema,
    recovery: z.string().nullable(),
  })
  .loose()

const researchJobPageSchema = z.object({
  jobs: z.array(researchJobSchema),
  next: z.unknown().nullable(),
})

const observationArtifactSchema = z.object({
  artifactId: z.string().min(1),
  sha256: digestSchema,
  byteCount: z.number().int().nonnegative(),
  mediaType: z.string().min(1),
  rowCount: z.number().int().nonnegative(),
})

const inlineObservationResultSchema = z.object({
  manifest: researchManifestSchema,
  arrowIpcBytes: z.number().int().nonnegative(),
  rows: z.array(z.record(z.string(), z.unknown())),
})

const artifactObservationResultSchema = z.object({
  manifest: researchManifestSchema,
  artifact: observationArtifactSchema,
})

const researchJobReceiptSchema = z.object({
  jobId: z.string().min(1),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  state: z.literal("queued"),
})

export type ResearchDataset = z.infer<typeof researchDatasetSchema>
export type ResearchJob = z.infer<typeof researchJobSchema>
export type ResearchManifest = z.infer<typeof researchManifestSchema>
export type ResearchJobReceipt = z.infer<typeof researchJobReceiptSchema>

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
  return { ...page.data, completeness: result.metadata.completeness }
}

export function parseResearchJobs(result: ApplicationResult): ResearchJob[] {
  const page = researchJobPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported background-work response.",
    )
  }
  return page.data.jobs.filter((job) => job.kind.startsWith("research."))
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
    if (result.metadata.returnedItems !== 0) {
      throw new Error("The research-history count does not match its empty result.")
    }
    return { kind: "empty", ...common }
  }

  const inline = inlineObservationResultSchema.safeParse(result.data)
  if (
    inline.success &&
    inline.data.manifest.datasetId === expectedDataset &&
    inline.data.rows.length === result.metadata.returnedItems
  ) {
    return { kind: "inline", ...inline.data, ...common }
  }

  const artifact = artifactObservationResultSchema.safeParse(result.data)
  if (
    artifact.success &&
    artifact.data.manifest.datasetId === expectedDataset &&
    artifact.data.artifact.rowCount === result.metadata.returnedItems
  ) {
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
  return receipt.data
}
