import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const U64_MAX = 18_446_744_073_709_551_615n
const unsignedU64Schema = losslessIntegerSchema.refine(
  (value) => {
    const parsed = BigInt(value)
    return parsed >= 0n && parsed <= U64_MAX
  },
  { message: "Expected an unsigned 64-bit integer" },
)
const positiveU64Schema = unsignedU64Schema.refine(
  (value) => BigInt(value) > 0n,
  { message: "Expected a positive 64-bit integer" },
)

const jobStateSchema = z.enum([
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
])

const evidenceDigestSchema = z.object({
  algorithm: z.literal("sha256"),
  bytes: z.array(z.number().int().min(0).max(255)).length(32),
})

const artifactDigestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const artifactIdSchema = z
  .string()
  .max(160)
  .regex(/^[A-Za-z0-9][A-Za-z0-9_-]*$/)
const artifactMediaTypeSchema = z
  .string()
  .max(128)
  .regex(/^[A-Za-z0-9.+/-]+$/)

const jobArtifactSchema = z
  .object({
    id: artifactIdSchema,
    sha256: artifactDigestSchema,
    byteCount: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    mediaType: artifactMediaTypeSchema,
  })
  .strict()

const jobFailureSchema = z.object({
  class: z.string().min(1),
  diagnostic: z.string().min(1),
  retryable: z.boolean(),
})

const jobResultSchema = z.object({
  authority: z.string().min(1),
  identity: z.string().min(1),
  evidenceDigest: evidenceDigestSchema,
  artifacts: z.array(jobArtifactSchema).max(64),
})

const artifactReadSchema = z
  .object({
    artifact: z
      .object({
        artifactId: artifactIdSchema,
        sha256: artifactDigestSchema,
        byteCount: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
        mediaType: artifactMediaTypeSchema,
      })
      .strict(),
    offset: z.number().int().nonnegative(),
    returnedBytes: z.number().int().nonnegative(),
    contentBase64: z.string(),
    nextOffset: z.number().int().nonnegative(),
    complete: z.boolean(),
  })
  .strict()

export const jobViewSchema = z.object({
  jobId: z.string().uuid(),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  kind: z.string().min(1),
  state: jobStateSchema,
  phase: z.string().min(1).nullable(),
  completedUnits: z.number().int().nonnegative().nullable(),
  totalUnits: z.number().int().nonnegative().nullable(),
  cancellationRequested: z.boolean(),
  result: jobResultSchema.nullable(),
  failure: jobFailureSchema.nullable(),
  updatedAt: losslessIntegerSchema,
  recovery: z.string().min(1).nullable(),
})

const jobPageSchema = z.object({
  jobs: z.array(jobViewSchema).max(1_024),
  next: z.string().min(1).nullable(),
})

const confirmationSchema = z.object({
  identity: z.string().min(1),
  digest: evidenceDigestSchema,
  expiresAt: losslessIntegerSchema,
})

const jobEventSchema = z.object({
  state: jobStateSchema,
  occurredAt: losslessIntegerSchema,
  progress: z.unknown().nullable(),
  confirmation: confirmationSchema.nullable(),
  result: z.unknown().nullable(),
  failure: z.unknown().nullable(),
})

const jobEventPageSchema = z.object({
  events: z
    .array(z.tuple([z.number().int().nonnegative(), jobEventSchema]))
    .max(4_096),
  next: z.number().int().nonnegative().nullable(),
})

const runtimeStatusSchema = z
  .object({
    ready: z.literal(true),
    workspace: z
      .object({
        workspaceId: z.string().uuid(),
        generation: positiveU64Schema,
      })
      .strict(),
    workspaceSchemaVersion: z.number().int().positive().max(0xffff_ffff),
    availableDiskBytes: unsignedU64Schema,
    runningJobs: z.number().int().nonnegative().max(0xffff_ffff),
    runningMutationJobs: z.number().int().nonnegative().max(0xffff_ffff),
    activeSources: z.number().int().nonnegative().max(0xffff_ffff),
    connectedClients: z.number().int().nonnegative().max(0xffff_ffff),
    paperExecutionActive: z.boolean(),
    executionReconciliationPending: z.boolean(),
  })
  .strict()
  .refine((status) => status.runningMutationJobs <= status.runningJobs, {
    message: "Mutation jobs cannot exceed total running jobs",
    path: ["runningMutationJobs"],
  })

export type JobView = z.infer<typeof jobViewSchema>
export type JobState = z.infer<typeof jobStateSchema>
export type JobArtifact = z.infer<typeof jobArtifactSchema>
export type PreviewableArtifactMediaType =
  | "application/json"
  | "application/x-ndjson"
export type JobConfirmationEvidence = z.infer<typeof confirmationSchema>
export type PendingJobAction =
  | { kind: "cancel"; job: JobView }
  | { kind: "retry"; job: JobView }
  | {
      kind: "confirm"
      job: JobView
      confirmation: JobConfirmationEvidence
    }
export type RuntimeStatus = z.infer<typeof runtimeStatusSchema>

export function parseJobPage(result: ApplicationResult) {
  return jobPageSchema.parse(result.data)
}

export function parseRuntimeStatus(result: ApplicationResult): RuntimeStatus {
  return runtimeStatusSchema.parse(result.data)
}

export function parseCurrentConfirmation(
  result: ApplicationResult,
  expectedSequence: number,
): JobConfirmationEvidence | null {
  const page = jobEventPageSchema.parse(result.data)
  const current = page.events.find(
    ([sequence, event]) =>
      sequence === expectedSequence && event.state === "awaiting_confirmation",
  )
  return current?.[1].confirmation ?? null
}

export function previewableMediaType(
  artifact: JobArtifact,
): PreviewableArtifactMediaType | null {
  switch (artifact.mediaType) {
    case "application/json":
    case "application/x-ndjson":
      return artifact.mediaType
    default:
      return null
  }
}

export function parseArtifactChunk(
  result: ApplicationResult,
  requested: JobArtifact,
  requestedOffset: number,
  maximumBytes: number,
): {
  contentBase64: string
  returnedBytes: number
  nextOffset: number
  complete: boolean
} {
  const parsed = artifactReadSchema.parse(result.data)
  if (
    parsed.artifact.artifactId !== requested.id ||
    parsed.artifact.sha256 !== requested.sha256 ||
    parsed.artifact.byteCount !== requested.byteCount ||
    parsed.artifact.mediaType !== requested.mediaType ||
    parsed.offset !== requestedOffset ||
    parsed.returnedBytes > maximumBytes ||
    parsed.nextOffset !== requestedOffset + parsed.returnedBytes ||
    parsed.complete !== (parsed.nextOffset === requested.byteCount)
  ) {
    throw new Error("The service returned artifact evidence that does not match this job.")
  }
  if (base64ByteLength(parsed.contentBase64) !== parsed.returnedBytes) {
    throw new Error("The service returned an artifact chunk with an invalid byte count.")
  }
  return {
    contentBase64: parsed.contentBase64,
    returnedBytes: parsed.returnedBytes,
    nextOffset: parsed.nextOffset,
    complete: parsed.complete,
  }
}

function base64ByteLength(value: string): number | null {
  if (
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      value,
    )
  ) {
    return null
  }
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0
  return (value.length / 4) * 3 - padding
}

export function digestHex(digest: JobConfirmationEvidence["digest"]): string {
  return digest.bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

export function isActiveJob(state: JobState): boolean {
  return !["completed", "failed", "cancelled", "interrupted"].includes(state)
}

export function canCancel(job: JobView): boolean {
  return (
    !job.cancellationRequested &&
    [
      "queued",
      "preparing",
      "running",
      "awaiting_confirmation",
      "recovering",
    ].includes(job.state)
  )
}

export function canRetry(job: JobView): boolean {
  return job.state === "failed" && job.failure?.retryable === true
}
