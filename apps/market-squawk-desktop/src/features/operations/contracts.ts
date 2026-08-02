import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

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

const jobFailureSchema = z.object({
  class: z.string().min(1),
  diagnostic: z.string().min(1),
  retryable: z.boolean(),
})

const jobResultSchema = z.object({
  authority: z.string().min(1),
  identity: z.string().min(1),
  evidenceDigest: evidenceDigestSchema,
  artifacts: z.array(z.unknown()).max(64),
})

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

export type JobView = z.infer<typeof jobViewSchema>
export type JobState = z.infer<typeof jobStateSchema>
export type JobConfirmationEvidence = z.infer<typeof confirmationSchema>
export type PendingJobAction =
  | { kind: "cancel"; job: JobView }
  | { kind: "retry"; job: JobView }
  | {
      kind: "confirm"
      job: JobView
      confirmation: JobConfirmationEvidence
    }

export function parseJobPage(result: ApplicationResult) {
  return jobPageSchema.parse(result.data)
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
