import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

const candidateSchema = z.object({
  version: z.string().min(1),
  trustedMetadataSha256: digestSchema,
  manifestSha256: digestSchema,
  bundleSha256: digestSchema,
  bundleBytes: losslessIntegerSchema,
  minimumSchemaVersion: z.number().int().positive(),
  maximumSchemaVersion: z.number().int().positive(),
})

const activitySchema = z.object({
  schemaVersion: z.number().int().positive(),
  availableDiskBytes: losslessIntegerSchema,
  requiredDiskBytes: losslessIntegerSchema,
  runningMutationJobs: z.number().int().nonnegative(),
  paperExecutionActive: z.boolean(),
  executionReconciliationPending: z.boolean(),
})

const previewEnvelopeSchema = z.object({
  previewId: z.string().uuid(),
  previewDigest: digestSchema,
  expiresAt: losslessIntegerSchema,
  evidence: z.unknown(),
})

const updatePreviewSchema = previewEnvelopeSchema.extend({
  evidence: z.object({
    currentGeneration: losslessIntegerSchema,
    candidate: candidateSchema,
    activity: activitySchema,
    canApprove: z.boolean(),
    previewSha256: digestSchema,
  }),
})

const rollbackPreviewSchema = previewEnvelopeSchema.extend({
  evidence: z.object({
    currentGeneration: losslessIntegerSchema,
    targetVersion: z.string().min(1),
    activeWorkBlocked: z.boolean(),
    knownGoodVerified: z.boolean(),
  }),
})

const updateStatusSchema = z.object({
  availability: z.enum([
    "available",
    "source_or_development_execution",
    "production_signing_material_unavailable",
  ]),
  currentGeneration: losslessIntegerSchema,
  knownGoodVersion: z.string().min(1),
  stagedCandidate: candidateSchema.nullable(),
  lastCheckedAt: losslessIntegerSchema.nullable(),
  recoveryRequired: z.boolean(),
})

const jobReceiptSchema = z.object({
  jobId: z.string().uuid(),
  generation: losslessIntegerSchema,
  sequence: losslessIntegerSchema,
  state: z.literal("queued"),
})

export type UpdateStatus = z.infer<typeof updateStatusSchema>
export type UpdatePreview = z.infer<typeof updatePreviewSchema>
export type ProgramRollbackPreview = z.infer<typeof rollbackPreviewSchema>
export type LifecycleJobReceipt = z.infer<typeof jobReceiptSchema>

export function parseUpdateStatus(result: ApplicationResult): UpdateStatus {
  return updateStatusSchema.parse(result.data)
}

export function parseUpdatePreview(result: ApplicationResult): UpdatePreview {
  return updatePreviewSchema.parse(result.data)
}

export function parseProgramRollbackPreview(
  result: ApplicationResult,
): ProgramRollbackPreview {
  return rollbackPreviewSchema.parse(result.data)
}

export function parseLifecycleJobReceipt(
  result: ApplicationResult,
): LifecycleJobReceipt {
  return jobReceiptSchema.parse(result.data)
}
