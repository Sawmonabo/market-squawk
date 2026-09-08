import { z } from "zod"

import { losslessIntegerSchema, type LosslessInteger } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const sha256Hex = z.string().regex(/^[0-9a-f]{64}$/)
const timestamp = losslessIntegerSchema

const componentSchema = z.object({
  kind: z.string().min(1),
  producer: z.string().min(1),
  schema: z.object({
    identity: z.string().min(1),
    version: z.union([z.number().int().positive(), z.string().min(1)]),
  }),
  byteLength: losslessIntegerSchema,
  sensitivity: z.string().min(1),
})

const encryptionSchema = z.union([
  z.literal("unencrypted_no_secret_payload"),
  z.object({
    encrypted: z.object({
      scheme: z.string().min(1),
      key_reference_sha256: sha256Hex,
    }),
  }),
])

export const backupManifestSchema = z.object({
  formatVersion: z.number().int().positive(),
  backupId: sha256Hex,
  snapshot: z.object({
    cutoff: timestamp,
    snapshotId: sha256Hex,
  }),
  ownership: z.object({
    installationId: z.string().uuid(),
    workspaceId: z.string().uuid(),
  }),
  analyticalReceipt: z
    .object({
      artifactCount: losslessIntegerSchema,
      artifactBytes: losslessIntegerSchema,
      cutoff: timestamp,
    })
    .passthrough(),
  components: z.array(componentSchema).max(9),
  encryption: encryptionSchema,
  manifestSha256: sha256Hex,
})

const previewReferenceSchema = z.object({
  previewId: z.string().uuid(),
  previewDigest: sha256Hex,
  expiresAt: timestamp,
})

const retentionEvidenceSchema = z.object({
  revision: losslessIntegerSchema,
  keepLatest: z.number().int().min(1).max(128),
  deleteBackupIds: z.array(sha256Hex).max(128),
  previewSha256: sha256Hex,
})

const restoreEvidenceSchema = z.object({
  backup: backupManifestSchema,
  active: z.object({
    workspaceId: z.string().uuid(),
    generation: losslessIntegerSchema,
  }),
  availableDiskBytes: losslessIntegerSchema,
  requiredDiskBytes: losslessIntegerSchema,
  schemaCompatible: z.boolean(),
  blockers: z.array(z.string().min(1)).max(64),
})

export const backupInventorySchema = z.object({
  revision: losslessIntegerSchema,
  manifests: z.array(backupManifestSchema).max(64),
  nextAfterBackupId: sha256Hex.nullable(),
  pendingDeletions: z.number().int().nonnegative(),
})

export const retentionPreviewSchema = previewReferenceSchema.extend({
  evidence: retentionEvidenceSchema,
})

export const restorePreviewSchema = previewReferenceSchema.extend({
  evidence: restoreEvidenceSchema,
})

export const programRollbackPreviewSchema = previewReferenceSchema.extend({
  evidence: z.object({
    currentGeneration: losslessIntegerSchema,
    targetVersion: z.string().min(1),
    activeWorkBlocked: z.boolean(),
    knownGoodVerified: z.boolean(),
  }),
})

export const jobReceiptSchema = z.object({
  jobId: z.string().uuid(),
  generation: losslessIntegerSchema,
  sequence: losslessIntegerSchema,
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
})

export const jobPageSchema = z.object({
  jobs: z.array(
    jobReceiptSchema.extend({
      kind: z.string().min(1),
      phase: z.string().min(1).nullable(),
      completedUnits: losslessIntegerSchema.nullable(),
      totalUnits: losslessIntegerSchema.nullable(),
      cancellationRequested: z.boolean(),
      failure: z
        .object({
          class: z.string().min(1),
          diagnostic: z.string().min(1),
          retryable: z.boolean(),
        })
        .nullable(),
      updatedAt: timestamp,
      recovery: z.string().min(1).nullable(),
    }),
  ),
  next: z.string().nullable(),
})

export type BackupManifest = z.infer<typeof backupManifestSchema>
export type BackupInventory = z.infer<typeof backupInventorySchema>
export type RetentionPreview = z.infer<typeof retentionPreviewSchema>
export type RestorePreview = z.infer<typeof restorePreviewSchema>
export type ProgramRollbackPreview = z.infer<typeof programRollbackPreviewSchema>
export type BackupJobReceipt = z.infer<typeof jobReceiptSchema>
export type BackupJob = z.infer<typeof jobPageSchema>["jobs"][number]

export function parseBackupInventory(result: ApplicationResult): BackupInventory {
  return backupInventorySchema.parse(result.data)
}

export function parseRetentionPreview(result: ApplicationResult): RetentionPreview {
  return retentionPreviewSchema.parse(result.data)
}

export function parseRestorePreview(result: ApplicationResult): RestorePreview {
  return restorePreviewSchema.parse(result.data)
}

export function parseProgramRollbackPreview(
  result: ApplicationResult,
): ProgramRollbackPreview {
  return programRollbackPreviewSchema.parse(result.data)
}

export function parseBackupJobReceipt(result: ApplicationResult): BackupJobReceipt {
  return jobReceiptSchema.parse(result.data)
}

export function parseBackupJobs(result: ApplicationResult): BackupJob[] {
  return jobPageSchema.parse(result.data).jobs
}

export function shortBackupId(backupId: string): string {
  return `${backupId.slice(0, 12)}…${backupId.slice(-8)}`
}

export function formatBytes(value: LosslessInteger): string {
  const bytes = BigInt(value)
  if (bytes < 1_024n) return `${bytes.toLocaleString()} B`
  const units = ["KiB", "MiB", "GiB", "TiB"]
  let scaled = bytes
  let index = 0
  let divisor = 1_024n
  scaled /= divisor
  while (scaled >= 1_024n && index < units.length - 1) {
    divisor *= 1_024n
    scaled = bytes / divisor
    index += 1
  }
  const tenths = ((bytes % divisor) * 10n) / divisor
  return `${scaled.toLocaleString()}.${tenths.toString()} ${units[index]}`
}

export function formatSnapshotTime(value: LosslessInteger): string {
  const milliseconds = BigInt(value) / 1_000_000n
  const safeMilliseconds = Number(milliseconds)
  if (!Number.isSafeInteger(safeMilliseconds)) return String(value)
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(safeMilliseconds))
}

export function encryptionLabel(value: BackupManifest["encryption"]): string {
  return typeof value === "string"
    ? "Unencrypted; no secret payload"
    : `Encrypted (${value.encrypted.scheme})`
}
