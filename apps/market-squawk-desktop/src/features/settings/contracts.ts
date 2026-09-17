import { z } from "zod"

import { losslessIntegerSchema, type LosslessInteger } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"
import type { OperationSettingValue } from "@/lib/transport"

const digestSchema = z.string().regex(/^[a-f0-9]{64}$/)
const uuidSchema = z.string().uuid()

export const settingKeySchema = z.enum([
  "log_retention_days",
  "log_minimum_severity",
  "update_channel",
  "automatic_update_checks",
  "storage_soft_limit_bytes",
  "default_query_row_limit",
  "maximum_concurrent_jobs",
  "market_freshness_millis",
  "backup_retention_count",
])

export const restartImpactSchema = z.enum([
  "none",
  "service_reload",
  "service_restart",
])

const settingValueSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("log_retention_days"), value: z.number().int() }),
  z.object({ kind: z.literal("log_minimum_severity"), value: z.enum(["trace", "debug", "info", "warn", "error"]) }),
  z.object({ kind: z.literal("update_channel"), value: z.enum(["stable", "preview"]) }),
  z.object({ kind: z.literal("automatic_update_checks"), value: z.boolean() }),
  z.object({ kind: z.literal("storage_soft_limit_bytes"), value: losslessIntegerSchema }),
  z.object({ kind: z.literal("default_query_row_limit"), value: z.number().int() }),
  z.object({ kind: z.literal("maximum_concurrent_jobs"), value: z.number().int() }),
  z.object({ kind: z.literal("market_freshness_millis"), value: z.number().int().safe() }),
  z.object({ kind: z.literal("backup_retention_count"), value: z.number().int() }),
])

const settingEntrySchema = z.object({
  key: settingKeySchema,
  value: settingValueSchema,
  origin: z.enum([
    "safe_default",
    "local_persisted",
    "local_configuration",
    "environment",
    "cli_override",
    "managed_policy",
  ]),
  locallyMutable: z.boolean(),
  restartImpact: restartImpactSchema,
})

const settingsSnapshotSchema = z.object({
  revision: losslessIntegerSchema,
  entries: z.array(settingEntrySchema).length(9),
  digest: digestSchema,
})

const previewEnvelopeSchema = <Evidence extends z.ZodType>(evidence: Evidence) =>
  z.object({
    previewId: uuidSchema,
    previewDigest: digestSchema,
    expiresAt: losslessIntegerSchema,
    evidence,
  })

const settingsChangePreviewSchema = previewEnvelopeSchema(
  z.object({
    currentRevision: losslessIntegerSchema,
    changes: z.array(settingValueSchema).min(1).max(9),
    restartImpact: restartImpactSchema,
    previewSha256: digestSchema,
  }),
)

const settingsRollbackPreviewSchema = previewEnvelopeSchema(
  z.object({
    currentRevision: losslessIntegerSchema,
    targetRevision: losslessIntegerSchema,
    restartRequired: z.boolean(),
    digest: digestSchema,
  }),
)

const settingsReceiptSchema = z.object({
  previousRevision: losslessIntegerSchema,
  activeRevision: losslessIntegerSchema,
  activeDigest: digestSchema,
  restartImpact: restartImpactSchema,
  rolledBackFromRevision: losslessIntegerSchema.nullable(),
})

const workspaceDescriptorSchema = z.object({
  workspaceId: uuidSchema,
  displayName: z.string().min(1).max(128),
  schemaVersion: z.number().int().positive(),
  health: z.enum(["prepared", "healthy", "recovery_required"]),
  estimatedBytes: losslessIntegerSchema,
})

const workspacePageSchema = z.object({
  active: z.object({
    workspaceId: uuidSchema,
    generation: losslessIntegerSchema,
  }),
  workspaces: z.array(workspaceDescriptorSchema).max(64),
  nextAfterWorkspaceId: uuidSchema.nullable(),
})

const workspaceSwitchPreviewSchema = previewEnvelopeSchema(
  z.object({
    active: z.object({ workspaceId: uuidSchema, generation: losslessIntegerSchema }),
    target: uuidSchema,
    activity: z.object({
      runningJobs: z.number().int().nonnegative(),
      activeSources: z.number().int().nonnegative(),
      paperExecutionActive: z.boolean(),
      executionReconciliationPending: z.boolean(),
      connectedClients: z.number().int().nonnegative(),
      availableDiskBytes: losslessIntegerSchema,
      requiredDiskBytes: losslessIntegerSchema,
      schemaCompatible: z.boolean(),
    }),
    blockers: z.array(z.enum([
      "running_jobs",
      "active_sources",
      "paper_execution_active",
      "execution_reconciliation_pending",
      "insufficient_disk",
      "incompatible_schema",
    ])).max(6),
    previewSha256: digestSchema,
  }),
)

const jobReceiptSchema = z.object({
  jobId: uuidSchema,
  generation: losslessIntegerSchema,
  sequence: losslessIntegerSchema,
  state: z.literal("queued"),
})

export type SettingEntry = z.infer<typeof settingEntrySchema>
export type SettingKey = z.infer<typeof settingKeySchema>
export type SettingsSnapshot = z.infer<typeof settingsSnapshotSchema>
export type SettingsChangePreview = z.infer<typeof settingsChangePreviewSchema>
export type SettingsRollbackPreview = z.infer<typeof settingsRollbackPreviewSchema>
export type SettingsReceipt = z.infer<typeof settingsReceiptSchema>
export type WorkspacePage = z.infer<typeof workspacePageSchema>
export type WorkspaceDescriptor = z.infer<typeof workspaceDescriptorSchema>
export type WorkspaceSwitchPreview = z.infer<typeof workspaceSwitchPreviewSchema>
export type JobReceipt = z.infer<typeof jobReceiptSchema>

export function parseSettingsSnapshot(result: ApplicationResult): SettingsSnapshot {
  return settingsSnapshotSchema.parse(result.data)
}

export function parseSettingsChangePreview(result: ApplicationResult): SettingsChangePreview {
  return settingsChangePreviewSchema.parse(result.data)
}

export function parseSettingsRollbackPreview(result: ApplicationResult): SettingsRollbackPreview {
  return settingsRollbackPreviewSchema.parse(result.data)
}

export function parseSettingsReceipt(result: ApplicationResult): SettingsReceipt {
  return settingsReceiptSchema.parse(result.data)
}

export function parseWorkspacePage(result: ApplicationResult): WorkspacePage {
  return workspacePageSchema.parse(result.data)
}

export function parseWorkspaceSwitchPreview(result: ApplicationResult): WorkspaceSwitchPreview {
  return workspaceSwitchPreviewSchema.parse(result.data)
}

export function parseJobReceipt(result: ApplicationResult): JobReceipt {
  return jobReceiptSchema.parse(result.data)
}

export function formatBytes(value: LosslessInteger): string {
  const bytes = BigInt(value)
  const units = ["B", "KiB", "MiB", "GiB", "TiB"]
  let amount = bytes
  let unit = 0
  while (amount >= 1024n && unit < units.length - 1) {
    amount /= 1024n
    unit += 1
  }
  const label = units[unit] ?? "TiB"
  return `${amount.toLocaleString()} ${label} (${bytes.toLocaleString()} bytes)`
}

export function settingValueToText(entry: SettingEntry): string {
  return typeof entry.value.value === "boolean"
    ? String(entry.value.value)
    : String(entry.value.value)
}

export function isMatchingSetting(entry: SettingEntry): boolean {
  return entry.key === entry.value.kind
}

export function asOperationSettingValue(entry: SettingEntry, value: string): OperationSettingValue | null {
  switch (entry.key) {
    case "storage_soft_limit_bytes":
      if (!/^\d+$/.test(value)) return null
      return { kind: entry.key, value }
    case "log_retention_days":
    case "default_query_row_limit":
    case "maximum_concurrent_jobs":
    case "market_freshness_millis":
    case "backup_retention_count": {
      const parsed = Number(value)
      if (!Number.isSafeInteger(parsed)) return null
      return { kind: entry.key, value: parsed }
    }
    case "automatic_update_checks":
      return { kind: entry.key, value: value === "true" }
    case "log_minimum_severity":
      if (value === "trace" || value === "debug" || value === "info" || value === "warn" || value === "error") {
        return { kind: entry.key, value }
      }
      return null
    case "update_channel":
      return value === "stable" || value === "preview"
        ? { kind: entry.key, value }
        : null
  }
}
