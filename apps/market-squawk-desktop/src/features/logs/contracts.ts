import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema, type LosslessInteger } from "@/lib/lossless-integer"
import type {
  OperationLogDomain,
  OperationLogFilter,
  OperationLogSeverity,
} from "@/lib/transport"

export const LOG_PAGE_LIMIT = 100
export const MAXIMUM_LOG_PAGE_LIMIT = 1_000
export const MAXIMUM_LOG_PAGES = 10

export const logSeverityOptions: readonly OperationLogSeverity[] = [
  "trace",
  "debug",
  "info",
  "warn",
  "error",
]

export const logDomainOptions: readonly OperationLogDomain[] = [
  "application",
  "source",
  "market",
  "research",
  "portfolio",
  "model",
  "backtest",
  "execution",
  "risk",
  "fair_value",
  "mcp",
  "lifecycle",
]

const structuredLogRecordSchema = z.object({
  sequence: losslessIntegerSchema,
  event: z.object({
    observedAt: losslessIntegerSchema,
    severity: z.enum(logSeverityOptions),
    domain: z.enum(logDomainOptions),
    operation: z.string().min(1).nullable(),
    sourceId: z.string().min(1).nullable(),
    jobId: z.string().min(1).nullable(),
    correlationId: z.string().min(1).nullable(),
    message: z.string().min(1),
    fields: z.record(z.string(), z.string()),
  }),
})

const structuredLogPageSchema = z.object({
  records: z.array(structuredLogRecordSchema).max(MAXIMUM_LOG_PAGE_LIMIT),
  nextAfterSequence: losslessIntegerSchema.nullable(),
})

const diagnosticArtifactReceiptSchema = z.object({
  artifactReference: z.string().min(1),
  byteLength: losslessIntegerSchema,
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
})

export type StructuredLogRecord = z.infer<typeof structuredLogRecordSchema>
export type StructuredLogPage = z.infer<typeof structuredLogPageSchema>
export type DiagnosticArtifactReceipt = z.infer<
  typeof diagnosticArtifactReceiptSchema
>

export type LogFilterDraft = {
  fromLocal: string
  throughLocal: string
  minimumSeverity: "" | OperationLogSeverity
  domain: "" | OperationLogDomain
  sourceId: string
  jobId: string
  correlationId: string
  search: string
  limit: string
}

export const defaultLogFilterDraft: LogFilterDraft = {
  fromLocal: "",
  throughLocal: "",
  minimumSeverity: "",
  domain: "",
  sourceId: "",
  jobId: "",
  correlationId: "",
  search: "",
  limit: String(LOG_PAGE_LIMIT),
}

export function parseStructuredLogPage(result: ApplicationResult) {
  return structuredLogPageSchema.parse(result.data)
}

export function parseDiagnosticArtifactReceipt(result: ApplicationResult) {
  return diagnosticArtifactReceiptSchema.parse(result.data)
}

/**
 * Converts a `datetime-local` value to a signed Unix-nanosecond decimal string.
 * The conversion keeps all entered fractional-second digits; no timestamp travels
 * through a JavaScript Number after local calendar validation.
 */
export function localDateTimeToUnixNanos(value: string): string | null {
  const match = /^(\d{4,})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?$/.exec(
    value,
  )
  if (!match) return null

  const [, yearText, monthText, dayText, hourText, minuteText, secondText, fractionText] =
    match
  const year = Number(yearText)
  const month = Number(monthText)
  const day = Number(dayText)
  const hour = Number(hourText)
  const minute = Number(minuteText)
  const second = Number(secondText ?? "0")
  if (
    !Number.isSafeInteger(year) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return null
  }

  const fractionNanos = BigInt((fractionText ?? "").padEnd(9, "0") || "0")
  const millisecond = Number(fractionNanos / 1_000_000n)
  const local = new Date(0)
  local.setFullYear(year, month - 1, day)
  local.setHours(hour, minute, second, millisecond)

  // A nonexistent local wall-clock time (for example during a DST jump) is not
  // silently normalized into a different instant.
  if (
    local.getFullYear() !== year ||
    local.getMonth() !== month - 1 ||
    local.getDate() !== day ||
    local.getHours() !== hour ||
    local.getMinutes() !== minute ||
    local.getSeconds() !== second ||
    local.getMilliseconds() !== millisecond
  ) {
    return null
  }

  return (
    BigInt(local.getTime()) * 1_000_000n + (fractionNanos % 1_000_000n)
  ).toString()
}

export function filterFromDraft(
  draft: LogFilterDraft,
): { filter: OperationLogFilter; error: string | null } {
  const limit = Number(draft.limit)
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAXIMUM_LOG_PAGE_LIMIT) {
    return {
      filter: { limit: LOG_PAGE_LIMIT },
      error: `Result limit must be a whole number from 1 to ${MAXIMUM_LOG_PAGE_LIMIT.toLocaleString()}.`,
    }
  }

  const fromUnixNanos = draft.fromLocal
    ? localDateTimeToUnixNanos(draft.fromLocal) ?? undefined
    : undefined
  const throughUnixNanos = draft.throughLocal
    ? localDateTimeToUnixNanos(draft.throughLocal) ?? undefined
    : undefined
  if ((draft.fromLocal && !fromUnixNanos) || (draft.throughLocal && !throughUnixNanos)) {
    return {
      filter: { limit },
      error: "Enter a valid local date and time. Times that do not exist locally cannot be queried.",
    }
  }
  if (fromUnixNanos && throughUnixNanos && BigInt(fromUnixNanos) > BigInt(throughUnixNanos)) {
    return {
      filter: { limit },
      error: "The start time must be at or before the end time.",
    }
  }

  const textFields = [
    ["Source ID", draft.sourceId],
    ["Job ID", draft.jobId],
    ["Correlation ID", draft.correlationId],
    ["Search", draft.search],
  ] as const
  const invalid = textFields.find(([, value]) => {
    const trimmed = value.trim()
    return trimmed.length > 256 || /[\u0000-\u001F\u007F]/.test(trimmed)
  })
  if (invalid) {
    return {
      filter: { limit },
      error: `${invalid[0]} must be at most 256 characters and cannot contain control characters.`,
    }
  }

  return {
    filter: {
      fromUnixNanos,
      throughUnixNanos,
      minimumSeverity: draft.minimumSeverity || undefined,
      domain: draft.domain || undefined,
      sourceId: optionalTrimmed(draft.sourceId),
      jobId: optionalTrimmed(draft.jobId),
      correlationId: optionalTrimmed(draft.correlationId),
      search: optionalTrimmed(draft.search),
      limit,
    },
    error: null,
  }
}

/** Preserves a service pagination cursor as an exact unsigned decimal string. */
export function asUnsignedCursor(value: LosslessInteger | null): string | null {
  if (value === null) return null
  const parsed = BigInt(value)
  if (parsed < 0n) return null
  return parsed.toString()
}

function optionalTrimmed(value: string) {
  const trimmed = value.trim()
  return trimmed || undefined
}
