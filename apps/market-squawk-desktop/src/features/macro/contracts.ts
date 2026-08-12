import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

export const H15_DASHBOARD_SCHEMA_IDENTITY =
  "market-squawk-macro-dashboard/v1" as const
export const H15_SURFACE_ID =
  "federal-reserve-board.data-download-program" as const
export const H15_SOURCE_ID = "federal-reserve-board-ddp" as const
export const H15_RELEASE_CODE = "H15" as const
export const H15_DATASET_FAMILY =
  "h15-treasury-constant-maturities" as const
export const H15_SERIES_COUNT = 11 as const
export const H15_SLOTS = [
  "1m",
  "3m",
  "6m",
  "1y",
  "2y",
  "3y",
  "5y",
  "7y",
  "10y",
  "20y",
  "30y",
] as const

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const analyticalDatasetIdSchema = z
  .string()
  .min(1)
  .max(256)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/)
const exactDecimalSchema = z
  .string()
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/)
const timestampSchema = z.string().datetime({ offset: true })
const calendarDateSchema = z
  .string()
  .regex(/^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$/)
const positiveLosslessIntegerSchema = losslessIntegerSchema.refine(
  (value) => BigInt(value) > 0n,
  "Expected a positive manifest version.",
)

const manifestSchema = z
  .object({
    datasetId: analyticalDatasetIdSchema,
    manifestVersion: positiveLosslessIntegerSchema,
    schema: z
      .object({
        name: z.string().min(1).max(256),
        version: z.number().int().positive(),
        fingerprint: digestSchema,
      })
      .strict(),
    contentHash: digestSchema,
  })
  .strict()

const h15SlotSchema = z.enum(H15_SLOTS)

const observedValueSchema = z
  .object({
    state: z.literal("observed"),
    decimal: exactDecimalSchema,
  })
  .strict()

const missingValueSchema = z
  .object({
    state: z.literal("missing"),
    marker: z.string().min(1).max(128),
    reason: z.string().min(1).max(512).nullable(),
  })
  .strict()

const observationSchema = z
  .object({
    slot: h15SlotSchema,
    label: z.string().min(1).max(256),
    maturityOrder: z.number().int().min(1).max(H15_SERIES_COUNT),
    seriesId: z.string().min(1).max(512),
    unitId: z.string().min(1).max(512),
    unitPresentation: z.literal("percent_per_year"),
    effectiveDate: calendarDateSchema,
    availableAt: timestampSchema,
    revision: z.number().int().positive().max(4_294_967_295),
    observation: z.discriminatedUnion("state", [
      observedValueSchema,
      missingValueSchema,
    ]),
    sourceIdentifier: z.string().min(1).max(512),
    sourcePayloadDigest: digestSchema,
  })
  .strict()

export const macroDashboardSchema = z
  .object({
    schemaIdentity: z.literal(H15_DASHBOARD_SCHEMA_IDENTITY),
    binding: z
      .object({
        surfaceId: z.literal(H15_SURFACE_ID),
        sourceId: z.literal(H15_SOURCE_ID),
        providerDatasetId: z.string().min(1).max(512),
        analyticalDatasetId: analyticalDatasetIdSchema,
        manifest: manifestSchema,
        objectGraphDigest: digestSchema,
        queryIdentity: digestSchema,
        resultDigest: digestSchema,
      })
      .strict(),
    release: z
      .object({
        code: z.literal(H15_RELEASE_CODE),
        title: z.literal("H.15 Selected Interest Rates"),
        family: z.literal(H15_DATASET_FAMILY),
        frequency: z.literal("business_daily"),
        quality: z.literal("official_delayed"),
      })
      .strict(),
    selection: z
      .object({
        policy: z.literal("latest_known_by_series_as_of_cutoff_v1"),
        evaluatedAt: timestampSchema,
        selectionDigest: digestSchema,
        returnedSeries: z.literal(H15_SERIES_COUNT),
        availableSeries: z.number().int().min(0).max(H15_SERIES_COUNT),
        missingSeries: z.number().int().min(0).max(H15_SERIES_COUNT),
        complete: z.literal(true),
      })
      .strict(),
    observations: z.array(observationSchema).length(H15_SERIES_COUNT),
  })
  .strict()
  .superRefine((dashboard, context) => {
    if (
      dashboard.binding.manifest.datasetId !==
      dashboard.binding.analyticalDatasetId
    ) {
      context.addIssue({
        code: "custom",
        path: ["binding", "manifest", "datasetId"],
        message: "Manifest and analytical dataset identities must match.",
      })
    }

    const slots = new Set<string>()
    const series = new Set<string>()
    let observed = 0
    let missing = 0
    dashboard.observations.forEach((observation, index) => {
      if (slots.has(observation.slot) || observation.slot !== H15_SLOTS[index]) {
        context.addIssue({
          code: "custom",
          path: ["observations", index, "slot"],
          message: "Every H.15 maturity slot must appear exactly once in contract order.",
        })
      }
      slots.add(observation.slot)

      if (series.has(observation.seriesId)) {
        context.addIssue({
          code: "custom",
          path: ["observations", index, "seriesId"],
          message: "Every H.15 maturity slot must bind a distinct series identity.",
        })
      }
      series.add(observation.seriesId)

      if (observation.maturityOrder !== index + 1) {
        context.addIssue({
          code: "custom",
          path: ["observations", index, "maturityOrder"],
          message: "H.15 observations must retain server-supplied maturity order.",
        })
      }

      if (observation.observation.state === "observed") observed += 1
      else missing += 1
    })

    if (
      dashboard.selection.availableSeries !== observed ||
      dashboard.selection.missingSeries !== missing ||
      dashboard.selection.returnedSeries !== dashboard.observations.length
    ) {
      context.addIssue({
        code: "custom",
        path: ["selection"],
        message: "H.15 selection counts do not match the returned typed slots.",
      })
    }
  })

export type MacroDashboard = z.infer<typeof macroDashboardSchema>
export type MacroDashboardObservation = MacroDashboard["observations"][number]
export type MacroDashboardSourceReadiness =
  | {
      state: "active"
      label: string
      detail: string
      lifecycleObservedAt: string | null
      runtimeObservedAt: string | null
    }
  | {
      state: "inactive" | "blocked" | "unavailable" | "unknown"
      label: string
      detail: string
      lifecycleObservedAt: string | null
      runtimeObservedAt: string | null
    }

export function parseMacroDashboard(
  result: ApplicationResult,
): MacroDashboard {
  const dashboard = macroDashboardSchema.safeParse(result.data)
  if (!dashboard.success) {
    throw new Error(
      "The installed service returned an unsupported H.15 macro-dashboard response.",
    )
  }

  if (
    result.metadata.completeness !== "complete" ||
    result.metadata.returnedItems !== H15_SERIES_COUNT ||
    result.metadata.availableItems !== H15_SERIES_COUNT
  ) {
    throw new Error(
      "The installed service returned incomplete H.15 macro-dashboard bounds.",
    )
  }

  return dashboard.data
}
