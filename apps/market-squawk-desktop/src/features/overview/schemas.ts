import { z } from "zod"

const availableSectionSchema = z
  .object({
    state: z.string(),
    count: z.number().int().nonnegative(),
  })
  .loose()

export const overviewJobSchema = z
  .object({
    jobId: z.string(),
    kind: z.string(),
    state: z.string(),
    phase: z.string().nullable().optional(),
    completedUnits: z.number().nonnegative().nullable().optional(),
    totalUnits: z.number().nonnegative().nullable().optional(),
    updatedAt: z.unknown().optional(),
  })
  .loose()

export const decisionOverviewSchema = z.object({
  providers: availableSectionSchema.extend({ items: z.array(z.unknown()) }),
  datasets: availableSectionSchema.extend({ hasMore: z.boolean() }),
  screens: availableSectionSchema.extend({
    items: z.array(
      z.object({
        id: z.string(),
        revision: z.number().int().positive(),
        maximumResults: z.number().int().positive(),
      }),
    ),
  }),
  jobs: availableSectionSchema.extend({ items: z.array(overviewJobSchema) }),
  commands: availableSectionSchema,
  unavailable: z.array(
    z.object({ category: z.string(), reason: z.string() }),
  ),
})

export const sourceHealthSchema = z
  .array(
    z
      .object({
        surfaceId: z.string(),
        onboardingState: z.string().nullable(),
        runtimeHealth: z.record(z.string(), z.unknown()),
      })
      .loose(),
  )
  .nullable()

export const marketSnapshotSchema = z
  .array(
    z
      .object({
        sourceId: z.string(),
        venueId: z.string(),
        instrumentId: z.string(),
        phase: z.string(),
        currentDisplayQuality: z.string(),
        freshAtReference: z.boolean(),
        tradingStatus: z.string(),
        lastTrade: z.unknown().nullable(),
      })
      .loose(),
  )
  .nullable()

export const paperStatusSchema = z
  .object({
    state: z.enum(["stopped", "starting", "stopping", "failed", "running"]),
    orders: z.number().int().nonnegative().optional(),
    fills: z.number().int().nonnegative().optional(),
    positions: z.number().int().nonnegative().optional(),
    complete: z.boolean().optional(),
    reconciliationRequired: z.boolean().optional(),
    financialReconciliationCurrent: z.boolean().optional(),
    requiresStop: z.boolean().optional(),
  })
  .loose()

export const jobListSchema = z.object({
  jobs: z.array(overviewJobSchema),
  next: z.string().nullable(),
})

export type DecisionOverview = z.infer<typeof decisionOverviewSchema>
export type MarketSnapshot = z.infer<typeof marketSnapshotSchema>
export type OverviewJob = z.infer<typeof overviewJobSchema>
export type PaperStatus = z.infer<typeof paperStatusSchema>
export type SourceHealth = z.infer<typeof sourceHealthSchema>
