import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const resultCompletenessSchema = z.enum(["complete", "truncated"])
type ResultCompleteness = z.infer<typeof resultCompletenessSchema>

export const researchCollectionSchema = z.strictObject({
  collectionToken: z.string().uuid(),
  title: z.string().min(1),
  rowCount: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
})

const researchCollectionPageSchema = z.strictObject({
  items: z.array(researchCollectionSchema),
  hasMore: z.boolean(),
  nextCollection: z.string().uuid().nullable(),
})

const researchActionAcceptedSchema = z.strictObject({
  accepted: z.literal(true),
})

const researchObservationScalarSchema = z.union([
  z.string(),
  z.number().finite(),
  z.null(),
])

export const researchObservationSchema = z.strictObject({
  revision: researchObservationScalarSchema.optional(),
  quality: researchObservationScalarSchema.optional(),
  effectiveAt: researchObservationScalarSchema.optional(),
  publishedAt: researchObservationScalarSchema.optional(),
  availableAt: researchObservationScalarSchema.optional(),
  supersededAt: researchObservationScalarSchema.optional(),
})

const inlineObservationResultSchema = z.strictObject({
  kind: z.literal("inline"),
  rows: z.array(researchObservationSchema),
})

const artifactObservationResultSchema = z.strictObject({
  kind: z.literal("artifact"),
  rowCount: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
})

export type ResearchCollection = z.infer<typeof researchCollectionSchema>
export type ResearchObservation = z.infer<typeof researchObservationSchema>

export type ResearchObservationResult =
  | {
      kind: "empty"
      returnedItems: number
      completeness: ResultCompleteness
    }
  | {
      kind: "inline"
      rows: ResearchObservation[]
      returnedItems: number
      completeness: ResultCompleteness
    }
  | {
      kind: "artifact"
      rowCount: number
      returnedItems: number
      completeness: ResultCompleteness
    }

export interface ResearchCollectionPage {
  items: ResearchCollection[]
  hasMore: boolean
  nextCollection: string | null
  completeness: ResultCompleteness
}

export function parseResearchCollectionPage(
  result: ApplicationResult,
): ResearchCollectionPage {
  const completeness = resultCompletenessSchema.parse(
    result.metadata.completeness,
  )
  if (result.data === null) {
    validateReturnedItems(result, 0, "research collection")
    return {
      items: [],
      hasMore: false,
      nextCollection: null,
      completeness,
    }
  }
  const page = researchCollectionPageSchema.safeParse(result.data)
  if (!page.success) {
    throw new Error(
      "The installed service returned an unsupported research collection response.",
    )
  }
  if (page.data.hasMore && page.data.nextCollection === null) {
    throw new Error("The research collection continuation is incomplete.")
  }
  validateReturnedItems(result, page.data.items.length, "research collection")
  return { ...page.data, completeness }
}

export function parseResearchCollection(
  result: ApplicationResult,
  expectedCollection: string,
): ResearchCollection {
  const collection = researchCollectionSchema.safeParse(result.data)
  if (
    !collection.success ||
    collection.data.collectionToken !== expectedCollection
  ) {
    throw new Error(
      "The installed service returned an unsupported research collection response.",
    )
  }
  validateReturnedItems(result, 1, "research collection")
  return collection.data
}

export function parseResearchObservations(
  result: ApplicationResult,
): ResearchObservationResult {
  const common = {
    returnedItems: result.metadata.returnedItems,
    completeness: resultCompletenessSchema.parse(result.metadata.completeness),
  }
  if (result.data === null) {
    validateReturnedItems(result, 0, "research history")
    return { kind: "empty", ...common }
  }

  const inline = inlineObservationResultSchema.safeParse(result.data)
  if (inline.success && inline.data.rows.length === result.metadata.returnedItems) {
    validateReturnedItems(result, inline.data.rows.length, "research history")
    return { ...inline.data, ...common }
  }

  const artifact = artifactObservationResultSchema.safeParse(result.data)
  if (artifact.success && artifact.data.rowCount === result.metadata.returnedItems) {
    validateReturnedItems(result, artifact.data.rowCount, "research history")
    return { ...artifact.data, ...common }
  }

  throw new Error(
    "The installed service returned an unsupported research-history response.",
  )
}

export function parseResearchActionAccepted(result: ApplicationResult): void {
  const accepted = researchActionAcceptedSchema.safeParse(result.data)
  if (!accepted.success) {
    throw new Error(
      "The installed service did not accept the requested research action.",
    )
  }
  validateReturnedItems(result, 1, "research action")
}

export function validateReturnedItems(
  result: ApplicationResult,
  actual: number,
  label: string,
) {
  const { availableItems, returnedItems } = result.metadata
  const completeness = resultCompletenessSchema.parse(result.metadata.completeness)
  if (actual === 0 && availableItems > 0) {
    throw new Error(
      `The ${label} result reports available rows, but none were returned within its bounds.`,
    )
  }
  if (
    returnedItems !== actual ||
    returnedItems > availableItems ||
    (completeness === "complete" && returnedItems !== availableItems) ||
    (completeness === "truncated" && returnedItems >= availableItems)
  ) {
    throw new Error(`The ${label} result counts are inconsistent.`)
  }
}
