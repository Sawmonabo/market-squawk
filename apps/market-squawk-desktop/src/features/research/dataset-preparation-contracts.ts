import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const timestampSchema = losslessIntegerSchema
const preparationUseSchema = z.enum(["local_analysis", "train"])

const datasetPreparationOptionSchema = z
  .object({
    id: z.string().min(1).max(256),
    label: z.string().min(1).max(512),
    sourceDataset: z.string().min(1).max(256),
    immutableGeneration: losslessIntegerSchema.refine(
      (value) => BigInt(value) >= 0n,
      "Expected a non-negative immutable generation.",
    ),
    instrumentId: z.string().uuid(),
    observedPoints: z.number().int().positive().max(4_096),
    examples: z.number().int().min(3).max(2_048),
    observedFrom: timestampSchema,
    observedThrough: timestampSchema,
    availableUses: z.array(preparationUseSchema).min(1).max(2),
  })
  .strict()

const datasetPreparationOptionsSchema = z
  .object({
    catalogGeneration: digestSchema,
    datasets: z.array(datasetPreparationOptionSchema).max(256),
  })
  .strict()

const datasetPreparationReceiptSchema = z
  .object({
    receiptId: z.string().uuid(),
    preparationSha256: digestSchema,
    expiresAt: timestampSchema,
  })
  .strict()

const datasetPreparationPreviewSchema = z
  .object({
    receipt: datasetPreparationReceiptSchema,
    dataset: z.string().min(1).max(512),
    source: z.string().min(1).max(512),
    instrumentId: z.string().uuid(),
    intendedUse: preparationUseSchema,
    examples: z.number().int().min(3).max(2_048),
    trainExamples: z.number().int().positive().max(2_048),
    validationExamples: z.number().int().positive().max(2_048),
    testExamples: z.number().int().positive().max(2_048),
    observedFrom: timestampSchema,
    observedThrough: timestampSchema,
    buildSpecSha256: digestSchema,
    evidence: z.array(z.string().min(1).max(4_096)).min(1).max(16),
  })
  .strict()
  .superRefine((preview, context) => {
    if (
      preview.trainExamples +
        preview.validationExamples +
        preview.testExamples !==
      preview.examples
    ) {
      context.addIssue({
        code: "custom",
        message: "The prepared dataset split does not match its example count.",
      })
    }
    if (BigInt(preview.observedFrom) > BigInt(preview.observedThrough)) {
      context.addIssue({
        code: "custom",
        message: "The prepared dataset time range is not ordered.",
      })
    }
  })

export type DatasetPreparationUse = z.infer<typeof preparationUseSchema>
export type DatasetPreparationOptions = z.infer<
  typeof datasetPreparationOptionsSchema
>
export type DatasetPreparationOption = DatasetPreparationOptions["datasets"][number]
export type DatasetPreparationReceipt = z.infer<
  typeof datasetPreparationReceiptSchema
>
export type DatasetPreparationPreview = z.infer<
  typeof datasetPreparationPreviewSchema
>
export type DatasetPreparationSelection = {
  catalogGeneration: string
  dataset: string
  intendedUse: DatasetPreparationUse
}

export function parseDatasetPreparationOptions(
  result: ApplicationResult,
): DatasetPreparationOptions {
  const parsed = datasetPreparationOptionsSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned unsupported feature-dataset choices.",
    )
  }
  return parsed.data
}

export function parseDatasetPreparationPreview(
  result: ApplicationResult,
): DatasetPreparationPreview {
  const parsed = datasetPreparationPreviewSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported feature-dataset evidence preview.",
    )
  }
  return parsed.data
}
