import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const timestampSchema = losslessIntegerSchema
const preparationUseSchema = z.enum(["local_analysis", "train"])

const datasetPreparationOptionSchema = z
  .object({
    choiceToken: z.string().uuid(),
    title: z.string().min(1).max(128),
    examples: z.number().int().min(3).max(2_048),
    observedFrom: timestampSchema,
    observedThrough: timestampSchema,
    availableUses: z.array(preparationUseSchema).min(1).max(2),
  })
  .strict()

const datasetPreparationOptionsSchema = z
  .object({
    choices: z.array(datasetPreparationOptionSchema).max(256),
  })
  .strict()

const datasetPreparationPreviewSchema = z
  .object({
    confirmationToken: z.string().uuid(),
    intendedUse: preparationUseSchema,
    examples: z.number().int().min(3).max(2_048),
    trainExamples: z.number().int().positive().max(2_048),
    validationExamples: z.number().int().positive().max(2_048),
    testExamples: z.number().int().positive().max(2_048),
    observedFrom: timestampSchema,
    observedThrough: timestampSchema,
    expiresAt: timestampSchema,
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
  })

export type DatasetPreparationUse = z.infer<typeof preparationUseSchema>
export type DatasetPreparationOptions = z.infer<
  typeof datasetPreparationOptionsSchema
>
export type DatasetPreparationOption = DatasetPreparationOptions["choices"][number]
export type DatasetPreparationPreview = z.infer<
  typeof datasetPreparationPreviewSchema
>
export type DatasetPreparationConfirmation =
  DatasetPreparationPreview["confirmationToken"]
export type DatasetPreparationSelection = {
  choiceToken: string
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
