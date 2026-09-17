import { z } from "zod"

import {
  PRODUCT_LOOKUP_QUERY_MAXIMUM_CHARACTERS,
  productLookupActions,
  productLookupCategories,
  productLookupCategory,
  type ProductLookupCategory,
} from "@/lib/transport"

export const lookupCategories = productLookupCategories
export type LookupCategory = ProductLookupCategory

const lookupControlCharacter = /[\u0000-\u001F\u007F-\u009F]/u
const lookupNonScalarCharacter = /[\uD800-\uDFFF]/u
const productScreenId = /^[a-z][a-z0-9._-]*$/u

export function normalizeLookupQuery(value: string): string | null {
  const query = value.trim()
  if (
    query.length === 0 ||
    lookupControlCharacter.test(query) ||
    lookupNonScalarCharacter.test(query)
  ) {
    return null
  }
  return [...query].length <= PRODUCT_LOOKUP_QUERY_MAXIMUM_CHARACTERS
    ? query
    : null
}

export function admittedLookupQuery(value: string) {
  return normalizeLookupQuery(value) === value
}

const lookupQuerySchema = z.string().refine(admittedLookupQuery)

const lookupMatchText = {
  title: z.string().min(1).max(2_048),
  subtitle: z.string().min(1).max(2_048),
}

export const lookupMatchSchema = z.discriminatedUnion("category", [
  z.object({
    ...lookupMatchText,
    category: z.literal(productLookupCategory.investment),
    destination: z.object({
      action: z.literal(productLookupActions.openInvestment),
      instrumentId: z.string().uuid(),
    }).strict(),
  }).strict(),
  z.object({
    ...lookupMatchText,
    category: z.literal(productLookupCategory.savedScreen),
    destination: z.object({
      action: z.literal(productLookupActions.openSavedScreen),
      screenId: z.string().min(1).max(128).regex(productScreenId),
    }).strict(),
  }).strict(),
])

const lookupCategoryStateSchema = z.discriminatedUnion("state", [
  z.object({
    category: z.enum(lookupCategories),
    state: z.literal("available"),
  }).strict(),
  z.object({
    category: z.enum(lookupCategories),
    state: z.literal("unavailable"),
    message: z.string().min(1).max(256),
  }).strict(),
])

export const lookupResultSchema = z.object({
  query: lookupQuerySchema,
  matches: z.array(lookupMatchSchema).max(64),
  categories: z.array(lookupCategoryStateSchema).max(7),
  truncated: z.boolean(),
}).strict()

export type LookupMatch = z.infer<typeof lookupMatchSchema>
export type LookupResult = z.infer<typeof lookupResultSchema>
