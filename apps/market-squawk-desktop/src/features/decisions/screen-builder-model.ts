import { humanize } from "@/lib/formatters"
import { dataQualities, type DataQuality } from "@/lib/quality"

import {
  digestHex,
  featureSemanticBytes,
  type FeatureContractView,
  type FeatureDatasetView,
  type SavedScreenOutcome,
  type SavedScreenView,
} from "./contracts"

const SCREEN_ID_PATTERN = /^[a-z][a-z0-9._-]{0,127}$/

export const DEFAULT_QUALITIES: DataQuality[] = dataQualities.filter(
  (quality) => quality !== "stale" && quality !== "quarantined",
)
export const SELECT_CLASS =
  "mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"

export type ComparisonOperator =
  | "less_than"
  | "less_than_or_equal"
  | "equal"
  | "greater_than_or_equal"
  | "greater_than"
export type NullPolicy = "exclude" | "include"
export type RankingDirection = "ascending" | "descending"

export interface PredicateDraft {
  featureKey: string
  operator: ComparisonOperator
  threshold: string
  nullPolicy: NullPolicy
}

export interface SavedScreenReceipt {
  outcome: SavedScreenOutcome
  screenId: string
  revision: number
  dataset: FeatureDatasetView
}

export function emptyPredicate(): PredicateDraft {
  return {
    featureKey: "",
    operator: "greater_than_or_equal",
    threshold: "0",
    nullPolicy: "exclude",
  }
}

export function contractKey(contract: FeatureContractView): string {
  return `${contract.name}:${contract.version}:${contract.semanticDigest}`
}

export function bindingKey(binding: {
  name: string
  version: number
  semanticDigest: readonly number[]
}): string {
  return `${binding.name}:${binding.version}:${digestHex(binding.semanticDigest)}`
}

export function bindingFor(contract: FeatureContractView) {
  return {
    name: contract.name,
    version: contract.version,
    semanticDigest: featureSemanticBytes(contract.semanticDigest),
  }
}

export function datasetKeyFor(dataset: FeatureDatasetView): string {
  return `${dataset.manifest.dataset}:${dataset.manifest.contentHash}`
}

export function screenKey(screen: SavedScreenView): string {
  return `${screen.id}:${screen.revision}`
}

export function featureLabel(contract: FeatureContractView): string {
  return `${humanize(contract.name)} · v${contract.version}`
}

export function isDataQuality(value: string): value is DataQuality {
  return (dataQualities as readonly string[]).includes(value)
}

function finiteNumber(value: string): number | null {
  if (value.trim() === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

export function validateDraft(input: {
  screenId: string
  selectedDataset: FeatureDatasetView | undefined
  predicates: PredicateDraft[]
  contractByKey: ReadonlyMap<string, FeatureContractView>
  rankingFeatureKey: string
  maximumResults: string
  minimumCoverage: string
  minimumLiquidity: string
  qualities: DataQuality[]
}): { valid: true } | { valid: false; reason: string } {
  if (!SCREEN_ID_PATTERN.test(input.screenId)) {
    return {
      valid: false,
      reason: "Enter a stable screen ID beginning with a lowercase letter.",
    }
  }
  if (!input.selectedDataset) {
    return { valid: false, reason: "Select a prepared point-in-time dataset." }
  }
  if (
    input.predicates.length === 0 ||
    input.predicates.some(
      (predicate) =>
        !input.contractByKey.has(predicate.featureKey) ||
        finiteNumber(predicate.threshold) === null,
    )
  ) {
    return {
      valid: false,
      reason: "Choose an available feature and a finite threshold for every rule.",
    }
  }
  if (!input.contractByKey.has(input.rankingFeatureKey)) {
    return { valid: false, reason: "Choose the feature used to rank results." }
  }
  const resultLimit = finiteNumber(input.maximumResults)
  if (
    resultLimit === null ||
    !Number.isInteger(resultLimit) ||
    resultLimit < 1 ||
    resultLimit > 1024
  ) {
    return { valid: false, reason: "Maximum results must be a whole number from 1 to 1,024." }
  }
  const coverage = finiteNumber(input.minimumCoverage)
  if (coverage === null || coverage < 0 || coverage > 100) {
    return { valid: false, reason: "Required coverage must be between 0 and 100 percent." }
  }
  const liquidity = finiteNumber(input.minimumLiquidity)
  if (liquidity === null || liquidity < 0) {
    return { valid: false, reason: "Minimum liquidity must be zero or greater." }
  }
  if (input.qualities.length === 0 || new Set(input.qualities).size !== input.qualities.length) {
    return { valid: false, reason: "Select at least one evidence-quality class." }
  }
  return { valid: true }
}
