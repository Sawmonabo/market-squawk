import type { DecisionDossierView, TargetIndexView } from "./contracts"
import type {
  AssumptionEvidenceView,
  TargetPreparationView,
  TargetReferenceMarkView,
} from "./target-preparation-contracts"

export type TargetIntent = "buy" | "sell" | "hold"
export type TargetHorizon = "quarter" | "year" | "three_years"
export type TargetMethod =
  | "comparable_evidence"
  | "discounted_cash_flow"
  | "residual_income"
  | "forecast_distribution"
  | "fair_value_measurement"
export type TargetOperation = "create" | string
export type TargetPriceKey =
  | "downside"
  | "add"
  | "entryLower"
  | "entryUpper"
  | "base"
  | "trimLower"
  | "trimUpper"
  | "exitLower"
  | "exitUpper"
  | "upside"

export interface TargetPriceDraft {
  downside: string
  add: string
  entryLower: string
  entryUpper: string
  base: string
  trimLower: string
  trimUpper: string
  exitLower: string
  exitUpper: string
  upside: string
}

export interface TargetAssumptionDraft {
  text: string
  evidenceKey: string
}

export const TARGET_PRICE_FIELDS: ReadonlyArray<{
  key: TargetPriceKey
  label: string
  help: string
}> = [
  { key: "downside", label: "Downside case", help: "Lowest valuation case" },
  { key: "add", label: "Add level", help: "Price for adding exposure" },
  { key: "entryLower", label: "Entry lower", help: "Bottom of the buy range" },
  { key: "entryUpper", label: "Entry upper", help: "Top of the buy range" },
  { key: "base", label: "Base case", help: "Central research judgment" },
  { key: "trimLower", label: "Trim lower", help: "Start reducing exposure" },
  { key: "trimUpper", label: "Trim upper", help: "Top of the trim range" },
  { key: "exitLower", label: "Exit lower", help: "Start of the exit range" },
  { key: "exitUpper", label: "Exit upper", help: "Top of the exit range" },
  { key: "upside", label: "Upside case", help: "Highest valuation case" },
]

export const TARGET_METHODS: ReadonlyArray<{ value: TargetMethod; label: string }> = [
  { value: "comparable_evidence", label: "Comparable evidence" },
  { value: "discounted_cash_flow", label: "Discounted cash flow" },
  { value: "residual_income", label: "Residual income" },
  { value: "forecast_distribution", label: "Forecast distribution" },
  { value: "fair_value_measurement", label: "Fair-value measurement" },
]

export function emptyPrices(): TargetPriceDraft {
  return {
    downside: "",
    add: "",
    entryLower: "",
    entryUpper: "",
    base: "",
    trimLower: "",
    trimUpper: "",
    exitLower: "",
    exitUpper: "",
    upside: "",
  }
}

export function emptyAssumption(): TargetAssumptionDraft {
  return { text: "", evidenceKey: "dossier" }
}

export function eligibleTargets(
  dossier: DecisionDossierView,
  targets: TargetIndexView[],
): TargetIndexView[] {
  return targets.filter((target) => target.instrumentId === dossier.instrumentId)
}

export function assumptionEvidence(
  key: string,
): AssumptionEvidenceView | null {
  if (key === "dossier") return { kind: "dossier" }
  if (key === "forecast") return { kind: "forecast" }
  if (key === "fair_value") return { kind: "fair_value" }
  if (key === "portfolio") return { kind: "portfolio" }
  if (key === "reference_mark") return { kind: "reference_mark" }
  const match = /^dossier_reference:(\d+)$/.exec(key)
  if (!match) return null
  const index = Number(match[1])
  return Number.isSafeInteger(index) ? { kind: "dossier_reference", index } : null
}

export function evidenceKey(value: AssumptionEvidenceView): string {
  return value.kind === "dossier_reference"
    ? `dossier_reference:${value.index}`
    : value.kind
}

export function evidenceAvailable(input: {
  evidence: AssumptionEvidenceView
  dossier: DecisionDossierView
  preparation: TargetPreparationView
  forecastIndex: string
  useFairValue: boolean
  usePortfolio: boolean
}): boolean {
  switch (input.evidence.kind) {
    case "dossier":
    case "reference_mark":
      return true
    case "dossier_reference":
      return input.evidence.index < input.dossier.references.length
    case "forecast":
      return input.forecastIndex !== "none"
    case "fair_value":
      return input.preparation.fairValueAvailable && input.useFairValue
    case "portfolio":
      return input.preparation.portfolioAvailable && input.usePortfolio
  }
}

export function validateTargetDraft(input: {
  operation: TargetOperation
  targets: TargetIndexView[]
  mark: TargetReferenceMarkView | undefined
  intent: TargetIntent
  prices: TargetPriceDraft
  method: TargetMethod
  assumptions: TargetAssumptionDraft[]
  risks: string[]
  invalidations: string[]
  thesis: string
  dossier: DecisionDossierView
  preparation: TargetPreparationView
  forecastIndex: string
  useFairValue: boolean
  usePortfolio: boolean
}): { valid: true } | { valid: false; reason: string } {
  if (
    input.operation !== "create" &&
    !input.targets.some((target) => target.id === input.operation)
  ) {
    return { valid: false, reason: "Choose a retained target series or create a new one." }
  }
  if (!input.mark) return { valid: false, reason: "Choose an available reference mark." }
  if (
    input.forecastIndex !== "none" &&
    !input.preparation.forecastOptions.some(
      (option) => String(option.index) === input.forecastIndex,
    )
  ) {
    return { valid: false, reason: "Choose forecast evidence retained by the selected dossier." }
  }
  if (input.useFairValue && !input.preparation.fairValueAvailable) {
    return { valid: false, reason: "Fair-value evidence is not available for this dossier." }
  }
  if (input.usePortfolio && !input.preparation.portfolioAvailable) {
    return { valid: false, reason: "Portfolio evidence is not available for this dossier." }
  }

  const parsedPrices = TARGET_PRICE_FIELDS.map(({ key }) => parseDecimal(input.prices[key]))
  if (parsedPrices.some((price) => price === null)) {
    return {
      valid: false,
      reason: "Enter every price as a nonnegative decimal with no more than 28 decimal places.",
    }
  }
  const exactPrices = parsedPrices as ExactDecimal[]
  if (exactPrices.slice(1).some((price, index) => compareDecimal(exactPrices[index]!, price) > 0)) {
    return { valid: false, reason: "Keep the complete price ladder ordered from downside to upside." }
  }
  const reference = parseDecimal(input.mark.price.amount)
  if (!reference) return { valid: false, reason: "The selected reference mark is invalid." }
  const entryUpper = exactPrices[3]!
  const base = exactPrices[4]!
  const trimLower = exactPrices[5]!
  const intentValid =
    (input.intent === "buy" &&
      compareDecimal(reference, entryUpper) <= 0 &&
      compareDecimal(base, reference) > 0) ||
    (input.intent === "sell" &&
      compareDecimal(reference, trimLower) >= 0 &&
      compareDecimal(base, reference) < 0) ||
    (input.intent === "hold" &&
      compareDecimal(reference, entryUpper) > 0 &&
      compareDecimal(reference, trimLower) < 0)
  if (!intentValid) {
    return {
      valid: false,
      reason: "The selected buy, sell, or hold intent does not match the reference mark and ranges.",
    }
  }
  const forecastSelected = input.forecastIndex !== "none"
  const methodValid = methodSupported(
    input.method,
    forecastSelected,
    input.useFairValue,
  )
  if (!methodValid) {
    return { valid: false, reason: "Choose a method supported by the selected forecast or fair-value evidence." }
  }
  if (input.assumptions.length === 0 || input.assumptions.length > 32) {
    return { valid: false, reason: "Enter between 1 and 32 evidence-bound assumptions." }
  }
  for (const assumption of input.assumptions) {
    const evidence = assumptionEvidence(assumption.evidenceKey)
    if (
      !validNarrative(assumption.text) ||
      !evidence ||
      !evidenceAvailable({ ...input, evidence })
    ) {
      return { valid: false, reason: "Complete every assumption and bind it to selected evidence." }
    }
  }
  if (!validNarrative(input.thesis)) {
    return { valid: false, reason: "Enter a complete thesis of no more than 4,096 UTF-8 bytes." }
  }
  if (!validNarrativeList(input.risks)) {
    return { valid: false, reason: "Enter between 1 and 32 concise risk statements." }
  }
  if (!validNarrativeList(input.invalidations)) {
    return { valid: false, reason: "Enter between 1 and 32 clear invalidation conditions." }
  }
  return { valid: true }
}

interface ExactDecimal {
  coefficient: bigint
  scale: number
}

function parseDecimal(value: string): ExactDecimal | null {
  const match = /^(0|[1-9]\d*)(?:\.(\d{1,28}))?$/.exec(value)
  if (!match) return null
  const fraction = match[2] ?? ""
  const digits = `${match[1]}${fraction}`
  if (digits.replace(/^0+/, "").length > 29) return null
  return { coefficient: BigInt(digits), scale: fraction.length }
}

function compareDecimal(left: ExactDecimal, right: ExactDecimal): number {
  const scale = Math.max(left.scale, right.scale)
  const leftValue = left.coefficient * 10n ** BigInt(scale - left.scale)
  const rightValue = right.coefficient * 10n ** BigInt(scale - right.scale)
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0
}

function methodSupported(
  method: TargetMethod,
  forecastSelected: boolean,
  fairValueSelected: boolean,
): boolean {
  switch (method) {
    case "comparable_evidence":
      return forecastSelected || fairValueSelected
    case "discounted_cash_flow":
    case "residual_income":
    case "forecast_distribution":
      return forecastSelected
    case "fair_value_measurement":
      return fairValueSelected
  }
}

function validNarrative(value: string): boolean {
  return (
    value.length > 0 &&
    value.trim() === value &&
    new TextEncoder().encode(value).length <= 4_096 &&
    !/\p{Cc}/u.test(value)
  )
}

function validNarrativeList(values: string[]): boolean {
  return values.length > 0 && values.length <= 32 && values.every(validNarrative)
}
