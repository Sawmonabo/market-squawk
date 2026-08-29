import type * as React from "react"
import { CheckCircle2, Filter, Trash2 } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { humanize } from "@/lib/formatters"

import type { FeatureContractView, FeatureDatasetView } from "./contracts"
import {
  contractKey,
  featureDatasetLabel,
  featureLabel,
  SELECT_CLASS,
  type ComparisonOperator,
  type NullPolicy,
  type PredicateDraft,
  type SavedScreenReceipt,
} from "./screen-builder-model"

export function PredicateRow({
  index,
  predicate,
  contracts,
  removable,
  onChange,
  onRemove,
}: {
  index: number
  predicate: PredicateDraft
  contracts: FeatureContractView[]
  removable: boolean
  onChange: (next: PredicateDraft) => void
  onRemove: () => void
}) {
  const contract = contracts.find(
    (candidate) => contractKey(candidate) === predicate.featureKey,
  )
  return (
    <div className="grid gap-3 rounded-xl border border-border bg-background/45 p-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(180px,0.8fr)_minmax(140px,0.6fr)_minmax(170px,0.7fr)_auto]">
      <Field label={`Rule ${index + 1} feature`} htmlFor={`screen-feature-${index}`}>
        <select
          id={`screen-feature-${index}`}
          className={SELECT_CLASS}
          value={predicate.featureKey}
          onChange={(event) => onChange({ ...predicate, featureKey: event.target.value })}
        >
          <option value="">Select a feature</option>
          {contracts.map((item) => (
            <option key={contractKey(item)} value={contractKey(item)}>
              {featureLabel(item)}
            </option>
          ))}
        </select>
        {contract && (
          <p className="mt-2 text-[11px] text-muted-foreground">
            Output: {humanize(contract.outputUnit)} · {humanize(contract.timeSemantics.kind)}
          </p>
        )}
      </Field>
      <Field label="Comparison" htmlFor={`screen-operator-${index}`}>
        <select
          id={`screen-operator-${index}`}
          className={SELECT_CLASS}
          value={predicate.operator}
          onChange={(event) =>
            onChange({
              ...predicate,
              operator: event.target.value as ComparisonOperator,
            })
          }
        >
          <option value="greater_than_or_equal">At least</option>
          <option value="greater_than">More than</option>
          <option value="less_than_or_equal">At most</option>
          <option value="less_than">Less than</option>
          <option value="equal">Exactly</option>
        </select>
      </Field>
      <Field label="Threshold" htmlFor={`screen-threshold-${index}`}>
        <Input
          id={`screen-threshold-${index}`}
          className="mt-2"
          type="number"
          step="any"
          value={predicate.threshold}
          onChange={(event) => onChange({ ...predicate, threshold: event.target.value })}
        />
      </Field>
      <Field label="When data is missing" htmlFor={`screen-null-policy-${index}`}>
        <select
          id={`screen-null-policy-${index}`}
          className={SELECT_CLASS}
          value={predicate.nullPolicy}
          onChange={(event) =>
            onChange({ ...predicate, nullPolicy: event.target.value as NullPolicy })
          }
        >
          <option value="exclude">Exclude the investment</option>
          <option value="include">Keep it and flag missing data</option>
        </select>
      </Field>
      <Button
        type="button"
        variant="ghost"
        size="icon"
        className="self-end"
        onClick={onRemove}
        disabled={!removable}
        aria-label={`Remove rule ${index + 1}`}
      >
        <Trash2 aria-hidden="true" />
      </Button>
    </div>
  )
}

export function DatasetEvidence({ dataset }: { dataset: FeatureDatasetView }) {
  return (
    <div className="rounded-lg border border-border bg-card/55 p-3">
      <div className="flex items-center gap-2">
        <Filter className="size-4 text-primary" aria-hidden="true" />
        <p className="text-xs font-semibold">Research data selected</p>
      </div>
      <dl className="mt-3 grid gap-2 text-xs">
        <div>
          <dt className="text-muted-foreground">Dataset</dt>
          <dd className="font-medium">{featureDatasetLabel(dataset)}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Prepared examples</dt>
          <dd className="font-medium">
            {dataset.splitCounts.train.toLocaleString()} train ·{" "}
            {dataset.splitCounts.validation.toLocaleString()} validation ·{" "}
            {dataset.splitCounts.test.toLocaleString()} test
          </dd>
        </div>
      </dl>
    </div>
  )
}

export function Receipt({ receipt }: { receipt: SavedScreenReceipt }) {
  return (
    <Alert className="border-emerald-500/35 bg-emerald-500/5" role="status">
      <CheckCircle2 className="text-emerald-400" aria-hidden="true" />
      <AlertTitle>
        {receipt.outcome === "appended"
          ? "Saved-screen revision committed"
          : "This revision was already saved"}
      </AlertTitle>
      <AlertDescription>
        <span className="block">
          Your saved screen is ready to use. The saved-screen list has been refreshed.
        </span>
      </AlertDescription>
    </Alert>
  )
}

export function Field({
  label,
  htmlFor,
  help,
  children,
}: {
  label: string
  htmlFor: string
  help?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {help && <p className="mt-1.5 text-[11px] leading-4 text-muted-foreground">{help}</p>}
    </div>
  )
}
