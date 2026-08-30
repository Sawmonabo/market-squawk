import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import {
  AlertCircle,
  CalendarClock,
  Database,
  Layers3,
  Play,
  ShieldCheck,
  Split,
} from "lucide-react"

import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseDatasetPreparationOptions,
  parseDatasetPreparationPreview,
  type DatasetPreparationOption,
  type DatasetPreparationOptions,
  type DatasetPreparationPreview,
  type DatasetPreparationConfirmation,
  type DatasetPreparationSelection,
} from "./dataset-preparation-contracts"
import { parseResearchActionAccepted } from "./research-contracts"

const PREPARATION_CAPABILITIES = [
  "feature_dataset_preparation",
  "feature_dataset_preview",
  "feature_dataset_prepared_start",
] as const
const CONTROL_CLASS =
  "mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"

export function DatasetBuilder({
  bootstrap,
  transport,
  onStarted,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onStarted: () => Promise<unknown>
}) {
  const capabilities = productCapabilitySet(bootstrap)
  const capabilitiesAvailable = PREPARATION_CAPABILITIES.every((capability) =>
    capabilities.has(capability),
  )
  const [selection, setSelection] =
    React.useState<DatasetPreparationSelection | null>(null)
  const [preview, setPreview] =
    React.useState<DatasetPreparationPreview | null>(null)
  const [started, setStarted] = React.useState(false)
  const optionsQuery = useQuery({
    queryKey: [
      ...productKeys.domain(bootstrap.productSessionToken, "research"),
      "preparation-choices",
    ],
    enabled: capabilitiesAvailable,
    staleTime: 30_000,
    queryFn: async () =>
      parseDatasetPreparationOptions(
        await transport.datasetPreparation({ action: "options" }),
      ),
  })
  const previewMutation = useMutation({
    mutationFn: async (draft: DatasetPreparationSelection) =>
      parseDatasetPreparationPreview(
        await transport.datasetPreparation({
          action: "preview",
          choice: draft.choiceToken,
          intendedUse: draft.intendedUse,
        }),
      ),
    onSuccess: setPreview,
  })
  const startMutation = useMutation({
    mutationFn: async (confirmationToken: DatasetPreparationConfirmation) =>
      parseResearchActionAccepted(
        await transport.datasetPreparation(
          { action: "start", confirmationToken },
          true,
        ),
      ),
    onSuccess: async () => {
      setStarted(true)
      setPreview(null)
      await onStarted()
    },
  })

  React.useEffect(() => {
    const options = optionsQuery.data
    if (!options) return
    const currentDataset = options.choices.find(
      (candidate) => candidate.choiceToken === selection?.choiceToken,
    )
    const currentUse = selection?.intendedUse
    if (
      currentUse !== undefined &&
      currentDataset?.availableUses.includes(currentUse)
    ) {
      return
    }
    setSelection(defaultSelection(options))
    setPreview(null)
    previewMutation.reset()
    startMutation.reset()
  }, [optionsQuery.data, selection])

  const selectedDataset =
    optionsQuery.data && selection
      ? optionsQuery.data.choices.find(
          (candidate) => candidate.choiceToken === selection.choiceToken,
        ) ?? null
      : null

  return (
    <section className="mt-5 rounded-xl border border-primary/20 bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Prepare research
          </p>
          <h2 className="mt-2 text-xl font-semibold">Prepare information for analysis</h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Choose the information and how you plan to use it. Market Squawk keeps dates in order
            and separates training, review, and testing periods before work begins.
          </p>
        </div>
        <Layers3 className="size-5 text-primary" aria-hidden="true" />
      </div>

      {!capabilitiesAvailable ? (
        <Alert className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Research preparation is unavailable</AlertTitle>
          <AlertDescription>
            This installation cannot safely prepare research yet. Other Research tools remain
            available.
          </AlertDescription>
        </Alert>
      ) : optionsQuery.isPending ? (
        <Status text="Finding information that is ready to use…" />
      ) : optionsQuery.isError ? (
        <Status text="Available research choices could not be loaded. Try again." tone="error" />
      ) : optionsQuery.data.choices.length === 0 ? (
        <Alert className="mt-4">
          <Database aria-hidden="true" />
          <AlertTitle>No information is ready to prepare</AlertTitle>
          <AlertDescription>
            Add research history first. A choice appears here when Market Squawk has enough dated
            information to prepare it safely.
          </AlertDescription>
        </Alert>
      ) : selection && selectedDataset ? (
        <div className="mt-5 space-y-4">
          <DatasetPreparationFields
            options={optionsQuery.data}
            selection={selection}
            selectedDataset={selectedDataset}
            disabled={previewMutation.isPending || startMutation.isPending}
            onChange={(next) => {
              setSelection(next)
              setPreview(null)
              setStarted(false)
              previewMutation.reset()
              startMutation.reset()
            }}
          />
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-[11px] leading-5 text-muted-foreground">
              Market Squawk checks the available dates and keeps training, review, and testing
              periods separate.
            </p>
            <Button
              disabled={previewMutation.isPending}
              onClick={() => previewMutation.mutate(selection)}
            >
              <ShieldCheck aria-hidden="true" />
              {previewMutation.isPending ? "Preparing review…" : "Review preparation"}
            </Button>
          </div>
          {previewMutation.isError ? (
            <Status text="This preparation could not be reviewed. Check your choices and try again." tone="error" />
          ) : null}
          {started ? (
            <Status
              text="Preparation started. You can follow it in Operations & Jobs."
              tone="success"
            />
          ) : null}
        </div>
      ) : null}

      <Dialog
        open={preview !== null}
        onOpenChange={(open) => {
          if (!open && !startMutation.isPending) setPreview(null)
        }}
      >
        <DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>Prepare this information?</DialogTitle>
            <DialogDescription>
              Review the date range and how the examples will be divided before you continue.
            </DialogDescription>
          </DialogHeader>
          {preview && selectedDataset ? (
            <DatasetPreparationReview
              preview={preview}
              collectionName={selectedDataset.title}
            />
          ) : null}
          {startMutation.isError ? (
            <Status text="Preparation could not be started. Review your choices and try again." tone="error" />
          ) : null}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={startMutation.isPending}
              onClick={() => setPreview(null)}
            >
              Change choices
            </Button>
            <Button
              disabled={!preview || startMutation.isPending}
              onClick={() => {
                if (preview) startMutation.mutate(preview.confirmationToken)
              }}
            >
              <Play aria-hidden="true" />
              {startMutation.isPending ? "Starting…" : "Start preparation"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function DatasetPreparationFields({
  options,
  selection,
  selectedDataset,
  disabled,
  onChange,
}: {
  options: DatasetPreparationOptions
  selection: DatasetPreparationSelection
  selectedDataset: DatasetPreparationOption
  disabled: boolean
  onChange: (selection: DatasetPreparationSelection) => void
}) {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <label className="text-xs font-medium">
        Available research collections
        <select
          className={CONTROL_CLASS}
          value={selection.choiceToken}
          disabled={disabled}
          onChange={(event) => {
            const dataset =
              options.choices.find(
                (candidate) => candidate.choiceToken === event.target.value,
              ) ?? options.choices[0]
            const intendedUse = dataset?.availableUses[0]
            if (!dataset || !intendedUse) return
            onChange({
              choiceToken: dataset.choiceToken,
              intendedUse,
            })
          }}
        >
          {options.choices.map((dataset) => (
            <option key={dataset.choiceToken} value={dataset.choiceToken}>
              {dataset.title} · {dataset.examples.toLocaleString()} examples
            </option>
          ))}
        </select>
        <span className="mt-2 block text-[11px] font-normal leading-5 text-muted-foreground">
          {selectedDataset.examples.toLocaleString()} examples from{" "}
          {formatTimestamp(selectedDataset.observedFrom)} through{" "}
          {formatTimestamp(selectedDataset.observedThrough)}.
        </span>
      </label>
      <label className="text-xs font-medium">
        What will you use it for?
        <select
          className={CONTROL_CLASS}
          value={selection.intendedUse}
          disabled={disabled}
          onChange={(event) => {
            const intendedUse = selectedDataset.availableUses.find(
              (candidate) => candidate === event.target.value,
            )
            if (!intendedUse) return
            onChange({
              ...selection,
              intendedUse,
            })
          }}
        >
          {selectedDataset.availableUses.map((useCase) => (
            <option key={useCase} value={useCase}>
              {useCase === "train" ? "Train or evaluate a model" : "Analyze locally"}
            </option>
          ))}
        </select>
        <span className="mt-2 block text-[11px] font-normal leading-5 text-muted-foreground">
          Only purposes supported by the available data are shown.
        </span>
      </label>
    </div>
  )
}

function DatasetPreparationReview({
  preview,
  collectionName,
}: {
  preview: DatasetPreparationPreview
  collectionName: string
}) {
  return (
    <div className="space-y-4 py-2">
      <div className="grid gap-3 sm:grid-cols-2">
        <ReviewFact icon={Database} label="Collection" value={collectionName} />
        <ReviewFact
          icon={ShieldCheck}
          label="Planned use"
          value={
            preview.intendedUse === "train"
              ? "Train or evaluate a model"
              : "Analyze locally"
          }
        />
        <ReviewFact
          icon={CalendarClock}
          label="Review available until"
          value={formatTimestamp(preview.expiresAt)}
        />
      </div>

      <section className="rounded-lg border border-border bg-background/35 p-4">
        <div className="flex items-center gap-2">
          <Split className="size-4 text-primary" aria-hidden="true" />
          <h3 className="text-sm font-semibold">Chronological split</h3>
        </div>
        <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2 lg:grid-cols-4">
          <ReviewMetric label="All examples" value={preview.examples} />
          <ReviewMetric label="Training" value={preview.trainExamples} />
          <ReviewMetric label="Validation" value={preview.validationExamples} />
          <ReviewMetric label="Testing" value={preview.testExamples} />
        </dl>
        <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
          Information covers {formatTimestamp(preview.observedFrom)} through{" "}
          {formatTimestamp(preview.observedThrough)}.
        </p>
      </section>

      <section className="rounded-lg border border-border bg-background/35 p-4">
        <div className="flex items-center gap-2">
          <ShieldCheck className="size-4 text-primary" aria-hidden="true" />
          <h3 className="text-sm font-semibold">Checks completed</h3>
        </div>
        <ul className="mt-3 grid gap-2 sm:grid-cols-3">
          <li className="rounded-md border border-border/70 bg-card/35 p-3 text-xs leading-5">
            Dates are in order
          </li>
          <li className="rounded-md border border-border/70 bg-card/35 p-3 text-xs leading-5">
            Periods do not overlap
          </li>
          <li className="rounded-md border border-border/70 bg-card/35 p-3 text-xs leading-5">
            Information is currently available
          </li>
        </ul>
        <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
          These checks are repeated when preparation starts.
        </p>
      </section>
    </div>
  )
}

function ReviewFact({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Database
  label: string
  value: string
}) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-4">
      <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        {label}
      </div>
      <p className="mt-2 break-words text-sm font-medium">{value}</p>
    </div>
  )
}

function ReviewMetric({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-base font-semibold">{value.toLocaleString()}</dd>
    </div>
  )
}

function Status({
  text,
  tone = "neutral",
}: {
  text: string
  tone?: "neutral" | "error" | "success"
}) {
  return (
    <p
      className={`mt-4 rounded-lg border p-3 text-xs leading-5 ${
        tone === "error"
          ? "border-destructive/35 bg-destructive/10 text-destructive"
          : tone === "success"
            ? "border-[var(--success)]/35 bg-[var(--success)]/10 text-[var(--success)]"
            : "border-border bg-background/35 text-muted-foreground"
      }`}
      role={tone === "error" ? "alert" : "status"}
    >
      {text}
    </p>
  )
}

function defaultSelection(
  options: DatasetPreparationOptions,
): DatasetPreparationSelection | null {
  const dataset = options.choices[0]
  const intendedUse = dataset?.availableUses[0]
  if (!dataset || !intendedUse) return null
  return {
    choiceToken: dataset.choiceToken,
    intendedUse,
  }
}
