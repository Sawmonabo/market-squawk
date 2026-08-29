import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import {
  BrainCircuit,
  CalendarClock,
  ChartSpline,
  CircleAlert,
  Database,
  Play,
  ShieldCheck,
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
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import { humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseForecastPreparationOptions,
  parseForecastPreparationPreview,
  parseForecastPreparedJobReceipt,
  type ForecastPreparationDataset,
  type ForecastPreparationModel,
  type ForecastPreparationOptions,
  type ForecastPreparationPolicy,
  type ForecastPreparationPreview,
  type ForecastPreparationReceipt,
  type ForecastPreparationSelection,
} from "./forecast-preparation-contracts"

const PREPARATION_OPERATIONS = [
  "Model.GetForecastPreparation",
  "Model.PrepareForecast",
  "Model.StartPreparedForecast",
] as const
const CONTROL_CLASS =
  "h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

export function ForecastPreparation({
  bootstrap,
  transport,
  selectedModel,
  onStarted,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  selectedModel: {
    modelId: string
    bundleId: string
    bundleVersion: number
  } | null
  onStarted: () => Promise<unknown>
}) {
  const operations = new Set(bootstrap.operations.map((operation) => operation.name))
  const operationsAvailable = PREPARATION_OPERATIONS.every((operation) =>
    operations.has(operation),
  )
  const guidedTransport = asForecastPreparationTransport(transport)
  const [draft, setDraft] = React.useState<PreparationDraft | null>(null)
  const [preview, setPreview] = React.useState<ForecastPreparationPreview | null>(null)
  const [queuedJobId, setQueuedJobId] = React.useState<string | null>(null)
  const optionsQuery = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecastPreparation",
      {},
    ),
    enabled: operationsAvailable && guidedTransport !== null,
    staleTime: 30_000,
    queryFn: async () => {
      if (!guidedTransport) {
        throw new Error("Guided forecast preparation is unavailable.")
      }
      return parseForecastPreparationOptions(
        await guidedTransport.forecastPreparation({ action: "options" }),
      )
    },
  })
  const previewMutation = useMutation({
    mutationFn: async (selection: ForecastPreparationSelection) => {
      if (!guidedTransport) {
        throw new Error("Guided forecast preparation is unavailable.")
      }
      return parseForecastPreparationPreview(
        await guidedTransport.forecastPreparation({
          action: "preview",
          selection,
        }),
      )
    },
    onSuccess: setPreview,
  })
  const startMutation = useMutation({
    mutationFn: async (receipt: ForecastPreparationReceipt) => {
      if (!guidedTransport) {
        throw new Error("Guided forecast preparation is unavailable.")
      }
      return parseForecastPreparedJobReceipt(
        await guidedTransport.forecastPreparation(
          { action: "start", receipt },
          true,
        ),
      )
    },
    onSuccess: async (receipt) => {
      setQueuedJobId(receipt.jobId)
      setPreview(null)
      await onStarted()
    },
  })

  React.useEffect(() => {
    const options = optionsQuery.data
    if (!options) return
    setDraft(defaultDraft(options, selectedModel))
    setPreview(null)
    previewMutation.reset()
    startMutation.reset()
  }, [
    optionsQuery.data,
    selectedModel?.modelId,
    selectedModel?.bundleId,
    selectedModel?.bundleVersion,
  ])

  const context = draft && optionsQuery.data
    ? resolveDraft(optionsQuery.data, draft)
    : null
  const ready = context !== null && selectionIsValid(context, draft)

  return (
    <section className="rounded-xl border border-primary/20 bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-wider text-primary">
            Point-in-time forecast builder
          </p>
          <h2 className="mt-2 text-xl font-semibold">Prepare a statistical forecast</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Choose a reviewed model, investment, time horizon, and historical cutoff. Check the
            limitations and uncertainty before starting.
          </p>
        </div>
        <BrainCircuit className="size-5 text-primary" aria-hidden="true" />
      </div>

      {!operationsAvailable || guidedTransport === null ? (
        <Alert className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Guided forecast preparation is unavailable</AlertTitle>
          <AlertDescription>
            Forecast preparation is not available in this workspace.
          </AlertDescription>
        </Alert>
      ) : optionsQuery.isPending ? (
        <Status text="Loading admitted models and compatible evidence…" />
      ) : optionsQuery.isError ? (
        <Status text="Forecast choices are unavailable right now." tone="error" />
      ) : optionsQuery.data.models.length === 0 ? (
        <Alert className="mt-4">
          <Database aria-hidden="true" />
          <AlertTitle>No forecast-ready evidence is available</AlertTitle>
          <AlertDescription>
            A reviewed model needs enough compatible historical information before it can produce
            a forecast.
          </AlertDescription>
        </Alert>
      ) : draft && context ? (
        <div className="mt-5 space-y-4">
          <PreparationFields
            options={optionsQuery.data}
            draft={draft}
            context={context}
            disabled={previewMutation.isPending || startMutation.isPending}
            onChange={(next) => {
              setDraft(next)
              setPreview(null)
              previewMutation.reset()
              startMutation.reset()
            }}
          />
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-[11px] leading-5 text-muted-foreground">
              Review the model, investment, horizon, and historical cutoff before continuing.
            </p>
            <Button
              disabled={!ready || previewMutation.isPending}
              onClick={() => {
                if (ready) previewMutation.mutate(draft.selection)
              }}
            >
              <ShieldCheck aria-hidden="true" />
              {previewMutation.isPending ? "Preparing…" : "Review forecast"}
            </Button>
          </div>
          {previewMutation.isError ? (
            <Status text="This forecast could not be prepared. Review the choices and try again." tone="error" />
          ) : null}
          {queuedJobId ? (
            <Status
              text={`Forecast queued as job ${queuedJobId}. Progress appears below.`}
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
            <DialogTitle>Start this exact forecast?</DialogTitle>
            <DialogDescription>
              Review the model purpose, evidence cutoff, limitations, and fallback before the
              forecast begins.
            </DialogDescription>
          </DialogHeader>
          {preview ? <ForecastPreviewEvidence prepared={preview} /> : null}
          {startMutation.isError ? (
            <Status text="The forecast could not be started. Review it and try again." tone="error" />
          ) : null}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={startMutation.isPending}
              onClick={() => setPreview(null)}
            >
              Go back
            </Button>
            <Button
              disabled={!preview || startMutation.isPending}
              onClick={() => {
                if (preview) startMutation.mutate(preview.receipt)
              }}
            >
              <Play aria-hidden="true" />
              {startMutation.isPending ? "Starting…" : "Confirm and start forecast"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

interface PreparationDraft {
  selection: ForecastPreparationSelection
  policyKey: string
}

interface PreparationContext {
  model: ForecastPreparationModel
  dataset: ForecastPreparationDataset
  policy: ForecastPreparationPolicy
}

function PreparationFields({
  options,
  draft,
  context,
  disabled,
  onChange,
}: {
  options: ForecastPreparationOptions
  draft: PreparationDraft
  context: PreparationContext
  disabled: boolean
  onChange: (draft: PreparationDraft) => void
}) {
  const validityOptions = supportedValidityOptions(context.policy.maximumValidityNanos)
  const instrument = selectedInstrument(context, draft)
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      <Field label="Admitted model" htmlFor="forecast-model">
        <select
          id="forecast-model"
          className={CONTROL_CLASS}
          value={modelKey(context.model)}
          disabled={disabled}
          onChange={(event) => {
            const model = options.models.find(
              (candidate) => modelKey(candidate) === event.target.value,
            )
            if (model) onChange(draftForModel(model))
          }}
        >
          {options.models.map((model) => (
            <option key={modelKey(model)} value={modelKey(model)}>
              {model.bundleId} · v{model.bundleVersion} · {humanize(model.format)}
            </option>
          ))}
        </select>
      </Field>
      <Field label="Historical information" htmlFor="forecast-dataset">
        <select
          id="forecast-dataset"
          className={CONTROL_CLASS}
          value={datasetKey(context.dataset)}
          disabled={disabled}
          onChange={(event) => {
            const dataset = context.model.datasets.find(
              (candidate) => datasetKey(candidate) === event.target.value,
            )
            if (dataset) onChange(draftForDataset(context.model, dataset))
          }}
        >
          {context.model.datasets.map((dataset) => (
            <option key={datasetKey(dataset)} value={datasetKey(dataset)}>
              {dataset.label}
            </option>
          ))}
        </select>
      </Field>
      <Field label="Instrument" htmlFor="forecast-instrument">
        <select
          id="forecast-instrument"
          className={CONTROL_CLASS}
          value={draft.selection.instrumentId}
          disabled={disabled}
          onChange={(event) =>
            onChange({
              ...draft,
              selection: { ...draft.selection, instrumentId: event.target.value },
            })
          }
        >
          {context.dataset.instruments.map((instrument) => (
            <option key={instrument.instrumentId} value={instrument.instrumentId}>
              {instrument.label} · {instrument.observedPoints.toLocaleString()} observations
            </option>
          ))}
        </select>
      </Field>
      <Field label="Forecast cadence" htmlFor="forecast-policy">
        <select
          id="forecast-policy"
          className={CONTROL_CLASS}
          value={draft.policyKey}
          disabled={disabled}
          onChange={(event) => {
            const policy = context.dataset.policies.find(
              (candidate) => policyKey(candidate) === event.target.value,
            )
            if (policy) onChange(draftForPolicy(draft, policy))
          }}
        >
          {context.dataset.policies.map((policy) => (
            <option key={policyKey(policy)} value={policyKey(policy)}>
              Every {formatDuration(policy.horizonStepNanos)} · up to{" "}
              {policy.maximumHorizonPoints} points
            </option>
          ))}
        </select>
      </Field>
      <Field label="Forecast points" htmlFor="forecast-horizon-points">
        <input
          id="forecast-horizon-points"
          className={CONTROL_CLASS}
          type="number"
          min={1}
          max={context.policy.maximumHorizonPoints}
          step={1}
          value={draft.selection.horizonPoints}
          disabled={disabled}
          onChange={(event) =>
            onChange({
              ...draft,
              selection: {
                ...draft.selection,
                horizonPoints: Number(event.target.value),
              },
            })
          }
        />
      </Field>
      <Field label="Result validity" htmlFor="forecast-validity">
        <select
          id="forecast-validity"
          className={CONTROL_CLASS}
          value={draft.selection.validityNanos}
          disabled={disabled}
          onChange={(event) =>
            onChange({
              ...draft,
              selection: { ...draft.selection, validityNanos: event.target.value },
            })
          }
        >
          {validityOptions.map((value) => (
            <option key={value} value={value}>{formatDuration(value)}</option>
          ))}
        </select>
      </Field>
      <div className="rounded-lg border border-border bg-background/25 p-3 md:col-span-2 xl:col-span-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Selected evidence window
        </p>
        <p className="mt-1 text-xs leading-5">
          {formatTimestamp(instrument.observedFromUnixNanos)} through{" "}
          {formatTimestamp(instrument.observedThroughUnixNanos)} · available by{" "}
          {formatTimestamp(instrument.availableAtUnixNanos)} · decimal scale{" "}
          {instrument.decimalScale}
        </p>
      </div>
    </div>
  )
}

function ForecastPreviewEvidence({ prepared }: { prepared: ForecastPreparationPreview }) {
  const { preview, receipt } = prepared
  const facts = [
    ["Model", `${preview.model.bundleId} v${preview.model.bundleVersion}`],
    ["Instrument", `${preview.instrumentLabel} · ${short(preview.instrumentId)}`],
    ["Observed history", `${preview.observedPoints.toLocaleString()} points`],
    ["Observed cutoff", formatTimestamp(preview.observedThroughUnixNanos)],
    ["Evidence available", formatTimestamp(preview.availableAtUnixNanos)],
    ["Forecast horizon", `${preview.horizonPoints} × ${formatDuration(preview.horizonStepNanos)}`],
    ["Result validity", formatDuration(preview.validityNanos)],
    [
      "Uncertainty",
      preview.model.hasCalibratedIntervals
        ? "Calibrated intervals available"
        : "No calibrated bands; none will be shown",
    ],
  ] as const
  return (
    <div className="space-y-4 py-2">
      <div className="rounded-xl border border-primary/20 bg-primary/[0.04] p-4">
        <p className="flex items-center gap-2 text-sm font-semibold">
          <ChartSpline className="size-4 text-primary" aria-hidden="true" />
          Purpose and failure behavior
        </p>
        <p className="mt-2 text-sm leading-6">{preview.model.intendedUse}</p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          If inference cannot produce a valid result: {preview.model.fallbackReason}. Modeled output
          cannot place a trade.
        </p>
      </div>
      <dl className="grid gap-3 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <div key={label} className="rounded-lg border border-border bg-background/35 p-3">
            <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
            <dd className="mt-1 break-words text-xs">{value}</dd>
          </div>
        ))}
      </dl>
      <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3">
        <p className="text-xs font-medium text-amber-200">Known limitations</p>
        {preview.model.limitations.length > 0 ? (
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {preview.model.limitations.map((limitation) => <li key={limitation}>{limitation}</li>)}
          </ul>
        ) : (
          <p className="mt-2 text-xs text-muted-foreground">
            No additional limitation text was retained.
          </p>
        )}
      </div>
      <div className="grid gap-2 text-[10px] text-muted-foreground sm:grid-cols-2">
        <div className="rounded-lg border border-border p-2.5">
          <p className="uppercase tracking-wider">Review expires</p>
          <p className="mt-1 text-foreground">{formatTimestamp(receipt.expiresAtUnixNanos)}</p>
        </div>
      </div>
      <p className="flex gap-2 text-xs leading-5 text-muted-foreground">
        <CalendarClock className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
        If this review expires, prepare it again before starting the forecast.
      </p>
    </div>
  )
}

function defaultDraft(
  options: ForecastPreparationOptions,
  preferred: { modelId: string; bundleId: string; bundleVersion: number } | null,
): PreparationDraft | null {
  const model = options.models.find((candidate) =>
    preferred !== null &&
    candidate.modelId === preferred.modelId &&
    candidate.bundleId === preferred.bundleId &&
    candidate.bundleVersion === preferred.bundleVersion,
  ) ?? options.models[0]
  return model ? draftForModel(model) : null
}

function draftForModel(model: ForecastPreparationModel): PreparationDraft {
  const dataset = model.datasets[0]
  if (!dataset) throw new Error("The admitted model has no compatible forecast dataset.")
  return draftForDataset(model, dataset)
}

function draftForDataset(
  model: ForecastPreparationModel,
  dataset: ForecastPreparationDataset,
): PreparationDraft {
  const policy = dataset.policies[0]
  const instrument = dataset.instruments[0]
  if (!policy || !instrument) {
    throw new Error("The forecast dataset has no compatible instrument or policy.")
  }
  return {
    policyKey: policyKey(policy),
    selection: {
      modelId: model.modelId,
      bundleId: model.bundleId,
      bundleVersion: model.bundleVersion,
      datasetManifest: dataset.manifest,
      instrumentId: instrument.instrumentId,
      horizonPoints: Math.min(20, policy.maximumHorizonPoints),
      horizonStepNanos: policy.horizonStepNanos,
      validityNanos: defaultValidity(policy.maximumValidityNanos),
    },
  }
}

function draftForPolicy(
  draft: PreparationDraft,
  policy: ForecastPreparationPolicy,
): PreparationDraft {
  return {
    policyKey: policyKey(policy),
    selection: {
      ...draft.selection,
      horizonPoints: Math.min(draft.selection.horizonPoints, policy.maximumHorizonPoints),
      horizonStepNanos: policy.horizonStepNanos,
      validityNanos: defaultValidity(policy.maximumValidityNanos),
    },
  }
}

function resolveDraft(
  options: ForecastPreparationOptions,
  draft: PreparationDraft,
): PreparationContext | null {
  const model = options.models.find((candidate) =>
    candidate.modelId === draft.selection.modelId &&
    candidate.bundleId === draft.selection.bundleId &&
    candidate.bundleVersion === draft.selection.bundleVersion,
  )
  const dataset = model?.datasets.find((candidate) =>
    datasetKey(candidate) === datasetManifestKey(draft.selection.datasetManifest),
  )
  const policy = dataset?.policies.find((candidate) => policyKey(candidate) === draft.policyKey)
  return model && dataset && policy ? { model, dataset, policy } : null
}

function selectionIsValid(
  context: PreparationContext,
  draft: PreparationDraft | null,
): boolean {
  if (!draft) return false
  return context.dataset.instruments.some(
    (instrument) => instrument.instrumentId === draft.selection.instrumentId,
  ) && Number.isInteger(draft.selection.horizonPoints) &&
    draft.selection.horizonPoints > 0 &&
    draft.selection.horizonPoints <= context.policy.maximumHorizonPoints &&
    draft.selection.horizonStepNanos === context.policy.horizonStepNanos &&
    BigInt(draft.selection.validityNanos) > 0n &&
    BigInt(draft.selection.validityNanos) <= BigInt(context.policy.maximumValidityNanos)
}

function selectedInstrument(context: PreparationContext, draft: PreparationDraft) {
  const instrument = context.dataset.instruments.find(
    (instrument) => instrument.instrumentId === draft.selection.instrumentId,
  ) ?? context.dataset.instruments[0]
  if (!instrument) throw new Error("The forecast dataset has no compatible instrument.")
  return instrument
}

function supportedValidityOptions(maximum: string): string[] {
  const maximumValue = BigInt(maximum)
  const presets = [
    15n * 60n * 1_000_000_000n,
    60n * 60n * 1_000_000_000n,
    24n * 60n * 60n * 1_000_000_000n,
    7n * 24n * 60n * 60n * 1_000_000_000n,
    30n * 24n * 60n * 60n * 1_000_000_000n,
  ].filter((value) => value <= maximumValue)
  return Array.from(new Set([...presets, maximumValue].map(String)))
}

function defaultValidity(maximum: string): string {
  const values = supportedValidityOptions(maximum)
  const oneDay = String(24n * 60n * 60n * 1_000_000_000n)
  return values.includes(oneDay) ? oneDay : values.at(-1) ?? maximum
}

function modelKey(model: ForecastPreparationModel): string {
  return `${model.modelId}:${model.bundleId}:${model.bundleVersion}`
}

function datasetKey(dataset: ForecastPreparationDataset): string {
  return datasetManifestKey(dataset.manifest)
}

function datasetManifestKey(
  manifest: ForecastPreparationSelection["datasetManifest"],
): string {
  return `${manifest.dataset}:${manifest.manifestVersion}:${manifest.contentHash}`
}

function policyKey(policy: ForecastPreparationPolicy): string {
  return [
    policy.horizonStepNanos,
    policy.maximumHorizonPoints,
    policy.maximumValidityNanos,
    policy.minimumObservedPoints,
  ].join(":")
}

function Field({
  label,
  htmlFor,
  children,
}: {
  label: string
  htmlFor: string
  children: React.ReactNode
}) {
  return (
    <label className="grid gap-1.5 text-xs" htmlFor={htmlFor}>
      <span>{label}</span>
      {children}
    </label>
  )
}

function Status({
  text,
  tone = "neutral",
}: {
  text: string
  tone?: "neutral" | "error" | "success"
}) {
  const color = tone === "error"
    ? "text-red-300"
    : tone === "success"
      ? "text-emerald-300"
      : "text-muted-foreground"
  return (
    <p
      className={`mt-4 rounded-lg border border-border bg-background/25 p-3 text-xs leading-5 ${color}`}
    >
      {text}
    </p>
  )
}

type ForecastPreparationRequest =
  | { action: "options" }
  | { action: "preview"; selection: ForecastPreparationSelection }
  | { action: "start"; receipt: ForecastPreparationReceipt }

type ForecastPreparationTransport = ProductTransport & {
  forecastPreparation(
    request: ForecastPreparationRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
}

function asForecastPreparationTransport(
  transport: ProductTransport,
): ForecastPreparationTransport | null {
  const candidate = transport as ProductTransport & { forecastPreparation?: unknown }
  return typeof candidate.forecastPreparation === "function"
    ? (candidate as ForecastPreparationTransport)
    : null
}

function formatDuration(value: string): string {
  const nanos = BigInt(value)
  if (nanos <= 0n) return "Unavailable"
  const seconds = Number(nanos) / 1_000_000_000
  if (!Number.isFinite(seconds)) return `${value} ns`
  if (seconds < 60) return `${seconds.toLocaleString()} sec`
  const minutes = seconds / 60
  if (minutes < 60) return `${minutes.toLocaleString(undefined, { maximumFractionDigits: 2 })} min`
  const hours = minutes / 60
  if (hours < 48) return `${hours.toLocaleString(undefined, { maximumFractionDigits: 2 })} hr`
  return `${(hours / 24).toLocaleString(undefined, { maximumFractionDigits: 2 })} days`
}

function short(value: string): string {
  return value.length <= 20 ? value : `${value.slice(0, 11)}…${value.slice(-7)}`
}
