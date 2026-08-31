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
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseForecastPreparationOptions,
  parseForecastPreparationPreview,
  parseForecastStart,
  type ForecastPreparationOptions,
  type ForecastPreparationPreview,
  type ForecastPreparationSelection,
} from "./forecast-preparation-contracts"

const PREPARATION_CAPABILITIES = [
  "forecast_preparation",
  "forecast_prepare",
  "forecast_prepared_start",
] as const
const CONTROL_CLASS =
  "h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

export function ForecastPreparation({
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
  const [draft, setDraft] = React.useState<PreparationDraft | null>(null)
  const [preview, setPreview] =
    React.useState<ForecastPreparationPreview | null>(null)
  const [started, setStarted] = React.useState(false)
  const optionsQuery = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "Model",
      "Model.GetForecastPreparation",
      {},
    ),
    enabled: capabilitiesAvailable,
    staleTime: 30_000,
    queryFn: async () =>
      parseForecastPreparationOptions(
        await transport.forecastPreparation({ action: "options" }),
      ),
  })
  const previewMutation = useMutation({
    mutationFn: async (selection: ForecastPreparationSelection) =>
      parseForecastPreparationPreview(
        await transport.forecastPreparation({
          action: "preview",
          selection,
        }),
      ),
    onSuccess: setPreview,
  })
  const startMutation = useMutation({
    mutationFn: async (confirmationToken: string) =>
      parseForecastStart(
        await transport.forecastPreparation(
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
    if (!optionsQuery.data) return
    setDraft(defaultDraft(optionsQuery.data))
    setPreview(null)
    setStarted(false)
    previewMutation.reset()
    startMutation.reset()
  }, [optionsQuery.data])

  const ready =
    draft !== null &&
    optionsQuery.data !== undefined &&
    selectionIsValid(optionsQuery.data, draft.selection)

  return (
    <section className="rounded-xl border border-primary/20 bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-wider text-primary">
            Point-in-time forecast builder
          </p>
          <h2 className="mt-2 text-xl font-semibold">
            Prepare a statistical forecast
          </h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Choose an investment, historical cutoff, and horizon. Review the
            uncertainty and limitations before starting.
          </p>
        </div>
        <BrainCircuit className="size-5 text-primary" aria-hidden="true" />
      </div>

      {!capabilitiesAvailable ? (
        <Alert className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Forecast preparation is unavailable</AlertTitle>
          <AlertDescription>
            Forecast preparation is not available in this workspace.
          </AlertDescription>
        </Alert>
      ) : optionsQuery.isPending ? (
        <Status text="Loading forecast choices…" />
      ) : optionsQuery.isError ? (
        <Status text="Forecast choices are unavailable right now." tone="error" />
      ) : optionsQuery.data.models.length === 0 ? (
        <Alert className="mt-4">
          <Database aria-hidden="true" />
          <AlertTitle>No forecast-ready history is available</AlertTitle>
          <AlertDescription>
            A reviewed method needs enough compatible historical information
            before it can produce a forecast.
          </AlertDescription>
        </Alert>
      ) : draft ? (
        <div className="mt-5 space-y-4">
          <PreparationFields
            options={optionsQuery.data}
            draft={draft}
            disabled={previewMutation.isPending || startMutation.isPending}
            onChange={(next) => {
              setDraft(next)
              setPreview(null)
              setStarted(false)
              previewMutation.reset()
              startMutation.reset()
            }}
          />
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-[11px] leading-5 text-muted-foreground">
              Review the evidence cutoff, horizon, uncertainty, and limitations
              before continuing.
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
            <Status
              text="This forecast could not be prepared. Review the choices and try again."
              tone="error"
            />
          ) : null}
          {started ? (
            <Status
              text="Forecast preparation started. Progress appears below."
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
            <DialogTitle>Start this forecast?</DialogTitle>
            <DialogDescription>
              Review the purpose, evidence cutoff, limitations, and uncertainty
              before the forecast begins.
            </DialogDescription>
          </DialogHeader>
          {preview ? <ForecastPreviewEvidence prepared={preview} /> : null}
          {startMutation.isError ? (
            <Status
              text="The forecast could not be started. Review it and try again."
              tone="error"
            />
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
                if (preview) {
                  startMutation.mutate(preview.confirmationToken)
                }
              }}
            >
              <Play aria-hidden="true" />
              {startMutation.isPending
                ? "Starting…"
                : "Confirm and start forecast"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

interface PreparationDraft {
  selection: ForecastPreparationSelection
}

function PreparationFields({
  options,
  draft,
  disabled,
  onChange,
}: {
  options: ForecastPreparationOptions
  draft: PreparationDraft
  disabled: boolean
  onChange: (draft: PreparationDraft) => void
}) {
  const model = options.models.find(
    (candidate) => candidate.modelToken === draft.selection.modelToken,
  )
  const history = model?.histories.find(
    (candidate) => candidate.historyToken === draft.selection.historyToken,
  )
  const investment = history?.investments.find(
    (candidate) =>
      candidate.investmentToken === draft.selection.investmentToken,
  )
  const horizon = history?.horizons.find(
    (candidate) => candidate.horizonToken === draft.selection.horizonToken,
  )
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Field label="Forecast method" htmlFor="forecast-model">
        <select
          id="forecast-model"
          className={CONTROL_CLASS}
          value={draft.selection.modelToken}
          disabled={disabled}
          onChange={(event) => {
            onChange({
              selection: {
                modelToken: event.target.value,
                historyToken: "",
                investmentToken: "",
                horizonToken: "",
              },
            })
          }}
        >
          <option value="">Choose a forecast method</option>
          {options.models.map((model) => (
            <option key={model.modelToken} value={model.modelToken}>
              {model.name}
            </option>
          ))}
        </select>
        {model ? (
          <span className="text-[11px] leading-5 text-muted-foreground">
            {model.target.label}: {model.target.meaning} Values are shown in{" "}
            {model.target.unitLabel}. Model evidence is{" "}
            {evidenceLevelLabel(model.modelEvidence.overall).toLowerCase()}.
          </span>
        ) : null}
      </Field>
      <Field label="Historical information" htmlFor="forecast-history">
        <select
          id="forecast-history"
          className={CONTROL_CLASS}
          value={draft.selection.historyToken}
          disabled={disabled || !model}
          onChange={(event) => {
            onChange({
              selection: {
                ...draft.selection,
                historyToken: event.target.value,
                investmentToken: "",
                horizonToken: "",
              },
            })
          }}
        >
          <option value="">Choose historical information</option>
          {(model?.histories ?? []).map((history) => (
            <option key={history.historyToken} value={history.historyToken}>
              {history.label}
            </option>
          ))}
        </select>
      </Field>
      <Field label="Investment" htmlFor="forecast-investment">
        <select
          id="forecast-investment"
          className={CONTROL_CLASS}
          value={draft.selection.investmentToken}
          disabled={disabled || !history}
          onChange={(event) =>
            onChange({
              selection: {
                ...draft.selection,
                investmentToken: event.target.value,
              },
            })
          }
        >
          <option value="">Choose an investment</option>
          {(history?.investments ?? []).map((instrument) => (
            <option
              key={instrument.investmentToken}
              value={instrument.investmentToken}
            >
              {instrument.label} ·{" "}
              {instrument.observationCount.toLocaleString()} observations
            </option>
          ))}
        </select>
      </Field>
      <Field label="Forecast horizon" htmlFor="forecast-horizon">
        <select
          id="forecast-horizon"
          className={CONTROL_CLASS}
          value={draft.selection.horizonToken}
          disabled={disabled || !history}
          onChange={(event) =>
            onChange({
              selection: {
                ...draft.selection,
                horizonToken: event.target.value,
              },
            })
          }
        >
          <option value="">Choose a forecast horizon</option>
          {(history?.horizons ?? []).map((candidate) => (
            <option key={candidate.horizonToken} value={candidate.horizonToken}>
              {candidate.label}
            </option>
          ))}
        </select>
        {horizon ? (
          <span className="text-[11px] leading-5 text-muted-foreground">
            {horizon.description}
          </span>
        ) : null}
      </Field>
      {investment ? (
        <div className="rounded-lg border border-border bg-background/25 p-3 md:col-span-2">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Selected evidence window
          </p>
          <p className="mt-1 text-xs leading-5">
            {formatTimestamp(investment.observedFromUnixNanos)} through{" "}
            {formatTimestamp(investment.observedThroughUnixNanos)} · available by{" "}
            {formatTimestamp(investment.availableAtUnixNanos)}
          </p>
        </div>
      ) : null}
    </div>
  )
}

function ForecastPreviewEvidence({
  prepared,
}: {
  prepared: ForecastPreparationPreview
}) {
  const preview = prepared
  const facts = [
    ["Forecast method", preview.model.name],
    ["Investment", preview.instrumentLabel],
    ["Forecast target", preview.model.target.label],
    ["Target meaning", preview.model.target.meaning],
    ["Target unit", preview.model.target.unitLabel],
    [
      "Observed history",
      `${preview.observationCount.toLocaleString()} observations`,
    ],
    ["Observed cutoff", formatTimestamp(preview.observedThroughUnixNanos)],
    ["Evidence available", formatTimestamp(preview.availableAtUnixNanos)],
    ["Forecast horizon", preview.horizon.label],
    ["Horizon meaning", preview.horizon.description],
    ["Overall model evidence", evidenceLevelLabel(preview.model.modelEvidence.overall)],
    ["Point-in-time inputs", evidenceLevelLabel(preview.model.modelEvidence.pitInputs)],
    ["Held-out evaluation", evidenceLevelLabel(preview.model.modelEvidence.outOfSample)],
    ["Horizon alignment", evidenceLevelLabel(preview.model.modelEvidence.horizonAlignment)],
    ["Calibration", calibrationStateLabel(preview.model.modelEvidence.calibration)],
    ["Evidence meaning", preview.model.modelEvidence.interpretation],
  ] as const
  return (
    <div className="space-y-4 py-2">
      <div className="rounded-xl border border-primary/20 bg-primary/[0.04] p-4">
        <p className="flex items-center gap-2 text-sm font-semibold">
          <ChartSpline className="size-4 text-primary" aria-hidden="true" />
          Purpose and unavailable behavior
        </p>
        <p className="mt-2 text-sm leading-6">{preview.model.intendedUse}</p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          If valid evidence or a usable result is unavailable, Market Squawk
          suggests no action. A forecast cannot place a trade.
        </p>
      </div>
      <dl className="grid gap-3 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <div
            key={label}
            className="rounded-lg border border-border bg-background/35 p-3"
          >
            <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {label}
            </dt>
            <dd className="mt-1 break-words text-xs">{value}</dd>
          </div>
        ))}
      </dl>
      <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3">
        <p className="text-xs font-medium text-amber-200">Known limitations</p>
        {preview.limitations.length > 0 ? (
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {preview.limitations.map((limitation) => (
              <li key={limitation}>{limitation}</li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 text-xs text-muted-foreground">
            No additional limitations were supplied.
          </p>
        )}
      </div>
      <p className="flex gap-2 text-xs leading-5 text-muted-foreground">
        <CalendarClock
          className="mt-0.5 size-3.5 shrink-0"
          aria-hidden="true"
        />
        This review expires {formatTimestamp(preview.expiresAtUnixNanos)}. If
        it expires, prepare the forecast again before starting.
      </p>
    </div>
  )
}

function evidenceLevelLabel(
  state: ForecastPreparationPreview["model"]["modelEvidence"]["overall"],
): string {
  switch (state) {
    case "sufficient":
      return "Sufficient for this research use"
    case "limited":
      return "Limited"
    case "unavailable":
      return "Unavailable"
  }
}

function calibrationStateLabel(
  state: ForecastPreparationPreview["model"]["modelEvidence"]["calibration"],
): string {
  switch (state) {
    case "calibrated":
      return "Calibrated ranges available"
    case "limited":
      return "Limited; ranges may be unavailable"
    case "unavailable":
      return "Unavailable"
  }
}

function defaultDraft(
  options: ForecastPreparationOptions,
): PreparationDraft | null {
  if (options.models.length === 0) return null
  return {
    selection: {
      modelToken: "",
      historyToken: "",
      investmentToken: "",
      horizonToken: "",
    },
  }
}

function selectionIsValid(
  options: ForecastPreparationOptions,
  selection: ForecastPreparationSelection,
): boolean {
  const model = options.models.find(
    (candidate) => candidate.modelToken === selection.modelToken,
  )
  const history = model?.histories.find(
    (candidate) => candidate.historyToken === selection.historyToken,
  )
  if (!history) return false
  return (
    history.investments.some(
      (investment) =>
        investment.investmentToken === selection.investmentToken,
    ) &&
    history.horizons.some(
      (horizon) => horizon.horizonToken === selection.horizonToken,
    )
  )
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
  const color =
    tone === "error"
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
