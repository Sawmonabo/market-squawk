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
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseForecastPreparationOptions,
  parseForecastPreparationPreview,
  parseForecastStart,
  type ForecastPreparationHistory,
  type ForecastPreparationModel,
  type ForecastPreparationOptions,
  type ForecastPreparationPolicy,
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
  const guidedTransport = asForecastPreparationTransport(transport)
  const [draft, setDraft] = React.useState<PreparationDraft | null>(null)
  const [preview, setPreview] =
    React.useState<ForecastPreparationPreview | null>(null)
  const [started, setStarted] = React.useState(false)
  const optionsQuery = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecastPreparation",
      {},
    ),
    enabled: capabilitiesAvailable && guidedTransport !== null,
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
    mutationFn: async (confirmationToken: string) => {
      if (!guidedTransport) {
        throw new Error("Guided forecast preparation is unavailable.")
      }
      return parseForecastStart(
        await guidedTransport.forecastPreparation(
          { action: "start", confirmationToken },
          true,
        ),
      )
    },
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

  const context =
    draft && optionsQuery.data
      ? resolveDraft(optionsQuery.data, draft.selection)
      : null
  const ready = context !== null && selectionIsValid(context, draft?.selection)

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

      {!capabilitiesAvailable || guidedTransport === null ? (
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
                  startMutation.mutate(preview.receipt.confirmationToken)
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

interface PreparationContext {
  model: ForecastPreparationModel
  history: ForecastPreparationHistory
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
  const validityOptions = supportedValidityOptions(
    context.policy.maximumValidityNanos,
  )
  const investment = selectedInvestment(context, draft.selection)
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      <Field label="Forecast method" htmlFor="forecast-model">
        <select
          id="forecast-model"
          className={CONTROL_CLASS}
          value={context.model.modelToken}
          disabled={disabled}
          onChange={(event) => {
            const model = options.models.find(
              (candidate) => candidate.modelToken === event.target.value,
            )
            if (model) onChange(draftForModel(model))
          }}
        >
          {options.models.map((model) => (
            <option key={model.modelToken} value={model.modelToken}>
              {model.name}
            </option>
          ))}
        </select>
      </Field>
      <Field label="Historical information" htmlFor="forecast-history">
        <select
          id="forecast-history"
          className={CONTROL_CLASS}
          value={context.history.historyToken}
          disabled={disabled}
          onChange={(event) => {
            const history = context.model.histories.find(
              (candidate) => candidate.historyToken === event.target.value,
            )
            if (history) onChange(draftForHistory(context.model, history))
          }}
        >
          {context.model.histories.map((history, index) => (
            <option key={history.historyToken} value={history.historyToken}>
              History {index + 1} · {history.instruments.length.toLocaleString()} investments
            </option>
          ))}
        </select>
      </Field>
      <Field label="Investment" htmlFor="forecast-investment">
        <select
          id="forecast-investment"
          className={CONTROL_CLASS}
          value={draft.selection.investmentToken}
          disabled={disabled}
          onChange={(event) =>
            onChange({
              selection: {
                ...draft.selection,
                investmentToken: event.target.value,
              },
            })
          }
        >
          {context.history.instruments.map((instrument) => (
            <option
              key={instrument.investmentToken}
              value={instrument.investmentToken}
            >
              {instrument.label} · {instrument.observedPoints.toLocaleString()} observations
            </option>
          ))}
        </select>
      </Field>
      <Field label="Forecast cadence" htmlFor="forecast-policy">
        <select
          id="forecast-policy"
          className={CONTROL_CLASS}
          value={context.policy.policyToken}
          disabled={disabled}
          onChange={(event) => {
            const policy = context.history.policies.find(
              (candidate) => candidate.policyToken === event.target.value,
            )
            if (policy) onChange(draftForPolicy(draft, policy))
          }}
        >
          {context.history.policies.map((policy) => (
            <option key={policy.policyToken} value={policy.policyToken}>
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
              selection: {
                ...draft.selection,
                validityNanos: event.target.value,
              },
            })
          }
        >
          {validityOptions.map((value) => (
            <option key={value} value={value}>
              {formatDuration(value)}
            </option>
          ))}
        </select>
      </Field>
      <div className="rounded-lg border border-border bg-background/25 p-3 md:col-span-2 xl:col-span-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Selected evidence window
        </p>
        <p className="mt-1 text-xs leading-5">
          {formatTimestamp(investment.observedFromUnixNanos)} through{" "}
          {formatTimestamp(investment.observedThroughUnixNanos)} · available by{" "}
          {formatTimestamp(investment.availableAtUnixNanos)}
        </p>
      </div>
    </div>
  )
}

function ForecastPreviewEvidence({
  prepared,
}: {
  prepared: ForecastPreparationPreview
}) {
  const { preview, receipt } = prepared
  const facts = [
    ["Forecast method", preview.model.name],
    ["Investment", preview.instrumentLabel],
    ["Observed history", `${preview.observedPoints.toLocaleString()} points`],
    ["Observed cutoff", formatTimestamp(preview.observedThroughUnixNanos)],
    ["Evidence available", formatTimestamp(preview.availableAtUnixNanos)],
    [
      "Forecast horizon",
      `${preview.horizonPoints} × ${formatDuration(preview.horizonStepNanos)}`,
    ],
    ["Result validity", formatDuration(preview.validityNanos)],
    [
      "Uncertainty",
      preview.evidenceState === "calibrated"
        ? "Calibrated ranges available"
        : "Limited; do not treat the result as confident",
    ],
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
        {preview.model.limitations.length > 0 ? (
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {preview.model.limitations.map((limitation) => (
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
        This review expires {formatTimestamp(receipt.expiresAtUnixNanos)}. If
        it expires, prepare the forecast again before starting.
      </p>
    </div>
  )
}

function defaultDraft(
  options: ForecastPreparationOptions,
): PreparationDraft | null {
  const model = options.models[0]
  return model ? draftForModel(model) : null
}

function draftForModel(model: ForecastPreparationModel): PreparationDraft {
  const history = model.histories[0]
  if (!history) {
    throw new Error("The forecast method has no compatible history.")
  }
  return draftForHistory(model, history)
}

function draftForHistory(
  model: ForecastPreparationModel,
  history: ForecastPreparationHistory,
): PreparationDraft {
  const policy = history.policies[0]
  const investment = history.instruments[0]
  if (!policy || !investment) {
    throw new Error("The history has no compatible investment or horizon.")
  }
  return {
    selection: {
      modelToken: model.modelToken,
      historyToken: history.historyToken,
      investmentToken: investment.investmentToken,
      policyToken: policy.policyToken,
      horizonPoints: Math.min(20, policy.maximumHorizonPoints),
      validityNanos: defaultValidity(policy.maximumValidityNanos),
    },
  }
}

function draftForPolicy(
  draft: PreparationDraft,
  policy: ForecastPreparationPolicy,
): PreparationDraft {
  return {
    selection: {
      ...draft.selection,
      policyToken: policy.policyToken,
      horizonPoints: Math.min(
        draft.selection.horizonPoints,
        policy.maximumHorizonPoints,
      ),
      validityNanos: defaultValidity(policy.maximumValidityNanos),
    },
  }
}

function resolveDraft(
  options: ForecastPreparationOptions,
  selection: ForecastPreparationSelection,
): PreparationContext | null {
  const model = options.models.find(
    (candidate) => candidate.modelToken === selection.modelToken,
  )
  const history = model?.histories.find(
    (candidate) => candidate.historyToken === selection.historyToken,
  )
  const policy = history?.policies.find(
    (candidate) => candidate.policyToken === selection.policyToken,
  )
  return model && history && policy ? { model, history, policy } : null
}

function selectionIsValid(
  context: PreparationContext,
  selection: ForecastPreparationSelection | undefined,
): boolean {
  if (!selection) return false
  return (
    context.history.instruments.some(
      (investment) =>
        investment.investmentToken === selection.investmentToken,
    ) &&
    Number.isInteger(selection.horizonPoints) &&
    selection.horizonPoints > 0 &&
    selection.horizonPoints <= context.policy.maximumHorizonPoints &&
    BigInt(selection.validityNanos) > 0n &&
    BigInt(selection.validityNanos) <=
      BigInt(context.policy.maximumValidityNanos)
  )
}

function selectedInvestment(
  context: PreparationContext,
  selection: ForecastPreparationSelection,
) {
  const investment =
    context.history.instruments.find(
      (candidate) =>
        candidate.investmentToken === selection.investmentToken,
    ) ?? context.history.instruments[0]
  if (!investment) {
    throw new Error("The history has no compatible investment.")
  }
  return investment
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
  return values.includes(oneDay) ? oneDay : (values.at(-1) ?? maximum)
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

type ForecastPreparationRequest =
  | { action: "options" }
  | { action: "preview"; selection: ForecastPreparationSelection }
  | { action: "start"; confirmationToken: string }

type ForecastPreparationTransport = ProductTransport & {
  forecastPreparation(
    request: ForecastPreparationRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
}

function asForecastPreparationTransport(
  transport: ProductTransport,
): ForecastPreparationTransport | null {
  const candidate = transport as ProductTransport & {
    forecastPreparation?: unknown
  }
  return typeof candidate.forecastPreparation === "function"
    ? (candidate as ForecastPreparationTransport)
    : null
}

function formatDuration(value: string): string {
  const nanos = BigInt(value)
  if (nanos <= 0n) return "Unavailable"
  const second = 1_000_000_000n
  const minute = 60n * second
  const hour = 60n * minute
  const day = 24n * hour
  if (nanos % day === 0n) return `${nanos / day} days`
  if (nanos % hour === 0n) return `${nanos / hour} hours`
  if (nanos % minute === 0n) return `${nanos / minute} minutes`
  if (nanos % second === 0n) return `${nanos / second} seconds`
  return "Custom interval"
}
