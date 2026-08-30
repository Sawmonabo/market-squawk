import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  BadgeCheck,
  CircleAlert,
  FileSearch,
  Landmark,
  Layers3,
  RefreshCw,
  ScrollText,
} from "lucide-react"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import {
  hasProductCapability,
  productCapabilitySet,
} from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type FairValueGovernanceProposal,
  type FairValueMeasurement,
  type FairValueMeasurementSummary,
  type GovernanceAuthorization,
  parseFairValueGovernanceCommit,
  parseFairValueGovernancePreview,
  parseGovernanceAuthorization,
  parseGovernancePrincipals,
  parseFairValueWorkspace,
} from "./fair-value-contracts"
import { FairValueDetail } from "./fair-value-detail"
import { FairValueGovernanceWorkflow } from "./fair-value-governance"
import { PortfolioMeasurementWorkflow } from "./portfolio-measurement-workflow"

export function FairValuePage() {
  const product = useProduct()

  if (product.status === "loading") return <FairValueLoading />
  if (product.status === "error") {
    return (
      <FairValueFrame>
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Fair Value is unavailable</AlertTitle>
          <AlertDescription>
            The workspace could not start. Try again, or open Logs if the problem continues.
          </AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </FairValueFrame>
    )
  }

  const available = hasProductCapability(
    product.bootstrap,
    "fair_value_workspace",
  )
  if (!available) {
    return (
      <FairValueFrame>
        <EmptyState
          title="Fair-value workspace is not available"
          detail="This installation cannot open saved fair-value measurements. Update or restore the Fair Value component before relying on classifications."
        />
      </FairValueFrame>
    )
  }

  return (
    <FairValueWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function FairValueWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [selectedToken, setSelectedToken] = React.useState<string | null>(null)
  const [evaluatedAt] = React.useState(() => new Date().toISOString())
  const measurements = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "FairValue",
      "FairValue.GetWorkspace",
      { measurementToken: selectedToken, at: evaluatedAt },
    ),
    queryFn: async () =>
      parseFairValueWorkspace(
        await transport.query({
          query: "fairValueWorkspace",
          at: evaluatedAt,
          ...(selectedToken ? { measurementToken: selectedToken } : {}),
        }),
      ),
  })

  const rows = measurements.data?.measurements ?? []
  const selected = measurements.data?.selectedMeasurement ?? null
  const accountCount = new Set(rows.map((measurement) => measurement.accountId)).size
  const instrumentCount = new Set(
    rows.map((measurement) => measurement.instrumentId),
  ).size
  const latestMeasurement = rows.reduce<string | null>((latest, measurement) => {
    if (!latest || measurement.measurementAt > latest) return measurement.measurementAt
    return latest
  }, null)

  return (
    <FairValueFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => void measurements.refetch()}
          disabled={measurements.isFetching}
        >
          <RefreshCw
            className={measurements.isFetching ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh measurements
        </Button>
      }
    >
      <PortfolioMeasurementWorkflow
        bootstrap={bootstrap}
        transport={transport}
        onCreated={async (measurementToken) => {
          setSelectedToken(measurementToken)
          await measurements.refetch()
        }}
      />
      {measurements.isPending ? (
        <FairValueContentLoading />
      ) : measurements.isError ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Measurements could not be loaded</AlertTitle>
          <AlertDescription>
            Saved measurements are temporarily unavailable. Try again, or open Logs for details.
          </AlertDescription>
        </Alert>
      ) : rows.length === 0 ? (
        <EmptyState
          title="No fair-value measurements yet"
          detail="Create a measurement from a current portfolio holding. Saved measurements will appear here with their method, amount, supporting inputs, and classification."
        />
      ) : (
        <>
          <section
            aria-label="Fair-value workspace summary"
            className="grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-4"
          >
            <SummaryFact
              icon={Landmark}
              label="Measurements"
              value={String(rows.length)}
              detail="Saved valuations"
            />
            <SummaryFact
              icon={Layers3}
              label="Accounts"
              value={String(accountCount)}
              detail="Reporting accounts in view"
            />
            <SummaryFact
              icon={BadgeCheck}
              label="Instruments"
              value={String(instrumentCount)}
              detail="Measured instruments in view"
            />
            <SummaryFact
              icon={ScrollText}
              label="Latest measurement"
              value={latestMeasurement ? shortDate(latestMeasurement) : "Not available"}
              detail="Newest timestamp in this result"
            />
          </section>

          {measurements.data.availableItems > measurements.data.returnedItems ? (
            <Alert className="mt-4">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>Some measurements are not shown</AlertTitle>
              <AlertDescription>
                Showing {measurements.data.returnedItems.toLocaleString()} of{" "}
                {measurements.data.availableItems.toLocaleString()} saved measurements. Load or
                filter additional records before treating this view as complete.
              </AlertDescription>
            </Alert>
          ) : null}

          <div className="mt-4 grid min-h-[650px] gap-4 xl:grid-cols-[minmax(260px,0.68fr)_minmax(0,1.62fr)]">
            <MeasurementIndex
              measurements={rows}
              selectedToken={selected?.measurementToken ?? null}
              onSelect={setSelectedToken}
            />
            {selected ? (
              <SelectedFairValue
                key={selected.measurementToken}
                bootstrap={bootstrap}
                transport={transport}
                measurement={selected}
              />
            ) : null}
          </div>

          <p className="mt-4 text-[10px] leading-5 text-muted-foreground">
            Fair-value classification explains the valuation inputs and judgment. It does not make
            the result a live quote or authorize a trade.
          </p>
        </>
      )}
    </FairValueFrame>
  )
}

function SelectedFairValue({
  bootstrap,
  transport,
  measurement,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  measurement: FairValueMeasurement
}) {
  const queryClient = useQueryClient()
  const [announcement, setAnnouncement] = React.useState("")
  const [governanceAuthorizations, setGovernanceAuthorizations] = React.useState<
    GovernanceAuthorization[]
  >([])
  const capabilities = React.useMemo(
    () => productCapabilitySet(bootstrap),
    [bootstrap],
  )
  const governanceAvailable =
    capabilities.has("governance_principals") &&
    capabilities.has("governance_authenticate") &&
    capabilities.has("fair_value_governance_preview") &&
    capabilities.has("fair_value_governance_commit")
  const governancePrincipals = useQuery({
    queryKey: productKeys.operation(bootstrap.runtime, "Governance", "Governance.ListPrincipals", {}),
    queryFn: async () =>
      parseGovernancePrincipals(
        await transport.governanceQuery({ query: "principals" }),
      ),
    enabled: governanceAvailable,
  })
  const governancePreview = useMutation({
    mutationFn: async (proposal: FairValueGovernanceProposal) =>
      parseFairValueGovernancePreview(
        await transport.fairValueControl(
          { action: "previewGovernanceAction", proposal },
          true,
        ),
      ),
    onSuccess: () => {
      setGovernanceAuthorizations([])
      setAnnouncement("The governance review is ready. Authenticate every required reviewer before recording it.")
    },
  })
  const governanceAuthenticate = useMutation({
    mutationFn: async ({
      principalId,
      credential,
    }: {
      principalId: string
      credential: string
    }) => {
      const preview = governancePreview.data
      if (!preview) throw new Error("Create a governance preview before authenticating a principal.")
      return parseGovernanceAuthorization(
        await transport.governanceControl(
          {
            action: "authenticateAction",
            previewId: preview.previewId,
            principalId,
            credential,
          },
          true,
        ),
        preview.previewId,
      )
    },
    onSuccess: (authorization) => {
      setGovernanceAuthorizations((current) => [
        ...current.filter(
          (item) =>
            item.principalId !== authorization.principalId,
        ),
        authorization,
      ])
      setAnnouncement("The reviewer was authenticated for this proposal.")
    },
  })
  const governanceCommit = useMutation({
    mutationFn: async () => {
      const preview = governancePreview.data
      if (!preview) throw new Error("Create a governance preview before committing it.")
      return parseFairValueGovernanceCommit(
        await transport.fairValueControl(
          {
            action: "commitGovernanceAction",
            previewId: preview.previewId,
            authorizationHandles: governanceAuthorizations.map(
              (authorization) => authorization.authorizationHandle,
            ),
          },
          true,
        ),
        preview.previewId,
      )
    },
    onSuccess: async () => {
      setGovernanceAuthorizations([])
      governancePreview.reset()
      setAnnouncement("The governed action was recorded.")
      await queryClient.invalidateQueries({
        queryKey: productKeys.domain(bootstrap.runtime, "FairValue"),
      })
    },
  })

  return (
    <div className="min-w-0 space-y-3">
      <p className="sr-only" aria-live="polite">
        {announcement}
      </p>
      <FairValueDetail measurement={measurement} />
      {governanceAvailable ? (
        <FairValueGovernanceWorkflow
          measurement={measurement}
          classification={measurement.classification ?? undefined}
          inputs={measurement.inputs}
          approvals={measurement.approvals}
          principals={governancePrincipals.data}
          state={{
            preview: governancePreview.data ?? null,
            authorizations: governanceAuthorizations,
            busy:
              governancePreview.isPending ||
              governanceAuthenticate.isPending ||
              governanceCommit.isPending,
            error: governanceAuthenticate.isError
              ? "Authentication was not accepted. Reauthenticate an eligible reviewer for this proposal."
              : governanceCommit.isError
                ? "The action could not be recorded. Try again, or open Logs for details."
                : governancePreview.isError
                  ? "The proposal could not be prepared. Review it and try again, or open Logs for details."
                  : governancePrincipals.isError
                    ? "Eligible reviewers could not be loaded. Refresh, or open Logs for details."
                    : null,
          }}
          onPreview={(proposal) => governancePreview.mutate(proposal)}
          onAuthenticate={(principalId, credential) =>
            governanceAuthenticate.mutate({ principalId, credential })
          }
          onCommit={() => governanceCommit.mutate()}
        />
      ) : (
        <Alert className="border-dashed">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Guided governance is unavailable</AlertTitle>
          <AlertDescription>
            This installation cannot complete the governed review workflow. Update or restore Fair
            Value before recording an approval or override.
          </AlertDescription>
        </Alert>
      )}
    </div>
  )
}

function MeasurementIndex({
  measurements,
  selectedToken,
  onSelect,
}: {
  measurements: FairValueMeasurementSummary[]
  selectedToken: string | null
  onSelect: (token: string) => void
}) {
  return (
    <section className="overflow-hidden rounded-xl border border-border bg-card/35">
      <div className="border-b border-border px-4 py-3">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Measurement register
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Newest saved valuations.
        </p>
      </div>
      <ul className="max-h-[720px] space-y-1 overflow-y-auto p-2">
        {measurements.map((measurement) => {
          const active = measurement.measurementToken === selectedToken
          return (
            <li key={measurement.measurementToken}>
              <button
                type="button"
                aria-pressed={active}
                onClick={() => onSelect(measurement.measurementToken)}
                className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  active
                    ? "border-primary/45 bg-primary/10"
                    : "border-transparent hover:border-border hover:bg-accent/40"
                }`}
              >
                <span className="flex items-center justify-between gap-2">
                  <span className="truncate text-xs font-semibold">
                    {measurement.instrumentId}
                  </span>
                  <HierarchyPip
                    hierarchy={measurement.classification?.hierarchy ?? null}
                  />
                </span>
                <span className="mt-2 block font-mono text-sm">
                  {formatMoney(measurement.amount)}
                </span>
                <span className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
                  <span>{humanize(measurement.method)}</span>
                  <span>{shortDate(measurement.measurementAt)}</span>
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </section>
  )
}

function SummaryFact({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Landmark
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="border-border p-4 sm:border-r sm:last:border-r-0">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-xl font-semibold">{value}</p>
      <p className="mt-1 text-[10px] text-muted-foreground">{detail}</p>
    </div>
  )
}

function HierarchyPip({ hierarchy }: { hierarchy: string | null }) {
  return (
    <span className="shrink-0 rounded-full border border-border bg-background/55 px-2 py-0.5 font-mono text-[8px] uppercase text-muted-foreground">
      {hierarchy ? humanize(hierarchy) : "Unloaded"}
    </span>
  )
}

function FairValueFrame({
  children,
  action,
}: {
  children: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div className="mx-auto w-full max-w-[1320px] p-5 lg:p-7">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-border pb-6">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            ASC 820 · IFRS 13 evidence workspace
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Fair Value</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Inspect measurement methods, hierarchy decisions, evidence quality, market access, and
            governed review history without confusing analytical classification with live market
            depth or execution eligibility.
          </p>
        </div>
        {action}
      </header>
      <div className="mt-5">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <FileSearch className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function FairValueLoading() {
  return (
    <FairValueFrame>
      <FairValueContentLoading />
    </FairValueFrame>
  )
}

function FairValueContentLoading() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-28 rounded-xl" />
      <div className="grid gap-4 xl:grid-cols-[minmax(260px,0.68fr)_minmax(0,1.62fr)]">
        <Skeleton className="h-[650px] rounded-xl" />
        <Skeleton className="h-[650px] rounded-xl" />
      </div>
    </div>
  )
}

function shortDate(value: string) {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return value
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(timestamp)
}
