import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  DatabaseZap,
  ReceiptText,
  RefreshCw,
  ShieldCheck,
  WalletCards,
} from "lucide-react"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"
import type { PaperControlRequest, ProductTransport } from "@/lib/transport"

import {
  type PaperAccount,
  type PaperAuditDecision,
  type PaperControlResult,
  type PaperFill,
  type PaperOrder,
  type PaperPosition,
  type PaperStatus,
  parsePaperControlResult,
  parsePaperFills,
  parsePaperOrders,
  parsePaperStatus,
} from "./contracts"
import {
  type PaperControlAvailability,
  PaperConfirmationDialog,
  PaperControlPanel,
  paperActionCompleted,
} from "./paper-controls"
import { ManualPaperDraftPanel } from "./manual-paper-draft"

const CORE_PAPER_CAPABILITIES = [
  "bot_status",
  "bot_start",
  "bot_stop",
  "execution_orders",
  "execution_fills",
  "execution_cancel",
  "execution_reconcile",
  "risk_kill_switch",
] as const satisfies readonly ProductCapability[]

export function PaperExecutionPage() {
  const product = useProduct()

  if (product.status === "loading") return <PaperLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState
          title="Paper execution is unavailable"
          detail="Try again. If the problem continues, review Logs & Diagnostics."
        />
      </PageFrame>
    )
  }

  return (
    <ReadyPaperExecution
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadyPaperExecution({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const [pendingAction, setPendingAction] = React.useState<PaperControlRequest | null>(null)
  const [controlMessage, setControlMessage] = React.useState<string | null>(null)
  const [controlResult, setControlResult] = React.useState<PaperControlResult | null>(null)
  const capabilities = productCapabilitySet(bootstrap)
  const missingCoreCapabilities = CORE_PAPER_CAPABILITIES.filter(
    (capability) => !capabilities.has(capability),
  )
  const controlAvailability: PaperControlAvailability = {
    start: capabilities.has("bot_start"),
    stop: capabilities.has("bot_stop"),
    cancel: capabilities.has("execution_cancel"),
    reconcile: capabilities.has("execution_reconcile"),
    killSwitch: capabilities.has("risk_kill_switch"),
  }
  const statusAvailable = capabilities.has("bot_status")
  const ordersAvailable = capabilities.has("execution_orders")
  const fillsAvailable = capabilities.has("execution_fills")
  const manualPaperAvailable =
    capabilities.has("execution_manual_targets") &&
    capabilities.has("execution_manual_draft")
  const status = useQuery({
    queryKey: productKeys.operation(bootstrap.runtime, "bot", "Bot.GetStatus", {}),
    enabled: statusAvailable,
    queryFn: async () => parsePaperStatus(await transport.query({ query: "paperStatus" })),
  })
  const orders = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "execution",
      "Execution.GetOrders",
      {},
    ),
    enabled: ordersAvailable,
    queryFn: async () => parsePaperOrders(await transport.query({ query: "paperOrders" })),
  })
  const fills = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "execution",
      "Execution.GetFills",
      {},
    ),
    enabled: fillsAvailable,
    queryFn: async () => parsePaperFills(await transport.query({ query: "paperFills" })),
  })
  const refreshing = status.isFetching || orders.isFetching || fills.isFetching
  const failures = [status.error, orders.error, fills.error].filter(Boolean)
  const advertisedReadCount =
    Number(statusAvailable) + Number(ordersAvailable) + Number(fillsAvailable)
  const allAdvertisedReadsFailed =
    advertisedReadCount > 0 && failures.length === advertisedReadCount
  const control = useMutation({
    mutationFn: async (request: PaperControlRequest) =>
      parsePaperControlResult(await transport.paperControl(request, true), request),
    onSuccess: async (result, request) => {
      setPendingAction(null)
      setControlMessage(paperActionCompleted(request))
      setControlResult(result)
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "bot"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "execution"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "portfolio"),
        }),
      ])
    },
  })

  const refresh = () => {
    const reads: Promise<unknown>[] = []
    if (statusAvailable) reads.push(status.refetch())
    if (ordersAvailable) reads.push(orders.refetch())
    if (fillsAvailable) reads.push(fills.refetch())
    void Promise.all(reads)
  }
  const requestControl = (request: PaperControlRequest) => {
    control.reset()
    setControlMessage(null)
    setPendingAction(request)
  }

  return (
    <PageFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={refresh}
          disabled={refreshing || advertisedReadCount === 0}
        >
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh paper state
        </Button>
      }
    >
      {missingCoreCapabilities.length > 0 ? (
        <PaperSetupNotice />
      ) : null}
      {status.isLoading && orders.isLoading && fills.isLoading ? (
        <PaperGridLoading />
      ) : allAdvertisedReadsFailed ? (
        <EmptyState
          title="Paper activity could not be loaded"
          detail="Try refreshing. If the problem continues, review Logs & Diagnostics."
        />
      ) : (
        <>
          {failures.length > 0 ? (
            <Notice text="Some paper information is temporarily unavailable. The rest of the page remains usable." />
          ) : null}
          <PaperSummary
            status={status.data?.value}
            orderCount={orders.data?.value.length}
            fillCount={fills.data?.value.length}
          />
          <LifecycleEvidence status={status.data?.value} />
          <FinancialEvidence status={status.data?.value} />
          <SimulationAndReconciliationEvidence status={status.data?.value} />
          <PaperRiskEvidence status={status.data?.value} />
          <ManualPaperDraftPanel
            transport={transport}
            scope={bootstrap.runtime}
            enabled={
              status.data?.value.state === "running" &&
              status.data.value.strategyMode === "manual" &&
              manualPaperAvailable
            }
            busy={control.isPending}
            onAccepted={async () => {
              await Promise.all([
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.runtime, "bot"),
                }),
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.runtime, "execution"),
                }),
              ])
            }}
          />
          <PaperControlPanel
            status={status.data?.value}
            availability={controlAvailability}
            busy={control.isPending}
            onRequest={requestControl}
          />
          {controlMessage ? <SuccessNotice text={controlMessage} /> : null}
          {controlResult ? <CompletedAction result={controlResult} /> : null}
          <div className="mt-4 grid gap-4 2xl:grid-cols-[1.4fr_1fr]">
            <OrdersPanel
              orders={orders.data?.value ?? []}
              error={orders.error}
              available={ordersAvailable}
              cancelAvailable={controlAvailability.cancel}
              busy={control.isPending}
              onCancel={(orderToken) => requestControl({ action: "cancel", orderToken })}
            />
            <FillsPanel
              fills={fills.data?.value ?? []}
              error={fills.error}
              available={fillsAvailable}
            />
          </div>
          <EvidenceBoundary
            orders={orders.data}
            fills={fills.data}
          />
          <PaperConfirmationDialog
            request={pendingAction}
            busy={control.isPending}
            error={
              control.isError
                ? "This action could not be completed. Try again or review Logs & Diagnostics."
                : null
            }
            onClose={() => {
              control.reset()
              setPendingAction(null)
            }}
            onConfirm={() => {
              if (pendingAction) control.mutate(pendingAction)
            }}
          />
        </>
      )}
    </PageFrame>
  )
}

function PaperSetupNotice() {
  return (
    <div className="mb-4 rounded-xl border border-amber-400/25 bg-amber-400/5 p-4">
      <p className="text-sm font-semibold text-amber-100">Paper setup is incomplete</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Some paper features are unavailable. Review Connections or Updates &amp; Repair. Market data
        availability alone does not make an investment eligible for paper trading.
      </p>
    </div>
  )
}

function LifecycleEvidence({ status }: { status: PaperStatus | undefined }) {
  let content: React.ReactNode
  if (!status) {
    content = (
      <InlineEmpty detail="Paper lifecycle status is unavailable, so shutdown and recovery state cannot be confirmed." />
    )
  } else if (status.state === "stopped") {
    const shutdown =
      status.lastShutdownComplete === null
        ? "No previous shutdown recorded"
        : status.lastShutdownComplete
          ? "Complete"
          : "Incomplete"
    content = (
      <>
        <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Fact label="Lifecycle" value="Stopped" />
          <Fact label="Last shutdown" value={shutdown} />
        </dl>
        {status.lastShutdownComplete === false ? (
          <Notice
            bad
            text="The last shutdown was incomplete. Review Logs & Diagnostics before starting a new paper session."
          />
        ) : null}
      </>
    )
  } else if (status.state === "running") {
    content = (
      <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Lifecycle" value="Running" />
        <Fact label="Snapshot state" value={status.complete ? "Complete" : "Incomplete"} />
        <Fact
          label="Reconciliation"
          value={status.financialReconciliationCurrent ? "Current" : "Needs attention"}
        />
      </dl>
    )
  } else if (status.state === "failed") {
    content = (
      <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Lifecycle" value="Failed" />
        <Fact label="Market data" value="Unavailable" />
        <Fact label="Status" value="The paper session needs attention" />
        <Fact label="Next step" value="Stop the session before restarting" />
      </dl>
    )
  } else {
    content = (
      <dl className="grid gap-3 sm:grid-cols-2">
        <Fact label="Lifecycle transition" value={humanize(status.state)} />
        <Fact label="Next step" value="Wait for the current action to finish" />
      </dl>
    )
  }
  return (
    <div className="mt-4">
      <Panel
        title="Lifecycle and recovery"
        subtitle="Current session state, shutdown status, and any required recovery action."
      >
        {content}
      </Panel>
    </div>
  )
}

function CompletedAction({ result }: { result: PaperControlResult }) {
  let facts: React.ReactNode
  if (result.action === "start") {
    facts = (
      <>
        <Fact label="Action" value="Started virtual paper session" />
        <Fact label="Live market data" value="Best eligible data selected" />
        <Fact label="Paper mode" value={humanize(result.value.strategyMode)} />
        <Fact label="Real brokerage orders" value="Disabled" />
      </>
    )
  } else if (result.action === "stop" || result.action === "triggerKillSwitch") {
    facts = (
      <>
        <Fact label="Action" value={humanize(result.action)} />
        <Fact
          label="Shutdown"
          value={result.value.shutdownComplete ? "Complete" : "Incomplete"}
        />
        <Fact label="Reason" value={result.value.reason} />
      </>
    )
  } else if (result.action === "cancel") {
    facts = (
      <>
        <Fact label="Action" value="Paper cancellation" />
        <Fact label="Order" value={shortId(result.value.orderToken)} />
        <Fact label="Status" value={humanize(result.value.status)} />
        <Fact label="Observed" value={timeValue(result.value.observedAt)} />
        <Fact
          label="Cumulative virtual fill"
          value={`${result.value.cumulativeFilledLots.toLocaleString()} lots`}
        />
        <Fact label="Cumulative virtual fees" value={formatMoney(result.value.cumulativeFees)} />
      </>
    )
  } else if (result.action === "reconcile") {
    facts = (
      <>
        <Fact label="Action" value="Paper reconciliation" />
        <Fact label="Observed" value={timeValue(result.value.observedAt)} />
        <Fact
          label="Market data check"
          value={result.value.marketDataReady ? "Passed" : "Needs attention"}
        />
        <Fact
          label="Reconciliation state"
          value={result.value.reconciliationRequired ? "Still required" : "Current"}
        />
        <Fact label="Orders checked" value={result.value.ordersChecked.toLocaleString()} />
        <Fact label="Accounts checked" value={result.value.accountsChecked.toLocaleString()} />
      </>
    )
  } else {
    facts = null
  }
  return (
    <div className="mt-4">
      <Panel
        title="Latest completed action"
        subtitle="Review the latest change before continuing."
      >
        <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">{facts}</dl>
      </Panel>
    </div>
  )
}

function PaperSummary({
  status,
  orderCount,
  fillCount,
}: {
  status: PaperStatus | undefined
  orderCount: number | undefined
  fillCount: number | undefined
}) {
  const running = status?.state === "running"
  const reconciliation = running
    ? status.reconciliationRequired
      ? "Required"
      : status.financialReconciliationCurrent
        ? "Current"
        : "Not current"
    : "Not reported"

  return (
    <>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Summary
          label="Paper lifecycle"
          value={status ? humanize(status.state) : "Unavailable"}
          icon={Activity}
          tone={status?.state === "failed" ? "bad" : running ? "good" : "neutral"}
        />
        <Summary
          label="Order records"
          value={orderCount?.toLocaleString() ?? "Unavailable"}
          icon={WalletCards}
        />
        <Summary
          label="Fill events"
          value={fillCount?.toLocaleString() ?? "Unavailable"}
          icon={ReceiptText}
        />
        <Summary
          label="Reconciliation"
          value={reconciliation}
          icon={ShieldCheck}
          tone={reconciliation === "Required" || reconciliation === "Not current" ? "bad" : "neutral"}
        />
      </div>
      {running ? (
        <div className="mt-4 rounded-xl border border-border bg-card/35 p-4">
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-xs text-muted-foreground">
            <span>
              Positions <strong className="font-mono text-foreground">{status.positions}</strong>
            </span>
            <span>
              Account update <strong className="text-foreground">{status.complete ? "Complete" : "Incomplete"}</strong>
            </span>
          </div>
        </div>
      ) : null}
      {status?.state === "failed" ? (
        <Notice
          text="The paper session failed and must be stopped before it can restart."
          bad
        />
      ) : null}
    </>
  )
}

function FinancialEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.state !== "running") return null
  const accounts = status.accounts?.rows ?? []
  const cash = status.cash?.rows ?? []
  const positions = status.positionRecords?.rows ?? []
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel
        title="Virtual balances and P&L"
        subtitle="Virtual cash, equity, exposure, fees, and profit or loss; none are brokerage balances."
      >
        {accounts.length === 0 ? (
          <InlineEmpty detail="No virtual account balance is available yet." />
        ) : (
          <div className="space-y-4">
            {accounts.map((account) => <AccountEvidence key={account.accountId} account={account} />)}
          </div>
        )}
        <EvidenceCount label="Accounts shown" evidence={status.accounts} />
        <EvidenceCount label="Cash balances shown" evidence={status.cash} />
        {cash.length > 0 ? (
          <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-2">
            {cash.map((row) => <Fact key={`${row.accountId}:${row.balance.currency}`} label={`Cash · ${shortId(row.accountId)}`} value={formatMoney(row.balance)} />)}
          </dl>
        ) : null}
      </Panel>
      <Panel
        title="Virtual positions and reconciliation"
        subtitle="Settled paper lots and cost basis from the same snapshot; no brokerage position is represented."
      >
        {positions.length === 0 ? (
          <InlineEmpty detail="No settled paper positions yet." />
        ) : (
          <div className="space-y-3">
            {positions.map((position) => <PositionEvidence key={`${position.accountId}:${position.instrumentId}`} position={position} />)}
          </div>
        )}
        <EvidenceCount label="Positions shown" evidence={status.positionRecords} />
        <div className="mt-4 rounded-lg border border-border/70 bg-background/35 p-3 text-xs text-muted-foreground">
          <p>
            Account check: {status.financialReconciliationCurrent ? "current" : "needs attention"};
            reconciliation {status.reconciliationRequired ? "required" : "not required"}.
          </p>
        </div>
      </Panel>
    </div>
  )
}

function AccountEvidence({ account }: { account: PaperAccount }) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-3">
      <p className="font-mono text-[10px] text-muted-foreground">
        Virtual account {shortId(account.accountId)}
      </p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Marked equity" value={formatMoney(account.markedEquity)} />
        <Fact label="Peak marked equity" value={formatMoney(account.peakMarkedEquity)} />
        <Fact label="Settled capital" value={formatMoney(account.settledCapital)} />
        <Fact label="Realized P&L" value={formatMoney(account.realizedPnl)} />
        <Fact label="Unrealized P&L" value={formatMoney(account.unrealizedPnl)} />
        <Fact label="Realized loss" value={formatMoney(account.realizedLoss)} />
        <Fact label="Gross exposure" value={formatMoney(account.grossExposure)} />
        <Fact label="Drawdown" value={formatMoney(account.drawdown)} />
        <Fact label="Risk eligibility" value={account.eligible ? "Eligible" : "Ineligible"} />
      </dl>
    </article>
  )
}

function PaperRiskEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.state !== "running") return null
  const limits = status.riskLimits
  const decisions = status.riskDecisions
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel
        title="Central risk limits and supported instruments"
        subtitle="These limits are checked before any virtual order can proceed."
      >
        {!limits ? <InlineEmpty detail="Risk limits are unavailable for this paper session." /> : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Order notional" value={formatMoney(limits.maximumOrderNotional)} />
              <Fact label="Gross exposure" value={formatMoney(limits.maximumGrossExposure)} />
              <Fact label="Position limit" value={`${limits.maximumPositionLots.toLocaleString()} lots`} />
              <Fact label="Leverage limit" value={`${limits.maximumLeverageBasisPoints.toLocaleString()} bp`} />
              <Fact label="Minimum capital" value={formatMoney(limits.minimumCapital)} />
              <Fact label="Maximum loss" value={formatMoney(limits.maximumLoss)} />
              <Fact label="Maximum drawdown" value={formatMoney(limits.maximumDrawdown)} />
              <Fact label="Maximum fees" value={`${limits.maximumFeeBasisPoints.toLocaleString()} bp`} />
              <Fact label="Price deviation" value={`${limits.maximumPriceDeviationBasisPoints.toLocaleString()} bp`} />
              <Fact label="Maximum slippage" value={`${limits.maximumSlippageBasisPoints.toLocaleString()} bp`} />
              <Fact label="Shorting" value={limits.allowShort ? "Allowed" : "Disabled"} />
              <Fact label="Kill switch" value={limits.killSwitch ? "Engaged" : "Clear"} />
              <Fact label="Order rate" value={`${limits.maximumOrdersPerWindow} / ${durationNanos(limits.orderRateWindowNanos)}`} />
            </dl>
            <EligibleInstrumentEvidence limits={limits} />
          </>
        )}
      </Panel>
      <Panel
        title="Risk decisions and market timing"
        subtitle="Recent approvals and rejections, including when market data was observed and when each decision expires."
      >
        {!decisions ? <InlineEmpty detail="No recent risk decisions are available." /> : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Recent decisions shown" value={`${decisions.returnedItems} of ${decisions.availableItems}`} />
              <Fact label="Reconciliation" value={status.reconciliationRequired ? "Required" : "Current"} />
            </dl>
            {decisions.records.length === 0 ? (
              <InlineEmpty detail="No recent approval or rejection is available." />
            ) : (
              <div className="mt-4 max-h-80 space-y-2 overflow-y-auto pr-1">
                {decisions.records.map((decision) => (
                  <RiskDecisionEvidence
                    key={`${decision.orderToken}:${String(decision.observedAt)}`}
                    decision={decision}
                  />
                ))}
              </div>
            )}
          </>
        )}
      </Panel>
    </div>
  )
}

function EligibleInstrumentEvidence({ limits }: { limits: NonNullable<Extract<PaperStatus, { state: "running" }>["riskLimits"]> }) {
  const eligible = limits.eligibleInstruments
  return (
    <div className="mt-4 rounded-lg border border-border/70 bg-background/35 p-3">
      <h3 className="text-xs font-semibold">Assets eligible for paper trading</h3>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
        Only assets listed here can be used in this paper session. Availability in Markets alone
        does not make an investment eligible for paper trading.
      </p>
      {eligible.rows.length === 0 ? (
        <p className="mt-3 text-xs text-amber-100">No eligible instrument was returned.</p>
      ) : (
        <div className="mt-3 flex flex-wrap gap-2">
          {eligible.rows.map((instrument) => (
            <code key={instrument} className="rounded-md border border-border bg-card/50 px-2 py-1 text-[10px]">
              {instrument}
            </code>
          ))}
        </div>
      )}
      <p className="mt-3 text-[10px] text-muted-foreground">
        Showing {eligible.returnedItems} of {eligible.availableItems} eligible assets.
      </p>
    </div>
  )
}

function RiskDecisionEvidence({ decision }: { decision: PaperAuditDecision }) {
  return (
    <article className="rounded-lg border border-border/70 bg-background/35 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs font-medium">{humanize(decision.outcome)}</p>
        <EvidenceBadge
          label={decision.reasons.length > 0 ? `${decision.reasons.length} reason(s)` : "No rejection reason"}
          tone={decision.reasons.length > 0 ? "bad" : "neutral"}
        />
      </div>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Instrument" value={decision.instrumentId} />
        <Fact label="Market observed" value={timeValue(decision.marketObservedAt)} />
        <Fact label="Decision valid until" value={timeValue(decision.validUntil)} />
        <Fact label="Decision recorded" value={timeValue(decision.observedAt)} />
      </dl>
    </article>
  )
}

function SimulationAndReconciliationEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.state !== "running") return null
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-3">
      <Panel
        title="Market data readiness"
        subtitle="The paper session pauses if current market data becomes unavailable."
      >
        <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
          <Fact label="Market data health" value="Active" />
          <Fact
            label="Actual observation freshness"
            value={
              status.riskDecisions?.records.length
                ? "Observation times returned in risk decisions"
                : "Not returned"
            }
          />
          <Fact
            label="Maximum market-data age"
            value={
              status.simulation
                ? durationNanos(status.simulation.maximumMarkAgeNanos)
                : "Not returned"
            }
          />
        </dl>
        <p className="mt-3 text-[10px] leading-4 text-muted-foreground">
          A maximum allowed age is a safety limit, not proof that every observation is fresh.
        </p>
      </Panel>
      <Panel title="Paper fill assumptions" subtitle="These fixed assumptions shape virtual fills; they are not predictions about real execution.">
        {!status.simulation ? <InlineEmpty detail="Paper fill assumptions are unavailable." /> : (
          <dl className="grid gap-3 sm:grid-cols-2">
            <Fact label="Entry latency" value={`${durationNanos(status.simulation.minimumLatencyNanos)} to ${durationNanos(status.simulation.maximumLatencyNanos)}`} />
            <Fact label="Cancel latency" value={durationNanos(status.simulation.cancelLatencyNanos)} />
            <Fact label="Maximum mark age" value={durationNanos(status.simulation.maximumMarkAgeNanos)} />
            <Fact label="Book participation" value={`${status.simulation.maximumParticipationBasisPoints.toLocaleString()} bp`} />
            <Fact label="Impact per level" value={`${status.simulation.impactBasisPointsPerLevel.toLocaleString()} bp`} />
            <Fact label="Maker / taker fee" value={`${status.simulation.makerFeeBasisPoints} / ${status.simulation.takerFeeBasisPoints} bp`} />
            <Fact label="Fee floor / cap" value={`${formatMoney(status.simulation.minimumFee)} / ${status.simulation.maximumFee ? formatMoney(status.simulation.maximumFee) : "No cap"}`} />
          </dl>
        )}
      </Panel>
      <Panel title="Account reconciliation" subtitle="Checks that virtual balances, positions, orders, and fills agree; it never represents a brokerage balance.">
        {!status.reconciliation ? <InlineEmpty detail="No reconciliation details are available." /> : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Account update" value={status.reconciliation.snapshotComplete ? "Complete" : "Incomplete"} />
              <Fact label="Account check" value={status.reconciliation.financialReconciliationCurrent ? "Current" : "Needs attention"} />
              <Fact label="Virtual orders" value={`${status.reconciliation.activeOrderCount} active · ${status.reconciliation.archivedOrderCount} archived`} />
              <Fact label="Balances and positions" value={`${status.reconciliation.accountCount} accounts · ${status.reconciliation.cashBalanceCount} cash balances · ${status.reconciliation.positionCount} positions`} />
              <Fact label="Fills reviewed" value={status.reconciliation.fillCount.toLocaleString()} />
              <Fact label="Action required" value={status.reconciliation.reconciliationRequired ? "Yes" : "No"} />
            </dl>
          </>
        )}
      </Panel>
    </div>
  )
}

function PositionEvidence({ position }: { position: PaperPosition }) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-3">
      <p className="font-mono text-xs">{position.instrumentId}</p>
      <p className="mt-1 text-xs text-muted-foreground">{position.lots.toLocaleString()} settled lots · cost basis {formatMoney(position.costBasis)}</p>
    </article>
  )
}

function EvidenceCount({ label, evidence }: { label: string; evidence: { returnedItems: number; availableItems: number } | undefined }) {
  return <p className="mt-3 text-[10px] text-muted-foreground">{label}: {evidence ? `${evidence.returnedItems} of ${evidence.availableItems}` : "unavailable"}.</p>
}

function OrdersPanel({
  orders,
  error,
  available,
  cancelAvailable,
  busy,
  onCancel,
}: {
  orders: PaperOrder[]
  error: unknown
  available: boolean
  cancelAvailable: boolean
  busy: boolean
  onCancel: (orderId: string) => void
}) {
  return (
    <Panel title="Virtual orders" subtitle="Tracked virtual orders and fill progress; these are never brokerage orders.">
      {!available ? (
        <InlineEmpty detail="Virtual order history is not available in the current setup." />
      ) : error ? (
        <InlineEmpty detail="Virtual order history could not be loaded. Try refreshing." />
      ) : orders.length === 0 ? (
        <InlineEmpty detail="No virtual orders yet." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[860px] text-left text-xs">
            <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="pb-3 pr-4 font-medium">Order</th>
                <th className="pb-3 pr-4 font-medium">State</th>
                <th className="pb-3 pr-4 font-medium">Fill progress</th>
                <th className="pb-3 pr-4 font-medium">Investment target</th>
                <th className="pb-3 pr-4 font-medium">Price and risk limits</th>
                <th className="pb-3 pr-4 font-medium">Fees</th>
                <th className="pb-3 font-medium">Timing</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/70">
              {orders.map((order) => (
                <OrderRow
                  key={order.orderToken}
                  order={order}
                  cancelAvailable={cancelAvailable}
                  busy={busy}
                  onCancel={onCancel}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function OrderRow({
  order,
  cancelAvailable,
  busy,
  onCancel,
}: {
  order: PaperOrder
  cancelAvailable: boolean
  busy: boolean
  onCancel: (orderId: string) => void
}) {
  const filled = order.filledLots
  const partial =
    filled !== undefined && filled > 0 && filled < order.requestedLots
  return (
    <tr>
      <td className="py-3 pr-4 font-mono text-[11px]">
        {shortId(order.orderToken)}
      </td>
      <td className="py-3 pr-4">
        <EvidenceBadge
          label={humanize(order.status)}
          tone={order.status === "rejected" ? "bad" : "neutral"}
        />
        {partial ? (
          <span className="ml-2 inline-flex rounded-md border border-amber-400/20 bg-amber-400/10 px-2 py-1 text-[10px] font-medium text-amber-200">
            Partial fill
          </span>
        ) : null}
      </td>
      <td className="py-3 pr-4 font-mono">
        {filled === undefined ? "Not reported" : `${filled.toLocaleString()} / `}
        {order.requestedLots.toLocaleString()} lots
        {order.side ? (
          <span className="block text-[10px] text-muted-foreground">{humanize(order.side)}</span>
        ) : null}
      </td>
      <td className="py-3 pr-4 text-muted-foreground">
        {order.targetToken ? (
          <span className="block text-[11px] text-foreground">Linked investment target</span>
        ) : (
          "No investment target linked"
        )}
      </td>
      <td className="py-3 pr-4 text-muted-foreground">
        {order.averageFillPriceTicks === undefined || order.averageFillPriceTicks === null
          ? "Not reported"
          : `${order.averageFillPriceTicks.toLocaleString()} avg ticks`}
        {order.maximumFillPriceTicks !== undefined && order.maximumFillPriceTicks !== null ? (
          <span className="block">{order.maximumFillPriceTicks.toLocaleString()} max fill ticks</span>
        ) : null}
        {order.maximumExecutionPriceTicks !== undefined &&
        order.maximumExecutionPriceTicks !== null ? (
          <span className="block">
            {order.maximumExecutionPriceTicks.toLocaleString()} execution limit ticks
          </span>
        ) : null}
        {order.referencePriceTicks !== undefined ? (
          <span className="block">Reference {order.referencePriceTicks.toLocaleString()} ticks</span>
        ) : null}
        {order.maximumSlippageBasisPoints !== undefined ? (
          <span className="block">Maximum slippage {order.maximumSlippageBasisPoints.toLocaleString()} bp</span>
        ) : null}
        {order.observed?.averageFillSlippageTicks !== null && order.observed?.averageFillSlippageTicks !== undefined ? (
          <span className="block">Observed average slippage {order.observed.averageFillSlippageTicks.toLocaleString()} ticks ({order.observed.averageFillSlippageBasisPoints?.toLocaleString() ?? "not calculable"} bp)</span>
        ) : null}
      </td>
      <td className="py-3 pr-4 font-mono">
        {order.cumulativeFees ? formatMoney(order.cumulativeFees) : "Not reported"}
      </td>
      <td className="py-3 text-muted-foreground">
        {order.acceptedAt === undefined ? "Not reported" : `Accepted ${timeValue(order.acceptedAt)}`}
        {order.eligibleAt !== undefined ? (
          <span className="block">
            Eligible {timeValue(order.eligibleAt)}{order.acceptedAt !== undefined ? ` · ${latencyValue(order.acceptedAt, order.eligibleAt)}` : ""}
          </span>
        ) : null}
        {order.observed?.firstFillAfterEligibilityNanos !== null && order.observed?.firstFillAfterEligibilityNanos !== undefined ? (
          <span className="block">First fill {durationNanos(order.observed.firstFillAfterEligibilityNanos)} after eligibility</span>
        ) : null}
        {order.expiresAt !== undefined ? (
          <span className="block">Expires {timeValue(order.expiresAt)}</span>
        ) : null}
        {isCancelable(order.status) && cancelAvailable ? (
          <Button
            type="button"
            variant="ghost"
            size="xs"
            className="mt-2 text-rose-200 hover:text-rose-100"
            disabled={busy}
            onClick={() => onCancel(order.orderToken)}
          >
            Cancel order
          </Button>
        ) : isCancelable(order.status) ? (
          <span className="mt-2 block text-[10px] text-muted-foreground">
            Cancellation is unavailable in the current setup.
          </span>
        ) : null}
      </td>
    </tr>
  )
}

function FillsPanel({
  fills,
  error,
  available,
}: {
  fills: PaperFill[]
  error: unknown
  available: boolean
}) {
  return (
    <Panel title="Virtual fills" subtitle="Virtual fill prices, liquidity, fees, and costs; no brokerage fill is represented.">
      {!available ? (
        <InlineEmpty detail="Virtual fill history is not available in the current setup." />
      ) : error ? (
        <InlineEmpty detail="Virtual fill history could not be loaded. Try refreshing." />
      ) : fills.length === 0 ? (
        <InlineEmpty detail="No virtual fills yet." />
      ) : (
        <div className="space-y-3">
          {fills.map((fill) => (
            <article
              key={`${fill.orderToken}:${String(fill.eventAt)}:${fill.quantityLots}`}
              className="rounded-lg border border-border bg-background/45 p-4"
            >
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="font-mono text-[10px] text-muted-foreground">
                    Order {shortId(fill.orderToken)}
                  </p>
                  <p className="mt-2 font-mono text-base font-semibold">
                    {fill.quantityLots.toLocaleString()} lots
                  </p>
                </div>
                {fill.liquidity ? <EvidenceBadge label={humanize(fill.liquidity)} tone="neutral" /> : null}
              </div>
              <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-2">
                <Fact
                  label="Average price"
                  value={
                    fill.averagePriceTicks === undefined
                      ? "Not reported"
                      : `${fill.averagePriceTicks.toLocaleString()} ticks`
                  }
                />
                <Fact
                  label="Maximum price"
                  value={
                    fill.maximumPriceTicks === undefined
                      ? "Not reported"
                      : `${fill.maximumPriceTicks.toLocaleString()} ticks`
                  }
                />
                {fill.notional ? <Fact label="Notional" value={formatMoney(fill.notional)} /> : null}
                {fill.fee ? <Fact label="Fee" value={formatMoney(fill.fee)} /> : null}
                {fill.eventAt !== undefined ? (
                  <Fact label="Observed" value={timeValue(fill.eventAt)} />
                ) : null}
              </dl>
            </article>
          ))}
        </div>
      )}
    </Panel>
  )
}

function EvidenceBoundary({
  orders,
  fills,
}: {
  orders: { completeness: string; returnedItems: number; availableItems: number } | undefined
  fills: { completeness: string; returnedItems: number; availableItems: number } | undefined
}) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Order history: {countBoundary(orders)}. Fill history: {countBoundary(fills)}. Timing uses only
      recorded accepted and eligible times. Price limits are safety limits for the virtual order,
      not a prediction of future fills.
    </p>
  )
}

function Summary({
  label,
  value,
  icon: Icon,
  tone = "neutral",
}: {
  label: string
  value: string
  icon: typeof Activity
  tone?: "neutral" | "good" | "bad"
}) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon
        className={tone === "good" ? "size-4 text-emerald-400" : tone === "bad" ? "size-4 text-rose-400" : "size-4 text-primary"}
        aria-hidden="true"
      />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold">{value}</p>
    </div>
  )
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <h2 className="text-base font-semibold">{title}</h2>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{subtitle}</p>
      <div className="mt-5">{children}</div>
    </section>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1280px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Simulated execution
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Paper Execution</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Live-market-backed virtual cash, positions, orders, fills, fees, slippage, P&amp;L,
            safety limits, and session controls.
          </p>
        </div>
        {action}
      </div>
      <section
        className="mt-6 flex gap-3 rounded-xl border-2 border-primary/45 bg-primary/[0.08] p-4 shadow-[0_0_24px_rgba(39,95,255,0.08)]"
        aria-labelledby="paper-simulation-boundary"
      >
        <WalletCards className="mt-0.5 size-5 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <h2 id="paper-simulation-boundary" className="text-base font-semibold">
            Virtual paper execution — no real money or brokerage orders
          </h2>
          <p className="mt-1 max-w-4xl text-xs leading-5 text-muted-foreground">
            While a paper session is active, prices and market events come from the best eligible
            live market data. Balances, positions, orders, fills, P&amp;L, and risk
            outcomes are virtual. No brokerage account is connected or instructed by this page,
            and an investment recommendation never becomes an order automatically. A user-confirmed
            manual draft or explicit paper-only strategy still requires current supported-instrument
            eligibility and safety checks before it can proceed.
          </p>
        </div>
      </section>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <DatabaseZap className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function InlineEmpty({ detail }: { detail: string }) {
  return (
    <div className="rounded-lg border border-dashed border-border p-5 text-sm text-muted-foreground">
      {detail}
    </div>
  )
}

function Notice({ text, bad = false }: { text: string; bad?: boolean }) {
  return (
    <div className={`mt-4 flex gap-2 rounded-lg border p-3 text-xs ${bad ? "border-rose-400/20 bg-rose-400/5 text-rose-100" : "border-amber-400/20 bg-amber-400/5 text-amber-100"}`}>
      <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
      {text}
    </div>
  )
}

function SuccessNotice({ text }: { text: string }) {
  return (
    <div className="mt-4 flex gap-2 rounded-lg border border-emerald-400/20 bg-emerald-400/5 p-3 text-xs text-emerald-100">
      <ShieldCheck className="size-4 shrink-0" aria-hidden="true" />
      {text}
    </div>
  )
}

function EvidenceBadge({
  label,
  tone,
}: {
  label: string
  tone: "neutral" | "warn" | "bad"
}) {
  const style =
    tone === "bad"
      ? "border-rose-400/20 bg-rose-400/10 text-rose-200"
      : tone === "warn"
        ? "border-amber-400/20 bg-amber-400/10 text-amber-200"
        : "border-border bg-background/60 text-muted-foreground"
  return <span className={`inline-flex rounded-md border px-2 py-1 text-[10px] font-medium ${style}`}>{label}</span>
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-xs">{value}</dd>
    </div>
  )
}

function PaperLoading() {
  return (
    <PageFrame>
      <PaperGridLoading />
    </PageFrame>
  )
}

function PaperGridLoading() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {Array.from({ length: 4 }, (_, index) => (
          <Skeleton key={index} className="h-28 rounded-xl" />
        ))}
      </div>
      <div className="grid gap-4 2xl:grid-cols-[1.4fr_1fr]">
        <Skeleton className="h-96 rounded-xl" />
        <Skeleton className="h-96 rounded-xl" />
      </div>
    </div>
  )
}

function countBoundary(
  value: { completeness: string; returnedItems: number; availableItems: number } | undefined,
) {
  return value
    ? `${value.returnedItems} of ${value.availableItems} shown`
    : "unavailable"
}

function shortId(value: string) {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}

function timeValue(value: string | number) {
  if (typeof value === "string") return value
  const milliseconds = Math.trunc(value / 1_000_000)
  const date = new Date(milliseconds)
  return Number.isNaN(date.getTime()) ? value.toLocaleString() : date.toLocaleString()
}

function latencyValue(accepted: string | number, eligible: string | number) {
  if (typeof accepted !== "number" || typeof eligible !== "number") return "latency not numeric"
  const nanos = eligible - accepted
  if (nanos < 0) return "timing unavailable"
  return nanos >= 1_000_000 ? `${(nanos / 1_000_000).toLocaleString()} ms simulated latency` : `${nanos.toLocaleString()} ns simulated latency`
}

function durationNanos(nanos: number) {
  if (nanos >= 1_000_000_000) return `${(nanos / 1_000_000_000).toLocaleString()} s`
  if (nanos >= 1_000_000) return `${(nanos / 1_000_000).toLocaleString()} ms`
  return `${nanos.toLocaleString()} ns`
}

function isCancelable(state: string) {
  return state === "new" || state === "accepted" || state === "partially_filled"
}
