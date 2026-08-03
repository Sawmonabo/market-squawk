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

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { PaperControlRequest, ProductTransport } from "@/lib/transport"

import {
  type PaperFill,
  type PaperAccount,
  type PaperOrder,
  type PaperPosition,
  type PaperStatus,
  parsePaperFills,
  parsePaperOrders,
  parsePaperStatus,
} from "./contracts"
import {
  PaperConfirmationDialog,
  PaperControlPanel,
  paperActionCompleted,
} from "./paper-controls"
import { ManualPaperDraftPanel } from "./manual-paper-draft"

export function PaperExecutionPage() {
  const product = useProduct()

  if (product.status === "loading") return <PaperLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState title="Paper execution is unavailable" detail={product.error} />
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
  const operationNames = new Set(bootstrap.operations.map((operation) => operation.name))
  const manualPaperAvailable =
    operationNames.has("Execution.GetManualPaperTargets") &&
    operationNames.has("Execution.SubmitManualPaperDraft")
  const status = useQuery({
    queryKey: productKeys.operation(bootstrap.runtime, "Bot", "Bot.GetStatus", {}),
    queryFn: async () => parsePaperStatus(await transport.query({ query: "paperStatus" })),
  })
  const orders = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Execution",
      "Execution.GetOrders",
      {},
    ),
    queryFn: async () => parsePaperOrders(await transport.query({ query: "paperOrders" })),
  })
  const fills = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Execution",
      "Execution.GetFills",
      {},
    ),
    queryFn: async () => parsePaperFills(await transport.query({ query: "paperFills" })),
  })
  const refreshing = status.isFetching || orders.isFetching || fills.isFetching
  const failures = [status.error, orders.error, fills.error].filter(Boolean)
  const control = useMutation({
    mutationFn: (request: PaperControlRequest) => transport.paperControl(request, true),
    onSuccess: async (_result, request) => {
      setPendingAction(null)
      setControlMessage(paperActionCompleted(request))
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "Bot"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "Execution"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "Portfolio"),
        }),
      ])
    },
  })

  const refresh = () => {
    void Promise.all([status.refetch(), orders.refetch(), fills.refetch()])
  }
  const requestControl = (request: PaperControlRequest) => {
    control.reset()
    setControlMessage(null)
    setPendingAction(request)
  }

  return (
    <PageFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh paper state
        </Button>
      }
    >
      {status.isLoading && orders.isLoading && fills.isLoading ? (
        <PaperGridLoading />
      ) : failures.length === 3 ? (
        <EmptyState
          title="No paper evidence is available"
          detail={messageFrom(failures[0])}
        />
      ) : (
        <>
          {failures.length > 0 ? (
            <Notice text="One paper view could not be read. The page shows only evidence returned by the remaining views." />
          ) : null}
          <PaperSummary
            status={status.data?.value}
            orderCount={orders.data?.value.length}
            fillCount={fills.data?.value.length}
          />
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
                  queryKey: productKeys.domain(bootstrap.runtime, "Bot"),
                }),
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.runtime, "Execution"),
                }),
              ])
            }}
          />
          <PaperControlPanel
            status={status.data?.value}
            sessions={bootstrap.providerSessions}
            busy={control.isPending}
            onRequest={requestControl}
          />
          {controlMessage ? <SuccessNotice text={controlMessage} /> : null}
          <div className="mt-4 grid gap-4 2xl:grid-cols-[1.4fr_1fr]">
            <OrdersPanel
              orders={orders.data?.value ?? []}
              error={orders.error}
              busy={control.isPending}
              onCancel={(orderId) => requestControl({ action: "cancel", orderId })}
            />
            <FillsPanel fills={fills.data?.value ?? []} error={fills.error} />
          </div>
          <EvidenceBoundary
            status={status.data}
            orders={orders.data}
            fills={fills.data}
          />
          <PaperConfirmationDialog
            request={pendingAction}
            busy={control.isPending}
            error={control.isError ? messageFrom(control.error) : null}
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
              Runtime sequence <strong className="font-mono text-foreground">{status.sequence}</strong>
            </span>
            <span>
              Positions <strong className="font-mono text-foreground">{status.positions}</strong>
            </span>
            <span>
              Snapshot <strong className="text-foreground">{status.complete ? "Complete" : "Incomplete"}</strong>
            </span>
          </div>
        </div>
      ) : null}
      {status?.state === "failed" ? (
        <Notice text={`The ${status.provider} paper runtime failed and requires an explicit stop before restart.`} bad />
      ) : null}
    </>
  )
}

function FinancialEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.state !== "running") return null
  const accounts = status.accounts?.rows ?? []
  const cash = status.cash?.rows ?? []
  const positions = status.positionEvidence?.rows ?? []
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel title="Balances and P&L" subtitle="Current paper-ledger balances and marked accounting values.">
        {accounts.length === 0 ? (
          <InlineEmpty detail="The active snapshot returned no account evidence." />
        ) : (
          <div className="space-y-4">
            {accounts.map((account) => <AccountEvidence key={account.accountId} account={account} />)}
          </div>
        )}
        <EvidenceCount label="Account rows" evidence={status.accounts} />
        <EvidenceCount label="Cash rows" evidence={status.cash} />
        {cash.length > 0 ? (
          <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-2">
            {cash.map((row) => <Fact key={`${row.accountId}:${row.balance.currency}`} label={`Cash · ${shortId(row.accountId)}`} value={formatMoney(row.balance)} />)}
          </dl>
        ) : null}
      </Panel>
      <Panel title="Positions and reconciliation" subtitle="Settled lots and cost basis from the same complete paper snapshot.">
        {positions.length === 0 ? (
          <InlineEmpty detail="No settled paper positions were returned." />
        ) : (
          <div className="space-y-3">
            {positions.map((position) => <PositionEvidence key={`${position.accountId}:${position.instrumentId}`} position={position} />)}
          </div>
        )}
        <EvidenceCount label="Position rows" evidence={status.positionEvidence} />
        <div className="mt-4 rounded-lg border border-border/70 bg-background/35 p-3 text-xs text-muted-foreground">
          <p>Snapshot configuration {status.configurationDigestSha256 ? shortId(status.configurationDigestSha256) : "not returned"} · sequence {status.sequence}.</p>
          <p className="mt-1">Financial fence: {status.financialReconciliationCurrent ? "current" : "not current"}; reconciliation {status.reconciliationRequired ? "required" : "not required"}.</p>
        </div>
      </Panel>
    </div>
  )
}

function AccountEvidence({ account }: { account: PaperAccount }) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-3">
      <p className="font-mono text-[10px] text-muted-foreground">{shortId(account.accountId)} · revision {account.revision}</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Marked equity" value={formatMoney(account.markedEquity)} />
        <Fact label="Settled capital" value={formatMoney(account.settledCapital)} />
        <Fact label="Realized P&L" value={formatMoney(account.realizedPnl)} />
        <Fact label="Unrealized P&L" value={formatMoney(account.unrealizedPnl)} />
        <Fact label="Gross exposure" value={formatMoney(account.grossExposure)} />
        <Fact label="Drawdown" value={formatMoney(account.drawdown)} />
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
      <Panel title="Central risk limits" subtitle="Immutable limits enforced before paper dispatch.">
        {!limits ? <InlineEmpty detail="The active runtime did not return a risk-limit image." /> : (
          <dl className="grid gap-3 sm:grid-cols-2">
            <Fact label="Order notional" value={formatMoney(limits.maximumOrderNotional)} />
            <Fact label="Gross exposure" value={formatMoney(limits.maximumGrossExposure)} />
            <Fact label="Maximum slippage" value={`${limits.maximumSlippageBasisPoints.toLocaleString()} bp`} />
            <Fact label="Order rate" value={`${limits.maximumOrdersPerWindow} / ${limits.orderRateWindowNanos.toLocaleString()} ns`} />
          </dl>
        )}
      </Panel>
      <Panel title="Risk decisions and breaches" subtitle="Bounded decisions already committed by the durable audit owner.">
        {!decisions ? <InlineEmpty detail="No retained durable decision image was returned." /> : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Retained decisions" value={`${decisions.returnedItems} of ${decisions.availableItems}`} />
              <Fact label="Published decisions" value={decisions.totalPublished.toLocaleString()} />
              <Fact label="Latest sequence" value={decisions.latestSequence?.toLocaleString() ?? "Not reported"} />
              <Fact label="Reconciliation" value={status.reconciliationRequired ? "Required" : "Current"} />
            </dl>
            {decisions.records.some((decision) => decision.reasons.length > 0) ? <Notice bad text="One or more retained central-risk or dispatch decisions contain typed rejection reasons. Open Risk for the bounded decision evidence." /> : null}
          </>
        )}
      </Panel>
    </div>
  )
}

function SimulationAndReconciliationEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.state !== "running") return null
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel title="Configured paper simulation" subtitle="Fixed terms from the active paper runtime; these are not estimates inferred by the dashboard.">
        {!status.simulation ? <InlineEmpty detail="Configured simulation evidence was not returned." /> : (
          <dl className="grid gap-3 sm:grid-cols-2">
            <Fact label="Entry latency" value={`${durationNanos(status.simulation.minimumLatencyNanos)} to ${durationNanos(status.simulation.maximumLatencyNanos)}`} />
            <Fact label="Cancel latency" value={durationNanos(status.simulation.cancelLatencyNanos)} />
            <Fact label="Book participation" value={`${status.simulation.maximumParticipationBasisPoints.toLocaleString()} bp`} />
            <Fact label="Impact per level" value={`${status.simulation.impactBasisPointsPerLevel.toLocaleString()} bp`} />
            <Fact label="Maker / taker fee" value={`${status.simulation.makerFeeBasisPoints} / ${status.simulation.takerFeeBasisPoints} bp`} />
            <Fact label="Fee floor / cap" value={`${formatMoney(status.simulation.minimumFee)} / ${status.simulation.maximumFee ? formatMoney(status.simulation.maximumFee) : "No cap"}`} />
          </dl>
        )}
      </Panel>
      <Panel title="Reconciliation report" subtitle="A point-in-time report from the same complete paper snapshot; no external balance is fabricated.">
        {!status.reconciliation ? <InlineEmpty detail="The runtime did not return a reconciliation report." /> : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <Fact label="Snapshot" value={`${status.reconciliation.snapshotComplete ? "Complete" : "Incomplete"} · #${status.reconciliation.snapshotSequence}`} />
              <Fact label="Financial fence" value={status.reconciliation.financialReconciliationCurrent ? "Current" : "Not current"} />
              <Fact label="Order scope" value={`${status.reconciliation.activeOrderCount} active · ${status.reconciliation.archivedOrderCount} archived`} />
              <Fact label="Financial scope" value={`${status.reconciliation.accountCount} accounts · ${status.reconciliation.cashBalanceCount} cash balances · ${status.reconciliation.positionCount} positions`} />
              <Fact label="Fill evidence" value={status.reconciliation.fillCount.toLocaleString()} />
              <Fact label="Action required" value={status.reconciliation.reconciliationRequired ? "Yes" : "No"} />
            </dl>
            <p className="mt-3 font-mono text-[10px] text-muted-foreground">Configuration {shortId(status.reconciliation.configurationDigestSha256)}</p>
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
  return <p className="mt-3 text-[10px] text-muted-foreground">{label}: {evidence ? `${evidence.returnedItems} of ${evidence.availableItems} returned` : "not returned"}.</p>
}

function OrdersPanel({
  orders,
  error,
  busy,
  onCancel,
}: {
  orders: PaperOrder[]
  error: unknown
  busy: boolean
  onCancel: (orderId: string) => void
}) {
  return (
    <Panel title="Orders" subtitle="Requested and filled lots from the paper adapter.">
      {error ? (
        <InlineEmpty detail={messageFrom(error)} />
      ) : orders.length === 0 ? (
        <InlineEmpty detail="No paper orders were returned for the active workspace." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[860px] text-left text-xs">
            <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="pb-3 pr-4 font-medium">Order</th>
                <th className="pb-3 pr-4 font-medium">State</th>
                <th className="pb-3 pr-4 font-medium">Fill progress</th>
                <th className="pb-3 pr-4 font-medium">Target provenance</th>
                <th className="pb-3 pr-4 font-medium">Price evidence</th>
                <th className="pb-3 pr-4 font-medium">Fees</th>
                <th className="pb-3 font-medium">Timing</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/70">
              {orders.map((order) => (
                <OrderRow
                  key={order.orderId}
                  order={order}
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
  busy,
  onCancel,
}: {
  order: PaperOrder
  busy: boolean
  onCancel: (orderId: string) => void
}) {
  const filled = order.filledLots
  const partial =
    filled !== undefined && filled > 0 && filled < order.requestedLots
  return (
    <tr>
      <td className="py-3 pr-4 font-mono text-[11px]">{shortId(order.orderId)}</td>
      <td className="py-3 pr-4">
        <EvidenceBadge
          label={humanize(order.state)}
          tone={order.state === "rejected" ? "bad" : "neutral"}
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
      </td>
      <td className="py-3 pr-4 text-muted-foreground">
        {order.targetReference ? (
          <>
            <span className="block font-mono text-[11px] text-foreground">
              {shortId(order.targetReference.targetId)} · revision {order.targetReference.revision}
            </span>
            <span className="block font-mono text-[10px]">
              {shortId(order.targetReference.contentSha256)}
            </span>
          </>
        ) : (
          "No governed target recorded"
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
          <span className="block">Risk slippage bound {order.maximumSlippageBasisPoints.toLocaleString()} bp</span>
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
        {isCancelable(order.state) ? (
          <Button
            type="button"
            variant="ghost"
            size="xs"
            className="mt-2 text-rose-200 hover:text-rose-100"
            disabled={busy}
            onClick={() => onCancel(order.orderId)}
          >
            Cancel order
          </Button>
        ) : null}
      </td>
    </tr>
  )
}

function FillsPanel({ fills, error }: { fills: PaperFill[]; error: unknown }) {
  return (
    <Panel title="Fills" subtitle="Fill events, prices, liquidity, and exact returned costs.">
      {error ? (
        <InlineEmpty detail={messageFrom(error)} />
      ) : fills.length === 0 ? (
        <InlineEmpty detail="No paper fills were returned for the active workspace." />
      ) : (
        <div className="space-y-3">
          {fills.map((fill) => (
            <article
              key={`${fill.orderId}:${fill.sequence}`}
              className="rounded-lg border border-border bg-background/45 p-4"
            >
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="font-mono text-[10px] text-muted-foreground">
                    #{fill.sequence} · {shortId(fill.orderId)}
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
  status,
  orders,
  fills,
}: {
  status: { completeness: string } | undefined
  orders: { completeness: string; returnedItems: number; availableItems: number } | undefined
  fills: { completeness: string; returnedItems: number; availableItems: number } | undefined
}) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Bot status: {status?.completeness ?? "unavailable"}. Orders: {countBoundary(orders)}. Fills:{" "}
      {countBoundary(fills)}. Timing is calculated only from the returned accepted and eligible timestamps.
      Price bounds are the central-risk-approved bounds returned with the order, not a claim about future fills.
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
            Paper lifecycle, orders, fills, costs, and reconciliation evidence returned by the active workspace.
          </p>
        </div>
        {action}
      </div>
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
    ? `${value.completeness}, ${value.returnedItems} of ${value.availableItems} returned`
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
  if (nanos < 0) return "invalid timing evidence"
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
