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
import { formatMoney } from "@/lib/formatters"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"
import type { PaperControlRequest, ProductTransport } from "@/lib/transport"

import {
  type PaperAccount,
  type PaperAuditDecision,
  type PaperControlIntent,
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
import { ManualPaperDraftPanel } from "./manual-paper-draft"
import {
  type PaperControlAvailability,
  PaperConfirmationDialog,
  PaperControlPanel,
  paperActionCompleted,
} from "./paper-controls"

const CORE_PAPER_CAPABILITIES = [
  "bot_status",
  "bot_start_preparation",
  "bot_prepare_start",
  "bot_start",
  "bot_stop",
  "execution_orders",
  "execution_fills",
  "execution_cancel",
  "risk_kill_switch",
] as const satisfies readonly ProductCapability[]

export function PaperExecutionPage() {
  const product = useProduct()

  if (product.status === "loading") return <PaperLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState
          title="Paper practice is unavailable"
          detail="Try again. If the problem continues, review Logs & Diagnostics."
        />
      </PageFrame>
    )
  }

  return <ReadyPaperExecution bootstrap={product.bootstrap} transport={product.transport} />
}

function ReadyPaperExecution({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const [pendingAction, setPendingAction] = React.useState<PaperControlIntent | null>(null)
  const [controlMessage, setControlMessage] = React.useState<string | null>(null)
  const [controlResult, setControlResult] = React.useState<PaperControlResult | null>(null)
  const capabilities = productCapabilitySet(bootstrap)
  const missingCoreCapabilities = CORE_PAPER_CAPABILITIES.filter(
    (capability) => !capabilities.has(capability),
  )
  const controlAvailability: PaperControlAvailability = {
    stop: capabilities.has("bot_stop"),
    cancel: capabilities.has("execution_cancel"),
    killSwitch: capabilities.has("risk_kill_switch"),
  }
  const statusAvailable = capabilities.has("bot_status")
  const ordersAvailable = capabilities.has("execution_orders")
  const fillsAvailable = capabilities.has("execution_fills")
  const manualPaperAvailable =
    capabilities.has("execution_manual_targets") &&
    capabilities.has("execution_manual_prepare") &&
    capabilities.has("execution_manual_submit")
  const startAvailable =
    capabilities.has("bot_start_preparation") &&
    capabilities.has("bot_prepare_start") &&
    capabilities.has("bot_start")

  const status = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "bot", "Bot.GetStatus", {}),
    enabled: statusAvailable,
    queryFn: async () => parsePaperStatus(await transport.query({ query: "paperStatus" })),
  })
  const orders = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "execution",
      "Execution.GetOrders",
      {},
    ),
    enabled: ordersAvailable,
    queryFn: async () => parsePaperOrders(await transport.query({ query: "paperOrders" })),
  })
  const fills = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "execution",
      "Execution.GetFills",
      {},
    ),
    enabled: fillsAvailable,
    queryFn: async () => parsePaperFills(await transport.query({ query: "paperFills" })),
  })
  const control = useMutation({
    mutationFn: async (request: PaperControlIntent) =>
      parsePaperControlResult(
        await transport.paperControl(transportRequest(request), true),
        request,
      ),
    onSuccess: async (result, request) => {
      setPendingAction(null)
      setControlMessage(paperActionCompleted(request))
      setControlResult(result)
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.productSessionToken, "bot"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.productSessionToken, "execution"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.productSessionToken, "portfolio"),
        }),
      ])
    },
  })

  const refreshing = status.isFetching || orders.isFetching || fills.isFetching
  const failures = [status.error, orders.error, fills.error].filter(Boolean)
  const advertisedReadCount =
    Number(statusAvailable) + Number(ordersAvailable) + Number(fillsAvailable)
  const allAdvertisedReadsFailed =
    advertisedReadCount > 0 && failures.length === advertisedReadCount

  const refresh = () => {
    const reads: Promise<unknown>[] = []
    if (statusAvailable) reads.push(status.refetch())
    if (ordersAvailable) reads.push(orders.refetch())
    if (fillsAvailable) reads.push(fills.refetch())
    void Promise.all(reads)
  }
  const requestControl = (request: PaperControlIntent) => {
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
          Refresh
        </Button>
      }
    >
      {missingCoreCapabilities.length > 0 ? <PaperSetupNotice /> : null}
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
          <SessionSummary status={status.data?.value} />
          <AccountEvidence status={status.data?.value} />
          <SafetyEvidence status={status.data?.value} />
          <ManualPaperDraftPanel
            transport={transport}
            scope={bootstrap.productSessionToken}
            enabled={
              status.data?.value.sessionAvailability === "active" && manualPaperAvailable
            }
            busy={control.isPending}
            onAccepted={async () => {
              await Promise.all([
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.productSessionToken, "bot"),
                }),
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.productSessionToken, "execution"),
                }),
              ])
            }}
          />
          <PaperControlPanel
            status={status.data?.value}
            availability={controlAvailability}
            busy={control.isPending}
            onRequest={requestControl}
            transport={transport}
            scope={bootstrap.productSessionToken}
            startAvailable={startAvailable}
            onStarted={async (message) => {
              setControlMessage(message)
              await Promise.all([
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.productSessionToken, "bot"),
                }),
                queryClient.invalidateQueries({
                  queryKey: productKeys.domain(bootstrap.productSessionToken, "execution"),
                }),
              ])
            }}
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
              onCancel={(actionToken) => requestControl({ action: "cancel", actionToken })}
            />
            <FillsPanel
              fills={fills.data?.value ?? []}
              error={fills.error}
              available={fillsAvailable}
            />
          </div>
          <CoverageSummary orders={orders.data} fills={fills.data} />
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

function transportRequest(request: PaperControlIntent): PaperControlRequest {
  return request
}

function PaperSetupNotice() {
  return (
    <div className="mb-4 rounded-xl border border-amber-400/25 bg-amber-400/5 p-4">
      <p className="text-sm font-semibold text-amber-100">Paper practice needs setup</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Some paper features are unavailable. Review Connections or Updates &amp; Repair. Market
        availability alone does not make an investment eligible for paper trading.
      </p>
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
  const active = status?.sessionAvailability === "active"
  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <Summary
        label="Paper session"
        value={status ? sessionLabel(status.sessionAvailability) : "Unavailable"}
        icon={Activity}
        tone={status?.safeguards === "action_needed" ? "bad" : active ? "good" : "neutral"}
      />
      <Summary
        label="Virtual orders"
        value={orderCount?.toLocaleString() ?? "Unavailable"}
        icon={WalletCards}
      />
      <Summary
        label="Virtual fills"
        value={fillCount?.toLocaleString() ?? "Unavailable"}
        icon={ReceiptText}
      />
      <Summary
        label="Safeguards"
        value={status ? safeguardLabel(status.safeguards) : "Unavailable"}
        icon={ShieldCheck}
        tone={status?.safeguards === "action_needed" ? "bad" : "neutral"}
      />
    </div>
  )
}

function SessionSummary({ status }: { status: PaperStatus | undefined }) {
  if (!status) return null
  return (
    <div className="mt-4">
      <Panel title="Session status" subtitle="The current paper-only state and any action needed.">
        {status.sessionAvailability === "active" ? (
          <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Fact label="Mode" value={status.modeLabel} />
            <Fact label="Account update" value={status.accountUpdate === "complete" ? "Complete" : "Incomplete"} />
            <Fact label="Account check" value={reconciliationLabel(status.reconciliation.state)} />
            <Fact label="Real brokerage orders" value="Disabled" />
          </dl>
        ) : (
          <dl className="grid gap-3 sm:grid-cols-2">
            <Fact label="Paper session" value={sessionLabel(status.sessionAvailability)} />
            <Fact label="Safeguards" value={safeguardLabel(status.safeguards)} />
          </dl>
        )}
      </Panel>
    </div>
  )
}

function AccountEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.sessionAvailability !== "active") return null
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel
        title="Virtual balances and profit or loss"
        subtitle="These balances are simulated and are never brokerage balances."
      >
        {status.accounts.rows.length === 0 ? (
          <InlineEmpty detail="No virtual balance is available yet." />
        ) : (
          <div className="space-y-3">
            {status.accounts.rows.map((account, index) => (
              <PaperAccountCard key={`${account.displayName}:${index}`} account={account} />
            ))}
          </div>
        )}
        <EvidenceCount evidence={status.accounts} />
      </Panel>
      <Panel
        title="Virtual positions"
        subtitle="Simulated holdings and cost basis from the current account update."
      >
        {status.positions.rows.length === 0 ? (
          <InlineEmpty detail="No virtual positions yet." />
        ) : (
          <div className="space-y-3">
            {status.positions.rows.map((position, index) => (
              <PositionCard
                key={`${position.accountName}:${position.investment.name}:${index}`}
                position={position}
              />
            ))}
          </div>
        )}
        <EvidenceCount evidence={status.positions} />
      </Panel>
    </div>
  )
}

function PaperAccountCard({ account }: { account: PaperAccount }) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-3">
      <p className="text-sm font-semibold">{account.displayName}</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Current value" value={formatMoney(account.markedEquity)} />
        <Fact label="Starting capital" value={formatMoney(account.settledCapital)} />
        <Fact label="Peak value" value={formatMoney(account.peakMarkedEquity)} />
        <Fact label="Total exposure" value={formatMoney(account.grossExposure)} />
        <Fact label="Realized profit or loss" value={formatMoney(account.realizedPnl)} />
        <Fact label="Unrealized profit or loss" value={formatMoney(account.unrealizedPnl)} />
        <Fact label="Maximum drawdown" value={formatMoney(account.maximumDrawdown)} />
        <Fact label="Eligible for paper trading" value={account.eligible ? "Yes" : "No"} />
      </dl>
    </article>
  )
}

function PositionCard({ position }: { position: PaperPosition }) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-3">
      <p className="text-sm font-semibold">{investmentLabel(position.investment)}</p>
      <p className="mt-1 text-xs text-muted-foreground">
        {position.accountName} · {position.quantity} · cost basis {formatMoney(position.costBasis)}
      </p>
    </article>
  )
}

function SafetyEvidence({ status }: { status: PaperStatus | undefined }) {
  if (status?.sessionAvailability !== "active") return null
  const safety = status.safety
  return (
    <div className="mt-4 grid gap-4 xl:grid-cols-2">
      <Panel
        title="Active safeguards"
        subtitle="Every virtual order must remain inside these account and price protections."
      >
        <dl className="grid gap-3 sm:grid-cols-2">
          <Fact label="Maximum order value" value={formatMoney(safety.maximumOrderValue)} />
          <Fact label="Maximum total exposure" value={formatMoney(safety.maximumTotalExposure)} />
          <Fact label="Maximum position" value={safety.maximumPosition} />
          <Fact label="Leverage limit" value={safety.leverageLimit} />
          <Fact label="Minimum capital" value={formatMoney(safety.minimumCapital)} />
          <Fact label="Maximum loss" value={formatMoney(safety.maximumLoss)} />
          <Fact label="Maximum drawdown" value={formatMoney(safety.maximumDrawdown)} />
          <Fact label="Maximum fees" value={safety.maximumFees} />
          <Fact label="Price protection" value={safety.maximumPriceDeviation} />
          <Fact label="Maximum slippage" value={safety.maximumSlippage} />
          <Fact label="Order pace" value={safety.orderPace} />
          <Fact label="Short selling" value={safety.shorting === "allowed" ? "Allowed" : "Disabled"} />
          <Fact label="Emergency stop" value={safety.emergencyStop === "engaged" ? "Engaged" : "Clear"} />
        </dl>
        <div className="mt-4 border-t border-border/70 pt-4">
          <p className="text-xs font-semibold">Eligible investments</p>
          {safety.eligibleInvestments.rows.length === 0 ? (
            <p className="mt-2 text-xs text-amber-100">No eligible investment is available.</p>
          ) : (
            <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
              {safety.eligibleInvestments.rows.map((investment, index) => (
                <li key={`${investment.name}:${index}`}>{investmentLabel(investment)}</li>
              ))}
            </ul>
          )}
        </div>
      </Panel>
      <Panel
        title="Recent safety decisions"
        subtitle="Why recent virtual orders were allowed, declined, or held for review."
      >
        {status.recentDecisions.rows.length === 0 ? (
          <InlineEmpty detail="No recent paper-trading decision is available." />
        ) : (
          <div className="space-y-3">
            {status.recentDecisions.rows.map((decision, index) => (
              <DecisionCard key={`${decision.observedAt}:${index}`} decision={decision} />
            ))}
          </div>
        )}
        <EvidenceCount evidence={status.recentDecisions} />
      </Panel>
    </div>
  )
}

function DecisionCard({ decision }: { decision: PaperAuditDecision }) {
  return (
    <article className="rounded-lg border border-border/70 bg-background/35 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-sm font-semibold">{riskOutcomeLabel(decision.outcome)}</p>
        {decision.investment ? (
          <p className="text-xs text-muted-foreground">{investmentLabel(decision.investment)}</p>
        ) : null}
      </div>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Decision recorded" value={formatProductTimestamp(decision.observedAt)} />
        <Fact label="Valid until" value={formatProductTimestamp(decision.validUntil)} />
        {decision.marketObservedAt ? (
          <Fact label="Market checked" value={formatProductTimestamp(decision.marketObservedAt)} />
        ) : null}
      </dl>
      {decision.reasons.length > 0 ? (
        <ul className="mt-3 space-y-1 text-xs text-rose-200">
          {decision.reasons.map((reason) => (
            <li key={reason}>{reason}</li>
          ))}
        </ul>
      ) : null}
    </article>
  )
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
  onCancel: (actionToken: string) => void
}) {
  return (
    <Panel title="Virtual orders" subtitle="Paper-only orders and their current results.">
      {!available ? (
        <InlineEmpty detail="Virtual order history is not available in the current setup." />
      ) : error ? (
        <InlineEmpty detail="Virtual order history could not be loaded. Try refreshing." />
      ) : orders.length === 0 ? (
        <InlineEmpty detail="No virtual orders yet." />
      ) : (
        <div className="space-y-3">
          {orders.map((order, index) => (
            <OrderCard
              key={`${order.investment.name}:${order.acceptedAt}:${index}`}
              order={order}
              canCancel={cancelAvailable && order.cancellationAvailable}
              busy={busy}
              onCancel={onCancel}
            />
          ))}
        </div>
      )}
    </Panel>
  )
}

function OrderCard({
  order,
  canCancel,
  busy,
  onCancel,
}: {
  order: PaperOrder
  canCancel: boolean
  busy: boolean
  onCancel: (actionToken: string) => void
}) {
  return (
    <article className="rounded-lg border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-semibold">{investmentLabel(order.investment)}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {directionLabel(order.direction)} · {order.filledQuantity} of {order.requestedQuantity}
          </p>
        </div>
        <EvidenceBadge label={orderStateLabel(order.state)} tone={orderTone(order.state)} />
      </div>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2">
        <Fact
          label="Average fill price"
          value={order.averageFillPrice ? formatMoney(order.averageFillPrice) : "Not filled"}
        />
        <Fact label="Maximum execution price" value={formatMoney(order.maximumExecutionPrice)} />
        <Fact label="Maximum slippage" value={order.maximumSlippage} />
        <Fact label="Fees" value={formatMoney(order.fees)} />
        <Fact label="Accepted" value={formatProductTimestamp(order.acceptedAt)} />
        <Fact label="Expires" value={formatProductTimestamp(order.expiresAt)} />
        <Fact label="Investment plan" value={order.targetLinked ? "Linked" : "Not linked"} />
      </dl>
      {canCancel ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="mt-4"
          disabled={busy}
          onClick={() => onCancel(order.actionToken)}
        >
          Cancel virtual order
        </Button>
      ) : null}
    </article>
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
    <Panel title="Virtual fills" subtitle="Paper-only fills, costs, and fees.">
      {!available ? (
        <InlineEmpty detail="Virtual fill history is not available in the current setup." />
      ) : error ? (
        <InlineEmpty detail="Virtual fill history could not be loaded. Try refreshing." />
      ) : fills.length === 0 ? (
        <InlineEmpty detail="No virtual fills yet." />
      ) : (
        <div className="space-y-3">
          {fills.map((fill, index) => (
            <article
              key={`${fill.investment.name}:${fill.occurredAt}:${index}`}
              className="rounded-lg border border-border bg-background/35 p-4"
            >
              <p className="font-semibold">{investmentLabel(fill.investment)}</p>
              <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                <Fact label="Quantity" value={fill.quantity} />
                <Fact label="Average price" value={formatMoney(fill.averagePrice)} />
                <Fact label="Maximum price" value={formatMoney(fill.maximumPrice)} />
                <Fact label="Trade value" value={formatMoney(fill.notional)} />
                <Fact label="Fee" value={formatMoney(fill.fee)} />
                <Fact label="Completed" value={formatProductTimestamp(fill.occurredAt)} />
              </dl>
            </article>
          ))}
        </div>
      )}
    </Panel>
  )
}

function CompletedAction({ result }: { result: PaperControlResult }) {
  let body: React.ReactNode
  if (result.action === "stop" || result.action === "triggerKillSwitch") {
    body = <p className="text-sm text-muted-foreground">{result.value.message}</p>
  } else if (result.action === "cancel") {
    body = (
      <dl className="grid gap-3 sm:grid-cols-2">
        <Fact label="Cancellation" value={cancelStateLabel(result.value.state)} />
        <Fact label="Checked" value={formatProductTimestamp(result.value.observedAt)} />
        <Fact label="Filled quantity" value={result.value.filledQuantity} />
        <Fact
          label="Average fill price"
          value={
            result.value.averageFillPrice
              ? formatMoney(result.value.averageFillPrice)
              : "Not filled"
          }
        />
        <Fact label="Fees" value={formatMoney(result.value.fees)} />
      </dl>
    )
  }
  return (
    <div className="mt-4">
      <Panel title="Latest completed action" subtitle="Review the latest change before continuing.">
        {body}
      </Panel>
    </div>
  )
}

function CoverageSummary({
  orders,
  fills,
}: {
  orders: { returnedItems: number; availableItems: number } | undefined
  fills: { returnedItems: number; availableItems: number } | undefined
}) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Virtual orders: {countBoundary(orders)}. Virtual fills: {countBoundary(fills)}. Results are
      simulated and may differ from real trading because prices, timing, liquidity, and costs can
      change.
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
        className={
          tone === "good"
            ? "size-4 text-emerald-400"
            : tone === "bad"
              ? "size-4 text-rose-400"
              : "size-4 text-primary"
        }
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
            Market Squawk · Practice
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Paper Trading</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Practice investment plans with virtual cash, current market conditions, and the same
            safety checks used throughout Market Squawk.
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
            Practice only — no real money or brokerage orders
          </h2>
          <p className="mt-1 max-w-4xl text-xs leading-5 text-muted-foreground">
            Every balance, position, order, fill, fee, and profit or loss on this page is virtual.
            An investment recommendation never becomes an order automatically, and this page
            cannot instruct a brokerage account.
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

function Notice({ text }: { text: string }) {
  return (
    <div className="mt-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-100">
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
  return (
    <span className={`inline-flex rounded-md border px-2 py-1 text-[10px] font-medium ${style}`}>
      {label}
    </span>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs">{value}</dd>
    </div>
  )
}

function EvidenceCount({
  evidence,
}: {
  evidence: { returnedItems: number; availableItems: number }
}) {
  return (
    <p className="mt-3 text-[10px] text-muted-foreground">
      Showing {evidence.returnedItems} of {evidence.availableItems}.
    </p>
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
  value: { returnedItems: number; availableItems: number } | undefined,
) {
  return value ? `${value.returnedItems} of ${value.availableItems} shown` : "unavailable"
}

function investmentLabel(investment: { name: string; symbol: string | null }): string {
  return investment.symbol ? `${investment.name} (${investment.symbol})` : investment.name
}

function sessionLabel(value: PaperStatus["sessionAvailability"]): string {
  switch (value) {
    case "ready":
      return "Ready"
    case "active":
      return "Active"
    case "unavailable":
      return "Unavailable"
  }
}

function safeguardLabel(value: PaperStatus["safeguards"]): string {
  return value === "active" ? "Active" : "Action needed"
}

function reconciliationLabel(value: "current" | "action_needed" | "incomplete"): string {
  switch (value) {
    case "current":
      return "Current"
    case "action_needed":
      return "Action needed"
    case "incomplete":
      return "Incomplete"
  }
}

function riskOutcomeLabel(value: PaperAuditDecision["outcome"]): string {
  switch (value) {
    case "declined":
      return "Declined"
    case "approved":
      return "Approved by safeguards"
    case "accepted":
      return "Accepted for virtual execution"
    case "needs_review":
      return "Needs review"
    case "cancel_requested":
      return "Cancellation requested"
    case "cancelled":
      return "Cancelled"
    case "reconciled":
      return "Account check completed"
  }
}

function orderStateLabel(value: PaperOrder["state"]): string {
  switch (value) {
    case "waiting":
      return "Waiting"
    case "accepted":
      return "Accepted"
    case "partially_filled":
      return "Partially filled"
    case "filled":
      return "Filled"
    case "cancel_requested":
      return "Cancellation requested"
    case "cancelled":
      return "Cancelled"
    case "declined":
      return "Declined"
    case "expired":
      return "Expired"
  }
}

function orderTone(value: PaperOrder["state"]): "neutral" | "warn" | "bad" {
  if (value === "declined") return "bad"
  if (value === "partially_filled") return "warn"
  return "neutral"
}

function directionLabel(value: PaperOrder["direction"]): string {
  return value === "buy" ? "Buy" : "Sell"
}

function cancelStateLabel(value: "pending" | "cancelled" | "already_complete"): string {
  switch (value) {
    case "pending":
      return "Requested"
    case "cancelled":
      return "Cancelled"
    case "already_complete":
      return "Order already complete"
  }
}

function formatProductTimestamp(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? "Unavailable" : date.toLocaleString()
}
