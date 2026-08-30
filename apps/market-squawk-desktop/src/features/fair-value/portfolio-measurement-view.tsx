import * as React from "react"
import { BadgeCheck, CircleAlert, Landmark, Layers3, ShieldCheck } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Label } from "@/components/ui/label"
import { formatMoney, humanize } from "@/lib/formatters"

import type {
  PortfolioMeasurementAccount,
  PortfolioMeasurementHolding,
  PortfolioMeasurementMethod,
  PortfolioMeasurementResult,
} from "./portfolio-measurement-contracts"

export const PORTFOLIO_MEASUREMENT_METHODS: ReadonlyArray<{
  value: PortfolioMeasurementMethod
  name: string
  explanation: string
}> = [
  {
    value: "market_approach",
    name: "Market approach",
    explanation: "Uses observable market evidence or comparable market information.",
  },
  {
    value: "quoted_market_price",
    name: "Quoted market price",
    explanation:
      "Declares a quoted-price technique. Portfolio evidence alone will not qualify it as Level 1.",
  },
  {
    value: "income_approach",
    name: "Income approach",
    explanation: "Values expected future cash flows or earnings using explicit assumptions.",
  },
  {
    value: "cost_approach",
    name: "Cost approach",
    explanation: "Uses current replacement or reproduction cost as the valuation technique.",
  },
]

export type PortfolioMeasurementSignificance = "significant" | "not_significant"

export function PortfolioEvidenceSummary({
  account,
  holding,
  bounded,
}: {
  account: PortfolioMeasurementAccount
  holding: PortfolioMeasurementHolding
  bounded: boolean
}) {
  return (
    <div className="grid overflow-hidden rounded-lg border border-border bg-background/40 sm:grid-cols-2 xl:grid-cols-4">
      <EvidenceFact
        icon={Landmark}
        label="Current value"
        value={formatMoney(holding.marketValue)}
        detail={`Quantity ${holding.quantity}`}
      />
      <EvidenceFact
        icon={Layers3}
        label="Portfolio state"
        value="Current saved position"
        detail={bounded ? "Additional holdings are not shown" : "All current holdings are included"}
      />
      <EvidenceFact
        icon={ShieldCheck}
        label="Data quality"
        value={humanize(holding.price.confidence)}
        detail={`${humanize(holding.price.state)} · not a live trading quote`}
      />
      <EvidenceFact
        icon={BadgeCheck}
        label="Reconciliation"
        value={
          account.reconciliationDiscrepancies === 0
            ? "No reported breaks"
            : `${account.reconciliationDiscrepancies} breaks`
        }
        detail="Compared with the imported account totals"
      />
    </div>
  )
}

export function MeasurementSuccess({ result }: { result: PortfolioMeasurementResult }) {
  const hierarchy = result.measurement.classification?.hierarchy ?? "unclassified"
  const ready = hierarchy !== "unclassified"
  return (
    <Alert className="mt-4 border-emerald-400/25 bg-emerald-400/5">
      <BadgeCheck className="text-emerald-300" aria-hidden="true" />
      <AlertTitle>Measurement saved</AlertTitle>
      <AlertDescription>
        <span className="block">
          Classification: <strong>{humanize(hierarchy)}</strong> ·{" "}
          {ready ? "ready for governed review" : "more evidence or review is required"}.
        </span>
        <span className="mt-1 block text-[10px]">
          Open the measurement below to review its inputs and classification.
        </span>
      </AlertDescription>
    </Alert>
  )
}

export function MeasurementField({
  label,
  htmlFor,
  detail,
  error,
  children,
}: {
  label: string
  htmlFor: string
  detail: string
  error?: string | null
  children: React.ReactNode
}) {
  return (
    <div>
      <Label htmlFor={htmlFor}>{label}</Label>
      <div className="mt-2">{children}</div>
      <p
        id={`${htmlFor}-help`}
        className={`mt-1.5 text-[10px] leading-4 ${
          error ? "text-destructive" : "text-muted-foreground"
        }`}
      >
        {error ?? detail}
      </p>
    </div>
  )
}

export function WorkflowError({
  title = "Measurement was not created",
  message,
}: {
  title?: string
  message: string
}) {
  return (
    <Alert variant="destructive" className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  )
}

export function WorkflowNotice({ title, message }: { title: string; message: string }) {
  return (
    <Alert className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  )
}

function EvidenceFact({
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
    <div className="border-border p-3 sm:border-r sm:last:border-r-0">
      <Icon className="size-3.5 text-primary" aria-hidden="true" />
      <p className="mt-2 text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-xs font-semibold">{value}</p>
      <p className="mt-1 text-[9px] leading-4 text-muted-foreground">{detail}</p>
    </div>
  )
}

export function shortIdentity(value: string) {
  return value.length <= 16 ? value : `${value.slice(0, 8)}…${value.slice(-6)}`
}
