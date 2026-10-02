import * as React from "react"
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query"
import { Tabs } from "radix-ui"

import { productKeys, snapshotQueryMeta } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { CursorNavigation, useCursorNavigation } from "@/features/shared/cursor-navigation"
import { formatMoney, groupDecimal } from "@/lib/formatters"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import { FinancialPreparation } from "./financial-preparation"
import {
  parseInvestmentFinancialsResult,
  type InvestmentFinancialDate,
  type InvestmentFinancialEnvelope,
  type InvestmentFinancialFact,
  type InvestmentFinancialFiling,
  type InvestmentFinancialRatio,
  type InvestmentFinancialSection,
  type InvestmentFinancialSnapshot,
  type InvestmentFinancialStatement,
  type InvestmentFinancialTime,
  type InvestmentFinancialsResult,
} from "./investment-financials-schema"

type FinancialProps = {
  selectionToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}

const sectionLabels = {
  facts: "Reported financial facts",
  statements: "Financial statements",
  ratios: "Financial ratios",
  filings: "Filings",
} satisfies Record<InvestmentFinancialSection, string>

export function InvestmentFinancials(props: FinancialProps) {
  if (!hasProductCapability(props.bootstrap, "investment_financials")
    || !hasProductCapability(props.bootstrap, "investment_financials_close")) {
    return <section className="space-y-3" aria-label="Investment financial information">
      <h2 className="text-lg font-semibold">Financial information</h2>
      <p role="status" className="text-sm text-muted-foreground">Financial details are not available in this app session.</p>
    </section>
  }
  return <SelectedFinancials key={`${props.bootstrap.productSessionToken}:${props.selectionToken}`} {...props} />
}

function SelectedFinancials(props: FinancialProps) {
  const [preparedRevision, setPreparedRevision] = React.useState(0)
  const [preparationCompleted, setPreparationCompleted] = React.useState(false)
  const onPrepared = React.useCallback(async () => {
    setPreparationCompleted(true)
    setPreparedRevision((value) => value + 1)
  }, [])
  const onSettled = React.useCallback(async () => {
    setPreparationCompleted(false)
    setPreparedRevision((value) => value + 1)
  }, [])
  return <section className="rounded-xl border border-border bg-card/30 p-4" aria-label="Investment financial information">
    <h2 className="text-base font-semibold">Financial information</h2>
    <p className="mt-1 text-xs text-muted-foreground">Reported company values, with their reporting periods and source context.</p>
    <div className="mt-4"><FinancialPreparation {...props} onPrepared={onPrepared} onSettled={onSettled} /></div>
    <Tabs.Root defaultValue="facts" activationMode="manual" className="mt-4">
      <Tabs.List aria-label="Financial sections" className="flex flex-wrap gap-1 border-b border-border pb-2">
        {(["facts", "statements", "ratios", "filings"] as const).map((section) => <Tabs.Trigger key={section} value={section}
          className="rounded-md px-3 py-2 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring data-[state=active]:bg-primary/10 data-[state=active]:text-primary">
          {{ facts: "Facts", statements: "Statements", ratios: "Ratios", filings: "Filings" }[section]}
        </Tabs.Trigger>)}
      </Tabs.List>
      {(["facts", "statements", "ratios", "filings"] as const).map((section) => <Tabs.Content key={section} value={section} className="pt-4 focus-visible:outline-ring">
        <FinancialSectionRead {...props} section={section} preparedRevision={preparedRevision} preparationCompleted={preparationCompleted} />
      </Tabs.Content>)}
    </Tabs.Root>
  </section>
}

function FinancialSectionRead({ selectionToken, section, bootstrap, transport, preparedRevision, preparationCompleted }: FinancialProps & {
  section: InvestmentFinancialSection
  preparedRevision: number
  preparationCompleted: boolean
}) {
  const queryClient = useQueryClient()
  const navigation = useCursorNavigation()
  const snapshot = React.useRef<InvestmentFinancialSnapshot | undefined>(undefined)
  const lastChecked = React.useRef<InvestmentFinancialsResult | undefined>(undefined)
  const alive = React.useRef(true)
  const epoch = React.useRef(0)
  const [revision, setRevision] = React.useState(0)
  const [releasing, setReleasing] = React.useState(false)
  const [releaseFailed, setReleaseFailed] = React.useState(false)
  const [updateAvailable, setUpdateAvailable] = React.useState(false)
  const seenPreparedRevision = React.useRef(preparedRevision)
  const closeRead = React.useCallback(async (readToken: string) => {
    try {
      await transport.query({ query: "closeInvestmentFinancials", selectionToken, readToken })
      return true
    } catch {
      if (alive.current) setReleaseFailed(true)
      return false
    }
  }, [selectionToken, transport])

  React.useEffect(() => {
    alive.current = true
    return () => {
      alive.current = false
      epoch.current += 1
      const readToken = snapshot.current?.readToken
      snapshot.current = undefined
      lastChecked.current = undefined
      if (readToken) void closeRead(readToken)
    }
  }, [closeRead])

  const cursor = navigation.after
  const queryKey = productKeys.operation(bootstrap.productSessionToken, "research", "Research.GetInvestmentFinancials", {
    selectionToken, section, cursor, revision,
  })
  const page = useQuery({
    queryKey,
    gcTime: 0,
    meta: snapshotQueryMeta,
    retry: false,
    refetchOnWindowFocus: false,
    placeholderData: keepPreviousData,
    enabled: !releasing,
    queryFn: async ({ signal }) => {
      const requestEpoch = epoch.current
      const expectedSnapshot = cursor === undefined ? undefined : snapshot.current
      const result = parseInvestmentFinancialsResult(await transport.query({
        query: "investmentFinancials", selectionToken, section, limit: 32,
        ...(cursor === undefined ? {} : { cursor }),
      }, { signal }), { selectionToken, section, cursor, snapshot: expectedSnapshot })
      if (signal.aborted || !alive.current || requestEpoch !== epoch.current) {
        if (result.readToken && result.readToken !== snapshot.current?.readToken) void closeRead(result.readToken)
        throw new DOMException("The view was closed.", "AbortError")
      }
      const oldToken = snapshot.current?.readToken
      if (result.state === "expired") {
        snapshot.current = undefined
        if (oldToken) void closeRead(oldToken)
      } else {
        snapshot.current = { readToken: result.readToken, knowledgeAt: result.knowledgeAt, effectiveOn: result.effectiveOn }
        if (oldToken && oldToken !== result.readToken) void closeRead(oldToken)
      }
      lastChecked.current = result
      return result
    },
  })

  const refresh = React.useCallback(async () => {
    setReleasing(true)
    setReleaseFailed(false)
    epoch.current += 1
    await queryClient.cancelQueries({ queryKey, exact: true })
    const oldToken = snapshot.current?.readToken
    if (oldToken && !await closeRead(oldToken)) {
      if (alive.current) setReleasing(false)
      return
    }
    if (!alive.current) return
    snapshot.current = undefined
    setUpdateAvailable(false)
    navigation.restart()
    setRevision((value) => value + 1)
    setReleasing(false)
  }, [queryClient, queryKey, closeRead, navigation])
  React.useEffect(() => {
    if (seenPreparedRevision.current === preparedRevision) return
    seenPreparedRevision.current = preparedRevision
    // An already paged read keeps its original snapshot and exact cursor. An
    // inactive tab mounts a new reader and obtains fresh retained information.
    if (navigation.page === 1) void refresh()
    else setUpdateAvailable(true)
  }, [preparedRevision, navigation.page, refresh])
  const result = page.data ?? lastChecked.current
  const busy = page.isFetching || releasing
  const showingPrior = page.isPlaceholderData || page.isError || releasing
  const retainedPage = result && !page.isPlaceholderData && result.readToken === snapshot.current?.readToken

  return <section aria-label={sectionLabels[section]}>
    <div className="flex items-start justify-between gap-4">
      <h3 className="text-base font-semibold">{sectionLabels[section]}</h3>
      <Button variant="outline" size="sm" disabled={busy} onClick={() => void refresh()}>Refresh this section</Button>
    </div>
    <div className="mt-2 min-h-16 text-xs leading-5">
    {updateAvailable ? <p role="status" className="text-muted-foreground">{preparationCompleted
      ? "Updated financial information is ready. Refresh this section to open it."
      : "Financial preparation ended. Refresh this section to check for saved information."}</p> : null}
    {releaseFailed ? <p role="alert" className="text-destructive">The previous financial information could not be released. Try refreshing this section.</p>
      : page.isError ? <div className="flex items-start justify-between gap-3">
      <p role="alert" className="text-destructive">{result
        ? "This financial information could not be updated. Showing the last checked page; its currentness has not been verified."
        : "This financial information could not be loaded. Try again."}</p>
      <Button variant="outline" size="sm" disabled={busy} onClick={() => void page.refetch()}>Retry</Button>
    </div> : busy ? <p role="status" className="text-muted-foreground">{result ? "Updating this financial information… Showing the last checked page." : "Loading this financial information…"}</p> : null}
    </div>
    <div className="min-h-[280px]">
    {result ? <>
      {result.knowledgeAt !== null && result.effectiveOn !== null ? <p className="text-xs leading-5 text-muted-foreground">{showingPrior ? "Last checked information through" : "Information through"} <time dateTime={result.knowledgeAt} title={result.knowledgeAt}>{new Date(result.knowledgeAt).toLocaleString()}</time>
        {" · Reporting cutoff "}<time dateTime={result.effectiveOn}>{result.effectiveOn}</time>{" · Latest information known at that date"}</p> : null}
      {result.state !== "reported" ? <p role="status" className="mt-3 text-sm text-muted-foreground">{sectionAvailability(result.state)}</p> : null}
      <FinancialFamilies families={result.families} />
      <FinancialLimitations result={result} />
      <FinancialItems result={result} />
      {result.currentCursor !== null ? <CursorNavigation navigation={navigation} current={result.currentCursor}
        next={retainedPage && !page.isError ? result.nextCursor : null} busy={busy}
        onRestart={() => void refresh()} /> : null}
      {result.state === "expired" ? <Button variant="outline" size="sm" className="mt-3" disabled={busy} onClick={() => void refresh()}>Open fresh information</Button> : null}
    </> : null}
    </div>
  </section>
}

function FinancialItems({ result }: { result: InvestmentFinancialsResult }) {
  switch (result.section) {
    case "facts": return <div className="mt-4 divide-y divide-border">{result.items.map((fact, index) => <FinancialFact key={index} fact={fact} />)}</div>
    case "statements": return <div className="mt-4 divide-y divide-border">{result.items.map((statement, index) => <FinancialStatement key={index} statement={statement} />)}</div>
    case "ratios": return <div className="mt-4 divide-y divide-border">{result.items.map((ratio, index) => <FinancialRatio key={index} ratio={ratio} />)}</div>
    case "filings": return <div className="mt-4 divide-y divide-border">{result.items.map((filing, index) => <FinancialFiling key={index} filing={filing} />)}</div>
  }
}

function FinancialFact({ fact }: { fact: InvestmentFinancialFact }) {
  return <article className="py-4">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h4 className="text-sm font-medium">{fact.displayName}</h4>
      <p className="break-words font-mono text-sm">{fact.unit.kind === "shares" ? `${groupDecimal(fact.value)} shares`
        : `${formatMoney({ amount: fact.value, currency: fact.unit.currency })}${fact.unit.kind === "currency_per_share" ? " per share" : ""}`}</p>
    </div>
    <p className="mt-2 text-xs text-muted-foreground"><FinancialPeriod period={fact.period} /> · {revisionLabel(fact.revision)} · {fact.scope === "company_wide" ? "Company-wide report" : "Individual filing detail"}</p>
    <details className="mt-3 border-t border-border pt-2">
      <summary className="cursor-pointer text-xs focus-visible:outline-ring">Reporting context and dates</summary>
      <FinancialEnvelope envelope={fact} />
    </details>
  </article>
}

function FinancialStatement({ statement }: { statement: InvestmentFinancialStatement }) {
  const labels = { financial_position: "Financial position", operations: "Income and operations", cash_flows: "Cash flows", share_data: "Share information" }
  return <article className="py-4">
    <h4 className="text-sm font-semibold">{labels[statement.statement]}</h4>
    <FinancialEnvelope envelope={statement.envelope} />
    <div className="mt-3 divide-y divide-border">{statement.items.map((fact, index) => <FinancialFact key={index} fact={fact} />)}</div>
  </article>
}

function FinancialRatio({ ratio }: { ratio: InvestmentFinancialRatio }) {
  const reasons: Record<InvestmentFinancialRatio["state"], string> = {
    reported: "Reported", missing_input: "A required reported value is missing.",
    conflicting_input: "The reported values conflict.", incompatible_units: "The reported values use incompatible units.",
    zero_denominator: "The comparison value is zero, so this ratio cannot be calculated.", unavailable: "This ratio is unavailable.",
  }
  return <article className="py-4">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h4 className="text-sm font-medium">{ratio.displayName}</h4>
      <p className="font-mono text-sm">{ratio.value === null ? "Unavailable" : `${groupDecimal(ratio.value)} ratio`}</p>
    </div>
    {ratio.state !== "reported" ? <p className="mt-2 text-xs text-muted-foreground">{reasons[ratio.state]}</p> : null}
    {ratio.envelope ? <FinancialEnvelope envelope={ratio.envelope} /> : <p className="mt-2 text-xs text-muted-foreground">A reporting period is not available.</p>}
    <details className="mt-3 border-t border-border pt-2">
      <summary className="cursor-pointer text-xs focus-visible:outline-ring">Reported values used for this ratio</summary>
      {ratio.inputs.length === 0 ? <p className="mt-2 text-xs text-muted-foreground">No supporting reported values are available.</p>
        : <div className="mt-3 space-y-3">{ratio.inputs.map((input, index) => <div key={index}>
          <p className="mb-1 text-xs text-muted-foreground">{input.role === "numerator" ? "Amount being compared (numerator)" : "Comparison amount (denominator)"}</p>
          <FinancialFact fact={input.fact} />
        </div>)}</div>}
    </details>
  </article>
}

function FinancialFiling({ filing }: { filing: InvestmentFinancialFiling }) {
  return <article className="py-4">
    <h4 className="text-sm font-medium">Form {filing.form}</h4>
    <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
      <ContextValue label="Revision">{revisionLabel(filing.revision)}</ContextValue>
      <ContextValue label="Applies to"><FinancialTime value={filing.effective} /></ContextValue>
      <ContextValue label="Published">{filing.published ? <FinancialTime value={filing.published} /> : "Publication date not reported"}</ContextValue>
      <ContextValue label="Known at"><FinancialInstant value={filing.knownAt} /></ContextValue>
    </dl>
  </article>
}

function FinancialEnvelope({ envelope }: { envelope: InvestmentFinancialEnvelope }) {
  const fiscalPeriods = { fiscal_year: "Fiscal year", calendar_year: "Calendar year", first_quarter: "First quarter", second_quarter: "Second quarter", third_quarter: "Third quarter", fourth_quarter: "Fourth quarter", unavailable: "Fiscal period not reported" }
  const cadences = { annual: "Annual", quarterly: "Quarterly", other: "Other reporting cadence", unavailable: "Reporting cadence not available" }
  const consolidation = { reported_consolidated: "Consolidated", reported_non_consolidated: "Not consolidated", unavailable: "Consolidation not reported" }
  const restatement = { reported_restated: "Reported as restated", reported_not_restated: "Reported as not restated", unavailable: "Restatement status not reported" }
  const amendment = { original: "Original report", amendment: "Amended report", unavailable: "Amendment status not reported" }
  return <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
    <ContextValue label="Reporting period"><FinancialPeriod period={envelope.period} /></ContextValue>
    <ContextValue label="Fiscal period">{fiscalPeriods[envelope.fiscalContext.fiscalPeriod]}{envelope.fiscalContext.fiscalYear !== null ? ` ${envelope.fiscalContext.fiscalYear}` : ""} · {cadences[envelope.fiscalContext.cadence]}</ContextValue>
    <ContextValue label="Reporting basis">{consolidation[envelope.reportingContext.consolidation]}</ContextValue>
    <ContextValue label="Report status">{amendment[envelope.reportingContext.amendment]} · {restatement[envelope.reportingContext.restatement]}</ContextValue>
    <ContextValue label="Detail scope">{envelope.reportingContext.dimensionality === "no_dimensions" ? "No segment breakdown reported" : "Segment breakdown not available"}</ContextValue>
    <ContextValue label="Reported occurrence">{envelope.reportingContext.occurrence}</ContextValue>
    <ContextValue label="Filed on">{envelope.filedOn ? <FinancialDate value={envelope.filedOn} /> : "Filing date not reported"}</ContextValue>
    <ContextValue label="Effective"><FinancialTime value={envelope.effective} /></ContextValue>
    <ContextValue label="Known at"><FinancialInstant value={envelope.knownAt} /></ContextValue>
  </dl>
}

function ContextValue({ label, children }: { label: string; children: React.ReactNode }) {
  return <div className="min-w-0"><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 break-words">{children}</dd></div>
}

function FinancialPeriod({ period }: { period: InvestmentFinancialEnvelope["period"] }) {
  return period.kind === "instant" ? <>As of <FinancialDate value={period.instant} /></>
    : <><FinancialDate value={period.start} /> to <FinancialDate value={period.end} /></>
}

function FinancialDate({ value }: { value: InvestmentFinancialDate }) {
  const date = `${String(value.year).padStart(4, "0")}-${String(value.month).padStart(2, "0")}-${String(value.day).padStart(2, "0")}`
  return <time dateTime={date}>{date}</time>
}

function FinancialTime({ value }: { value: InvestmentFinancialTime }) {
  return value.precision === "calendar_date" ? <FinancialDate value={value.value} /> : <FinancialInstant value={value.value} />
}

function FinancialInstant({ value }: { value: string }) {
  const nanos = BigInt(value)
  const remainder = ((nanos % 1_000_000_000n) + 1_000_000_000n) % 1_000_000_000n
  const seconds = (nanos - remainder) / 1_000_000_000n
  const instant = new Date(Number(seconds * 1_000n)).toISOString().replace(/\.000Z$/, `.${String(remainder).padStart(9, "0")}Z`)
  return <time dateTime={instant} title={instant}>{formatTimestamp(value)}</time>
}

function revisionLabel(revision: InvestmentFinancialFact["revision"]): string {
  return revision === "current" ? "Current revision at the information date"
    : revision === "superseded" ? "Superseded revision" : "Revision history cannot be compared"
}

function sectionAvailability(state: InvestmentFinancialsResult["state"]): string {
  switch (state) {
    case "reported": return "Reported information is available."
    case "missing": return "No reported information is available for this section at the information date."
    case "conflict": return "Conflicting evidence prevents this section from being established."
    case "unavailable": return "This financial section is unavailable with the current evidence."
    case "expired": return "This information is no longer open. Refresh this section to read current saved information."
  }
}

function FinancialFamilies({ families }: { families: InvestmentFinancialsResult["families"] }) {
  const labels = { company_facts: "Company reports", filing_details: "Detailed filing information", filings: "Filing history" }
  const reasons = {
    identity_missing: "The company relationship has not been established.", identity_ambiguous: "More than one company relationship matches.",
    identity_stale: "The company relationship needs to be checked again.", identity_revoked: "The previous company relationship is no longer valid.",
    revision_conflict: "Reported revisions conflict.", no_records: "No records are available at the information date.",
    evidence_unavailable: "The supporting information could not be checked.", rights_unavailable: "This information is not available for the requested use.",
  }
  return families.length === 0 ? null : <details className="mt-3 border-t border-border pt-2">
    <summary className="cursor-pointer text-xs focus-visible:outline-ring">Information coverage</summary>
    <dl className="mt-3 space-y-3 text-xs">{families.map((family) => <ContextValue key={family.family} label={labels[family.family]}>
      {family.state === "reported" ? "Reported information available" : family.reason ? reasons[family.reason] : sectionAvailability(family.state)}
    </ContextValue>)}</dl>
  </details>
}

function FinancialLimitations({ result }: { result: InvestmentFinancialsResult }) {
  const labels = {
    some_reported_facts_not_supported: "Some reported facts cannot be shown with their available context.",
    item_exceeds_response_limit: "Some information is too large for this page and has not been shown.",
    read_expired: "Refresh this section to open fresh information.",
  }
  if (result.omittedItems === 0 && result.limitations.length === 0) return null
  return <div className="mt-3 space-y-1 text-xs text-muted-foreground">
    {result.omittedItems > 0 ? <p>{groupDecimal(String(result.omittedItems))} reported items not shown.</p> : null}
    {result.limitations.map((limitation) => <p key={limitation}>{labels[limitation]}</p>)}
  </div>
}
