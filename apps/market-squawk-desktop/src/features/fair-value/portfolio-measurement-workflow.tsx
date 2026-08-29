import * as React from "react"
import { useInfiniteQuery, useMutation, useQuery } from "@tanstack/react-query"
import {
  CircleAlert,
  FileCheck2,
  LoaderCircle,
  RefreshCw,
} from "lucide-react"

import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  changedPortfolio,
  exactAmountError,
  parsePortfolioMeasurementAccounts,
  parsePortfolioMeasurementHoldings,
  parsePortfolioMeasurementPrincipals,
  parsePortfolioMeasurementResult,
  type PortfolioMeasurementAmountBasis,
  type PortfolioMeasurementMethod,
  verifyPortfolioMeasurementEvidence,
} from "./portfolio-measurement-contracts"
import { readFreshPortfolioMeasurementContext } from "./portfolio-measurement-context"
import {
  MeasurementField,
  MeasurementSuccess,
  PORTFOLIO_MEASUREMENT_METHODS,
  PortfolioEvidenceSummary,
  type PortfolioMeasurementSignificance,
  shortIdentity,
  WorkflowError,
  WorkflowNotice,
} from "./portfolio-measurement-view"

const REQUIRED_OPERATIONS = [
  "FairValue.Measure",
  "FairValue.GetEvidence",
  "Portfolio.ListAccounts",
  "Portfolio.GetHoldings",
  "Governance.ListPrincipals",
] as const

class RetainedMeasurementVerificationError extends Error {}

export function PortfolioMeasurementWorkflow({
  bootstrap,
  transport,
  onCreated,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onCreated: (measurementId: string) => void | Promise<void>
}) {
  const advertised = React.useMemo(
    () => new Set(bootstrap.operations.map((operation) => operation.name)),
    [bootstrap.operations],
  )
  const missingOperations = REQUIRED_OPERATIONS.filter(
    (operation) => !advertised.has(operation),
  )
  const available = missingOperations.length === 0
  const [selectedAccountId, setSelectedAccountId] = React.useState("")
  const [selectedInstrumentId, setSelectedInstrumentId] = React.useState("")
  const [amount, setAmount] = React.useState("")
  const [currency, setCurrency] = React.useState("")
  const [scale, setScale] = React.useState("2")
  const [amountBasis, setAmountBasis] =
    React.useState<PortfolioMeasurementAmountBasis>("per_instrument_unit")
  const [method, setMethod] = React.useState<PortfolioMeasurementMethod>("market_approach")
  const [significance, setSignificance] =
    React.useState<PortfolioMeasurementSignificance>("significant")
  const [principalId, setPrincipalId] = React.useState("")
  const [announcement, setAnnouncement] = React.useState("")

  const accounts = useInfiniteQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Portfolio",
      "Portfolio.ListAccounts",
      { purpose: "fair-value-measurement" },
    ),
    initialPageParam: undefined as string | undefined,
    enabled: available,
    queryFn: async ({ pageParam }) =>
      parsePortfolioMeasurementAccounts(
        await transport.query({
          query: "portfolioAccounts",
          ...(pageParam ? { afterAccountId: pageParam } : {}),
        }),
      ),
    getNextPageParam: (page) => {
      if (page.completeness === "complete") return undefined
      return page.accounts.at(-1)?.accountId
    },
  })
  const accountSelections = React.useMemo(
    () =>
      (accounts.data?.pages ?? []).flatMap((page, pageIndex) => {
        const afterAccountId = accounts.data?.pageParams[pageIndex]
        return page.accounts.map((account) => ({
          account,
          ...(typeof afterAccountId === "string" ? { afterAccountId } : {}),
        }))
      }),
    [accounts.data],
  )
  const duplicateAccount = hasDuplicate(accountSelections.map(({ account }) => account.accountId))
  const selectedAccount = accountSelections.find(
    ({ account }) => account.accountId === selectedAccountId,
  )

  React.useEffect(() => {
    if (!selectedAccount && accountSelections[0]) {
      setSelectedAccountId(accountSelections[0].account.accountId)
    }
  }, [accountSelections, selectedAccount])

  const holdings = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Portfolio",
      "Portfolio.GetHoldings",
      {
        accountId: selectedAccount?.account.accountId ?? null,
        revisionId: selectedAccount?.account.currentRevision.revisionId ?? null,
        purpose: "fair-value-measurement",
      },
    ),
    enabled: available && selectedAccount !== undefined && !duplicateAccount,
    queryFn: async () => {
      if (!selectedAccount) throw changedPortfolio()
      return parsePortfolioMeasurementHoldings(
        await transport.query({
          query: "portfolioHoldings",
          accountId: selectedAccount.account.accountId,
        }),
        selectedAccount.account,
      )
    },
  })
  const selectedHolding = holdings.data?.holdings.find(
    (holding) => holding.instrument_id === selectedInstrumentId,
  )

  React.useEffect(() => {
    if (!selectedHolding && holdings.data?.holdings[0]) {
      setSelectedInstrumentId(holdings.data.holdings[0].instrument_id)
    }
  }, [holdings.data, selectedHolding])

  const holdingSeed = selectedHolding
    ? `${selectedHolding.revisionId}:${selectedHolding.instrument_id}:${selectedHolding.market_value.amount}:${selectedHolding.market_value.currency}`
    : ""
  React.useEffect(() => {
    if (!selectedHolding) return
    setAmount(selectedHolding.market_value.amount)
    setCurrency(selectedHolding.market_value.currency)
    setScale(String(decimalPlaces(selectedHolding.market_value.amount)))
  }, [holdingSeed])

  const principals = useInfiniteQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Governance",
      "Governance.ListPrincipals",
      { purpose: "fair-value-preparer" },
    ),
    initialPageParam: undefined as string | undefined,
    enabled: available,
    queryFn: async ({ pageParam }) =>
      parsePortfolioMeasurementPrincipals(
        await transport.governanceQuery({
          query: "principals",
          limit: 64,
          ...(pageParam ? { after: pageParam } : {}),
        }),
      ),
    getNextPageParam: (page) => page.nextAfter ?? undefined,
  })
  const principalSelections = React.useMemo(
    () =>
      (principals.data?.pages ?? []).flatMap((page, pageIndex) => {
        const after = principals.data?.pageParams[pageIndex]
        return page.principals.map((principal) => ({
          principal,
          ...(typeof after === "string" ? { after } : {}),
        }))
      }),
    [principals.data],
  )
  const duplicatePrincipal = hasDuplicate(
    principalSelections.map(({ principal }) => principal.principalId),
  )
  const selectedPrincipal = principalSelections.find(
    ({ principal }) => principal.principalId === principalId,
  )
  React.useEffect(() => {
    if (!selectedPrincipal && principalSelections[0]) {
      setPrincipalId(principalSelections[0].principal.principalId)
    }
  }, [principalSelections, selectedPrincipal])

  const scaleNumber = scale === "" ? Number.NaN : Number(scale)
  const amountIssue = exactAmountError(amount, scaleNumber)
  const currencyIssue = /^[A-Z]{3}$/.test(currency)
    ? null
    : "Currency must be a three-letter uppercase code such as USD."
  const ready =
    available &&
    !duplicateAccount &&
    !duplicatePrincipal &&
    selectedAccount !== undefined &&
    selectedHolding !== undefined &&
    selectedPrincipal !== undefined &&
    amountIssue === null &&
    currencyIssue === null

  const measure = useMutation({
    mutationFn: async () => {
      if (!ready || !selectedAccount || !selectedHolding || !selectedPrincipal) {
        throw new Error("Complete each measurement field before continuing.")
      }
      const context = await readFreshPortfolioMeasurementContext(
        transport,
        selectedAccount,
        selectedHolding,
        selectedPrincipal,
      )
      const at = new Date().toISOString()
      const expected = {
        accountId: context.account.accountId,
        instrumentId: context.holding.instrument_id,
        amount,
        currency,
        scale: scaleNumber,
        amountBasis,
        method,
        preparedBy: context.principal.principalId,
        at,
      }
      const result = parsePortfolioMeasurementResult(
        await transport.fairValueControl(
          {
            action: "measure",
            measurement: {
              accountId: expected.accountId,
              instrumentId: expected.instrumentId,
              amount: expected.amount,
              currency: expected.currency,
              scale: expected.scale,
              amountBasis: expected.amountBasis,
              measurementAt: at,
              preparedAt: at,
              preparedBy: expected.preparedBy,
              method: expected.method,
              producerSelections: [{ producer: "portfolio", significance }],
            },
          },
          true,
        ),
        expected,
      )
      try {
        verifyPortfolioMeasurementEvidence(
          await transport.query({
            query: "fairValueEvidence",
            measurementId: result.measurement.measurementId,
          }),
          {
            measurementId: result.measurement.measurementId,
            evidenceHash: result.measurement.evidenceHash,
            accountId: context.account.accountId,
            instrumentId: context.holding.instrument_id,
            revisionId: context.account.currentRevision.revisionId,
            quantity: context.holding.quantity,
            portfolioAmount: context.holding.market_value.amount,
            portfolioCurrency: context.holding.market_value.currency,
            significance,
          },
        )
      } catch {
        throw new RetainedMeasurementVerificationError(
          "The measurement was saved, but its supporting inputs could not be confirmed for display. Refresh before relying on it; diagnostic details are available in Logs.",
        )
      }
      return result
    },
    onSuccess: async (result) => {
      setAnnouncement(
        `Measurement saved and classified ${humanize(result.classification.hierarchy)}.`,
      )
      await onCreated(result.measurement.measurementId)
    },
    onError: (error) => setAnnouncement(measurementErrorMessage(error)),
  })

  if (!available) {
    return (
      <Alert className="mb-4">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Portfolio-backed measurements are unavailable</AlertTitle>
        <AlertDescription>
          This installation cannot create a portfolio-backed measurement right now. Existing saved
          measurements remain readable; update or restore Fair Value before creating another.
        </AlertDescription>
      </Alert>
    )
  }

  return (
    <section
      aria-labelledby="portfolio-measurement-heading"
      className="mb-5 rounded-xl border border-primary/25 bg-primary/[0.035] p-5"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Guided measurement
          </p>
          <h2 id="portfolio-measurement-heading" className="mt-2 text-lg font-semibold">
            Create fair value from a current portfolio holding
          </h2>
          <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
            Choose what you own, state the valuation amount and technique, and let Market Squawk
            use the current portfolio position as supporting information. The resulting valuation
            is separate from live quotes and does not authorize a trade.
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={() => {
            void accounts.refetch()
            void holdings.refetch()
            void principals.refetch()
          }}
          disabled={
            measure.isPending ||
            accounts.isFetching ||
            holdings.isFetching ||
            principals.isFetching
          }
        >
          <RefreshCw
            className={
              accounts.isFetching || holdings.isFetching || principals.isFetching
                ? "animate-spin"
                : ""
            }
            aria-hidden="true"
          />
          Refresh inputs
        </Button>
      </div>

      {duplicateAccount || duplicatePrincipal ? (
        <WorkflowError message="The account or reviewer list contains duplicate entries. Refresh before creating a measurement." />
      ) : accounts.isError || holdings.isError || principals.isError ? (
        <WorkflowError
          message="Portfolio or reviewer details could not be loaded. Refresh, or open Logs for diagnostic details."
        />
      ) : accounts.isSuccess && accountSelections.length === 0 ? (
        <WorkflowNotice
          title="Import a portfolio first"
          message="A portfolio-backed measurement needs one current account and holding. Open Portfolio, import and reconcile your records, then return here."
        />
      ) : principals.isSuccess && principalSelections.length === 0 ? (
        <WorkflowNotice
          title="Set up a governance principal"
          message="An authorized reviewer is required to prepare a fair-value measurement. Complete governance setup, then refresh this workflow."
        />
      ) : selectedAccount && holdings.isSuccess && holdings.data.holdings.length === 0 ? (
        <WorkflowNotice
          title="This account has no current holdings"
          message="Choose another account or import a current holding before creating a portfolio-backed measurement."
        />
      ) : null}

      <form
        className="mt-5 space-y-5"
        onSubmit={(event) => {
          event.preventDefault()
          measure.mutate()
        }}
      >
        <fieldset disabled={measure.isPending} className="space-y-5">
          <legend className="sr-only">Portfolio-backed fair-value measurement</legend>
          <div className="grid gap-4 lg:grid-cols-2">
            <MeasurementField
              label="1. Portfolio account"
              htmlFor="fair-value-portfolio-account"
              detail="Uses the latest saved account position when you submit."
            >
              <select
                id="fair-value-portfolio-account"
                aria-describedby="fair-value-portfolio-account-help"
                value={selectedAccountId}
                onChange={(event) => {
                  setSelectedAccountId(event.target.value)
                  setSelectedInstrumentId("")
                  measure.reset()
                }}
                disabled={accounts.isPending || accountSelections.length === 0}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="">
                  {accounts.isPending ? "Loading portfolio accounts…" : "Select an account"}
                </option>
                {accountSelections.map(({ account }) => (
                  <option key={account.accountId} value={account.accountId}>
                    {shortIdentity(account.accountId)} · {account.currency} · {account.holdingCount} holdings
                  </option>
                ))}
              </select>
              {accounts.hasNextPage ? (
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="mt-2"
                  onClick={() => void accounts.fetchNextPage()}
                  disabled={accounts.isFetchingNextPage}
                >
                  {accounts.isFetchingNextPage ? "Loading…" : "Load more accounts"}
                </Button>
              ) : null}
            </MeasurementField>
            <MeasurementField
              label="2. Holding"
              htmlFor="fair-value-portfolio-holding"
              detail="The holding supports the valuation; it is not treated as a live trading quote."
            >
              <select
                id="fair-value-portfolio-holding"
                aria-describedby="fair-value-portfolio-holding-help"
                value={selectedInstrumentId}
                onChange={(event) => {
                  setSelectedInstrumentId(event.target.value)
                  measure.reset()
                }}
                disabled={holdings.isPending || !selectedAccount || !holdings.data?.holdings.length}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="">
                  {holdings.isPending ? "Loading current holdings…" : "Select a holding"}
                </option>
                {(holdings.data?.holdings ?? []).map((holding) => (
                  <option key={holding.instrument_id} value={holding.instrument_id}>
                    {shortIdentity(holding.instrument_id)} · {formatMoney(holding.market_value)}
                  </option>
                ))}
              </select>
            </MeasurementField>
          </div>

          {selectedAccount && selectedHolding ? (
            <PortfolioEvidenceSummary
              account={selectedAccount.account}
              holding={selectedHolding}
              bounded={
                holdings.data !== undefined &&
                holdings.data.availableItems > holdings.data.returnedItems
              }
            />
          ) : null}

          <div className="grid gap-4 lg:grid-cols-4">
            <MeasurementField
              label="3. Valuation amount"
              htmlFor="fair-value-amount"
              detail="Enter the amount without commas or a currency symbol."
              error={amountIssue}
            >
              <Input
                id="fair-value-amount"
                aria-describedby="fair-value-amount-help"
                inputMode="decimal"
                value={amount}
                maxLength={96}
                aria-invalid={amountIssue !== null}
                onChange={(event) => {
                  setAmount(event.target.value)
                  measure.reset()
                }}
              />
            </MeasurementField>
            <MeasurementField
              label="Amount basis"
              htmlFor="fair-value-amount-basis"
              detail="Per-unit values can support investment recommendations; entity and position totals remain explicitly separate."
            >
              <select
                id="fair-value-amount-basis"
                aria-describedby="fair-value-amount-basis-help"
                value={amountBasis}
                onChange={(event) => {
                  setAmountBasis(event.target.value as PortfolioMeasurementAmountBasis)
                  measure.reset()
                }}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="per_instrument_unit">Per instrument unit</option>
                <option value="reporting_entity_total">Entire reporting entity</option>
                <option value="position_total">Entire portfolio position</option>
              </select>
            </MeasurementField>
            <MeasurementField
              label="Currency"
              htmlFor="fair-value-currency"
              detail="Three-letter reporting currency, for example USD."
              error={currencyIssue}
            >
              <Input
                id="fair-value-currency"
                aria-describedby="fair-value-currency-help"
                value={currency}
                maxLength={3}
                aria-invalid={currencyIssue !== null}
                onChange={(event) => {
                  setCurrency(event.target.value.toUpperCase())
                  measure.reset()
                }}
              />
            </MeasurementField>
            <MeasurementField
              label="Declared scale"
              htmlFor="fair-value-scale"
              detail="Number of decimal places, from 0 through 28."
              error={amountIssue?.startsWith("Scale") ? amountIssue : null}
            >
              <Input
                id="fair-value-scale"
                aria-describedby="fair-value-scale-help"
                type="number"
                min={0}
                max={28}
                step={1}
                value={scale}
                onChange={(event) => {
                  setScale(event.target.value)
                  measure.reset()
                }}
              />
            </MeasurementField>
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <MeasurementField
              label="4. Valuation method"
              htmlFor="fair-value-method"
              detail={
                PORTFOLIO_MEASUREMENT_METHODS.find((option) => option.value === method)
                  ?.explanation ?? ""
              }
            >
              <select
                id="fair-value-method"
                aria-describedby="fair-value-method-help"
                value={method}
                onChange={(event) => {
                  setMethod(event.target.value as PortfolioMeasurementMethod)
                  measure.reset()
                }}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                {PORTFOLIO_MEASUREMENT_METHODS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.name}
                  </option>
                ))}
              </select>
            </MeasurementField>
            <MeasurementField
              label="Evidence significance"
              htmlFor="fair-value-significance"
              detail={
                significance === "significant"
                  ? "This holding materially affects the measurement as a whole."
                  : "This holding does not materially affect the measurement as a whole. With one input, review this choice carefully."
              }
            >
              <select
                id="fair-value-significance"
                aria-describedby="fair-value-significance-help"
                value={significance}
                onChange={(event) => {
                  setSignificance(event.target.value as PortfolioMeasurementSignificance)
                  measure.reset()
                }}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="significant">Significant to the full measurement</option>
                <option value="not_significant">Not significant to the full measurement</option>
              </select>
            </MeasurementField>
          </div>

          <MeasurementField
            label="5. Prepared by"
            htmlFor="fair-value-principal"
            detail="Choose an authorized reviewer to record with this measurement."
          >
            <select
              id="fair-value-principal"
              aria-describedby="fair-value-principal-help"
              value={principalId}
              onChange={(event) => {
                setPrincipalId(event.target.value)
                measure.reset()
              }}
              disabled={principals.isPending || principalSelections.length === 0}
              className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">
                {principals.isPending ? "Loading admitted principals…" : "Select a principal"}
              </option>
              {principalSelections.map(({ principal }) => (
                <option key={principal.principalId} value={principal.principalId}>
                  {principal.displayName} · {principal.roles.map(humanize).join(", ")}
                </option>
              ))}
            </select>
            {principals.hasNextPage ? (
              <Button
                type="button"
                size="sm"
                variant="ghost"
                className="mt-2"
                onClick={() => void principals.fetchNextPage()}
                disabled={principals.isFetchingNextPage}
              >
                {principals.isFetchingNextPage ? "Loading…" : "Load more principals"}
              </Button>
            ) : null}
          </MeasurementField>
        </fieldset>

        <div className="flex flex-wrap items-center gap-3 border-t border-primary/20 pt-4">
          <Button type="submit" disabled={!ready || measure.isPending}>
            {measure.isPending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <FileCheck2 aria-hidden="true" />
            )}
            {measure.isPending ? "Verifying and measuring…" : "Create measurement"}
          </Button>
          <p className="text-[10px] leading-4 text-muted-foreground">
            Market Squawk confirms the selected holding and reviewer before saving the measurement.
          </p>
        </div>
      </form>

      {measure.isError ? (
        <WorkflowError
          title={
            measure.error instanceof RetainedMeasurementVerificationError
              ? "Measurement outcome needs verification"
              : undefined
          }
          message={measurementErrorMessage(measure.error)}
        />
      ) : null}
      {measure.data ? <MeasurementSuccess result={measure.data} /> : null}
      <p className="sr-only" aria-live="polite">
        {announcement}
      </p>
    </section>
  )
}

function decimalPlaces(value: string) {
  return value.split(".")[1]?.length ?? 0
}

function measurementErrorMessage(error: unknown) {
  return error instanceof RetainedMeasurementVerificationError
    ? error.message
    : "The measurement could not be created. Review the fields and try again, or open Logs for diagnostic details."
}

function hasDuplicate(values: string[]) {
  return new Set(values).size !== values.length
}
