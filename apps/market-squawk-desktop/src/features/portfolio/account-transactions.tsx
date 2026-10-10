import { RefreshButton } from "@/components/ui/refresh-button"
import * as React from "react"
import { CircleAlert } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, groupDecimal } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import type { PortfolioAccountSummary, PortfolioTransaction } from "./portfolio-contracts"
import { usePortfolioTransactions } from "./use-portfolio"

type TransactionsProps = {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}

// The demand panel unmounts this read on close; account/session changes also
// replace its pinned cursor and release the previous request and page.
export function AccountTransactions(props: TransactionsProps) {
  const [generation, setGeneration] = React.useState(0)
  return <TransactionsRead key={`${props.bootstrap.productSessionToken}:${props.account.accountToken}:${generation}`}
    {...props} refresh={() => setGeneration((value) => value + 1)} />
}

function TransactionsRead({ account, bootstrap, transport, refresh }: TransactionsProps & { refresh: () => void }) {
  const readSession = React.useId()
  const transactions = usePortfolioTransactions(transport, bootstrap, account.accountToken, readSession)
  const page = transactions.query.data

  if (!transactions.available) {
    return <TransactionsUnavailable title="Transaction history unavailable"
      detail="Recorded transactions cannot currently be opened for this portfolio." />
  }

  return (
    <section className="space-y-4" aria-label={`Transaction history for ${account.displayName}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
          Recorded transactions are shown one page at a time from the same saved portfolio
          observation. Refresh transactions to use the latest available observation.
        </p>
        <RefreshButton label="Refresh transactions" refreshing={transactions.query.isFetching} onClick={refresh} disabled={transactions.query.isFetching} />
      </div>
      {transactions.query.isPending ? (
        <Skeleton className="h-64 rounded-xl" aria-label={`Loading transactions for ${account.displayName}`} />
      ) : transactions.query.isError ? (
        <div>
          <TransactionsUnavailable title="Transactions could not be opened"
            detail="Try again to read the same saved observation, or refresh transactions to start again." />
          <Button className="mt-4" onClick={() => void transactions.query.refetch()} disabled={transactions.query.isFetching}>
            Try again
          </Button>
        </div>
      ) : page ? (
        <>
          <dl className="grid gap-3 text-xs sm:grid-cols-2">
            <div>
              <dt className="text-muted-foreground">Portfolio observation</dt>
              <dd className="mt-1">{formatUnixNanos(page.effectiveAtUnixNanos)}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Information available</dt>
              <dd className="mt-1">{page.availableAtUnixNanos === null
                ? "Not recorded" : formatUnixNanos(page.availableAtUnixNanos)}</dd>
            </div>
          </dl>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <caption className="sr-only">Recorded transactions on this page</caption>
              <thead className="border-b border-border text-muted-foreground">
                <tr>
                  <th scope="col" className="px-3 py-3 font-medium">Date</th>
                  <th scope="col" className="px-3 py-3 font-medium">Activity</th>
                  <th scope="col" className="px-3 py-3 font-medium">Investment</th>
                  <th scope="col" className="px-3 py-3 font-medium">Amount</th>
                  <th scope="col" className="px-3 py-3 font-medium">Quantity</th>
                  <th scope="col" className="px-3 py-3 font-medium">Lot method</th>
                </tr>
              </thead>
              <tbody>
                {page.transactions.map((transaction) => (
                  <tr key={transaction.transactionToken} className="border-b border-border/60">
                    <th scope="row" className="whitespace-nowrap px-3 py-3 font-medium">
                      {formatUnixNanos(transaction.occurredAtUnixNanos)}
                    </th>
                    <td className="px-3 py-3">{categoryLabel[transaction.category]}</td>
                    <td className="px-3 py-3">{investmentName(transaction)}</td>
                    <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(transaction.amount)}</td>
                    <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">
                      {transaction.quantity === null ? "Not recorded" : groupDecimal(transaction.quantity)}
                    </td>
                    <td className="px-3 py-3">{transaction.lotMethod ?? "Not recorded"}</td>
                  </tr>
                ))}
                {page.transactions.length === 0 ? (
                  <tr><td colSpan={6} className="px-3 py-5 text-muted-foreground">No recorded transactions are available on this page.</td></tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </>
      ) : null}
      <CursorNavigation navigation={transactions.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={transactions.query.isFetching} error={transactions.query.isError} onRestart={refresh} />
    </section>
  )
}

const categoryLabel: Record<PortfolioTransaction["category"], string> = {
  trade: "Trade",
  cash_transfer: "Cash transfer",
  income: "Income",
  fee: "Fee",
  corporate_action: "Corporate action",
}

function investmentName(transaction: PortfolioTransaction) {
  if (transaction.instrumentId === null) return "No investment associated"
  const investment = transaction.investment
  return `${investment?.name ?? "Investment name unavailable"}${investment?.symbol ? ` (${investment.symbol})` : ""}`
}

function TransactionsUnavailable({ title, detail }: { title: string; detail: string }) {
  return <Alert>
    <CircleAlert aria-hidden="true" />
    <AlertTitle>{title}</AlertTitle>
    <AlertDescription>{detail}</AlertDescription>
  </Alert>
}
