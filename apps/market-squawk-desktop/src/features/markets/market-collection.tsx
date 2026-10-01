import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { Activity, CircleAlert } from "lucide-react"
import { Link } from "react-router-dom"
import { z } from "zod"

import { productKeys, type ProductScope } from "@/app/query-client"
import { useSystem } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney } from "@/lib/formatters"
import type { ApplicationResult } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { marketProductRowSchema, type MarketProductRow } from "./market-product"

const revisionSchema = z.string().max(20).regex(/^[1-9][0-9]*$/)
  .pipe(z.string().refine((value) => BigInt(value) <= 18_446_744_073_709_551_615n))
const choiceSchema = z.object({ symbol: z.string().min(1).max(64), kept: z.boolean() }).strict()
const entrySchema = choiceSchema.extend({ market: marketProductRowSchema.nullable() })
  .refine((entry) => entry.market === null || entry.market.identity.symbol === entry.symbol, {
    message: "Market details must match the collection symbol.",
  })
const collectionSchema = z.object({ revision: revisionSchema, entries: z.array(entrySchema) }).strict()
  .refine((value) => new Set(value.entries.map((entry) => entry.symbol)).size === value.entries.length, {
    message: "Collection symbols must be unique.",
  })
const choicesSchema = z.object({ revision: revisionSchema, choices: z.array(choiceSchema) }).strict()
  .refine((value) => new Set(value.choices.map((choice) => choice.symbol)).size === value.choices.length, {
    message: "Collection symbols must be unique.",
  })
const collectionInput = { query: "marketCollection" } as const
const marketInformationInput = { ...collectionInput, includeMarket: true } as const

export function parseMarketCollectionResult(result: ApplicationResult) {
  return collectionSchema.parse(result.data)
}

export function useMarketCollection(transport: ProductTransport, scope: ProductScope) {
  const queryClient = useQueryClient()
  const collection = useQuery({
    queryKey: productKeys.operation(scope, "market", "Market.GetCollection", collectionInput),
    queryFn: async ({ signal }) => parseMarketCollectionResult(await transport.query(collectionInput, { signal })),
  })
  const marketInformation = useQuery({
    queryKey: productKeys.operation(scope, "market", "Market.GetCollection", marketInformationInput),
    enabled: collection.data !== undefined,
    queryFn: async ({ signal }) => parseMarketCollectionResult(await transport.query(marketInformationInput, { signal })),
  })
  const refresh = () => queryClient.invalidateQueries({
    queryKey: productKeys.domain(scope, "market"),
    predicate: (query) => query.queryKey[4] === "Market.GetCollection" || query.queryKey[4] === "Market.GetOverview",
  })
  const choice = useMutation({
    mutationKey: productKeys.operation(scope, "market", "Market.SetCollectionChoice", {}),
    mutationFn: async (input: { symbol: string; kept: boolean }) => {
      const snapshot = collection.data
      if (!snapshot || collection.isError || collection.isFetching) {
        throw new Error("Reload your collection before changing it.")
      }
      const result = choicesSchema.parse((await transport.query({
        query: "marketSetCollectionChoice", expectedRevision: snapshot.revision,
        symbol: input.symbol, kept: input.kept, confirmed: true,
      })).data)
      if (!result.choices.some((saved) => saved.symbol === input.symbol && saved.kept === input.kept)) {
        throw new Error("Your collection choice could not be verified.")
      }
      return result
    },
    // Saving is complete when the choice is durable; price refresh must not prolong it.
    onSuccess: () => { void refresh() },
    onError: () => { void refresh() },
  })
  return { collection, marketInformation, choice }
}

export function MarketCollection({
  state,
  onSelect,
  layout = "list",
}: {
  state: ReturnType<typeof useMarketCollection>
  onSelect?: (selectionToken: string) => void
  layout?: "list" | "grid"
}) {
  const { collection, marketInformation, choice } = state
  const { eventConnection } = useSystem()
  const savedCollection = collection.data
  const marketCollection = marketInformation.data ?? null
  const marketInformationMatches = savedCollection !== undefined && marketCollection !== null
    && savedCollection.revision === marketCollection.revision
    && savedCollection.entries.length === marketCollection.entries.length
    && savedCollection.entries.every((entry) => marketCollection.entries.some((marketEntry) =>
      marketEntry.symbol === entry.symbol && marketEntry.kept === entry.kept))
  const entries = savedCollection?.entries.map((entry) => ({
    ...entry,
    market: marketInformationMatches
      ? marketCollection?.entries.find((marketEntry) => marketEntry.symbol === entry.symbol)?.market ?? null
      : null,
  })) ?? []
  const kept = entries.filter((entry) => entry.kept)
  const removed = entries.filter((entry) => !entry.kept)
  const disconnected = eventConnection.status !== "connected"
  const busy = disconnected || choice.isPending || collection.isFetching || collection.isError
  const marketInformationUnverified = collection.isError || collection.isFetching
    || marketInformation.isError || marketInformation.isFetching || disconnected

  return <section className="rounded-xl border border-border bg-card/45 p-5" aria-label="Your market collection">
    <div className="flex items-start justify-between gap-3">
      <div>
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">Market context</p>
        <h2 className="mt-1 text-base font-semibold">Your market collection</h2>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">Keep the investments you want to see on Home and Markets. You can restore any removed investment below.</p>
      </div>
      <Activity className="size-5 shrink-0 text-primary" aria-hidden="true" />
    </div>
    {collection.isPending ? <Skeleton className="mt-5 h-40 rounded-lg" />
      : collection.isError && savedCollection === undefined ? <Alert className="mt-5">
        <CircleAlert aria-hidden="true" /><AlertTitle>Your market collection is unavailable</AlertTitle>
        <AlertDescription>Reload to check your saved choices.</AlertDescription>
      </Alert>
        : <>
          {collection.isError ? <Alert className="mt-5">
            <CircleAlert aria-hidden="true" /><AlertTitle>Saved collection could not be refreshed</AlertTitle>
            <AlertDescription>Showing your last saved choices. Refresh before changing your collection.</AlertDescription>
          </Alert> : null}
          {marketInformation.isError ? <Alert className="mt-5">
            <CircleAlert aria-hidden="true" /><AlertTitle>{marketInformationMatches ? "Market information could not be refreshed" : "Market information is unavailable"}</AlertTitle>
            <AlertDescription>{marketInformationMatches
              ? "Showing saved prices and investment details. Their freshness could not be checked. Refresh to try again."
              : "Your saved collection is available. Prices and investment details could not be checked. Refresh to try again."}</AlertDescription>
          </Alert> : marketCollection !== null && !marketInformationMatches ? <Alert className="mt-5">
            <CircleAlert aria-hidden="true" /><AlertTitle>Market information needs refreshing</AlertTitle>
            <AlertDescription>The market information does not match your latest saved collection. Refresh to check again.</AlertDescription>
          </Alert> : collection.isFetching || marketInformation.isFetching
            ? <p role="status" className="mt-4 text-xs text-muted-foreground">Checking your saved choices and market information…</p> : null}
          {kept.length === 0 ? <p className="mt-5 rounded-lg border border-dashed border-border p-5 text-xs text-muted-foreground">Your collection is empty. Keep an investment below to show it here again.</p>
            : <ul className={layout === "grid" ? "mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-3" : "mt-5 divide-y divide-border"}>
              {kept.map((entry) => <li key={entry.symbol} className={layout === "grid" ? "rounded-lg border border-border bg-background/35 p-3" : "py-3 first:pt-0 last:pb-0"}>
                <div className="flex items-center gap-3">
                  <div className="min-w-0 flex-1">
                    <CollectionInvestment symbol={entry.symbol} market={entry.market} unverified={marketInformationUnverified} onSelect={onSelect} />
                  </div>
                  <Button type="button" size="xs" variant="ghost" disabled={busy}
                    aria-label={`Remove ${entry.symbol} from your collection`}
                    onClick={() => choice.mutate({ symbol: entry.symbol, kept: false })}>Remove</Button>
                </div>
              </li>)}
            </ul>}
          {removed.length > 0 ? <details className="mt-4 rounded-lg border border-border bg-background/35 p-3">
            <summary className="cursor-pointer text-xs font-medium">Removed investments ({removed.length})</summary>
            <ul className="mt-3 space-y-3">
              {removed.map((entry) => <li key={entry.symbol} className="flex items-center justify-between gap-3">
                <span className="text-xs"><span className="font-medium">{entry.symbol}</span>{entry.market?.identity.name ? ` · ${entry.market.identity.name}` : ""}</span>
                <Button type="button" size="xs" variant="outline" disabled={busy}
                  aria-label={`Keep ${entry.symbol} in your collection`}
                  onClick={() => choice.mutate({ symbol: entry.symbol, kept: true })}>Keep</Button>
              </li>)}
            </ul>
          </details> : null}
        </>}
    {choice.isPending ? <p role="status" className="mt-3 text-xs text-muted-foreground">Saving your collection…</p> : null}
    {choice.isError ? <p role="alert" className="mt-3 text-xs text-destructive">Your choice could not be saved. Check the refreshed collection and try again.</p> : null}
    <div className="mt-4 flex flex-wrap gap-2">
      <Button type="button" size="sm" variant="outline" disabled={collection.isFetching || choice.isPending}
        onClick={() => {
          void collection.refetch()
          if (savedCollection !== undefined) void marketInformation.refetch()
        }}>Refresh collection</Button>
      <Button asChild size="sm" variant="outline"><Link to="/markets">Explore markets</Link></Button>
    </div>
  </section>
}

function CollectionInvestment({ symbol, market, unverified, onSelect }: {
  symbol: string
  market: MarketProductRow | null
  unverified: boolean
  onSelect?: (selectionToken: string) => void
}) {
  const label = <><span className="block text-xs font-medium">{symbol}</span>
    {market?.identity.name ? <span className="mt-0.5 block text-[10px] text-muted-foreground">{market.identity.name}</span> : null}</>
  return <>
    {market === null ? label
      : onSelect ? <button type="button" className="text-left underline-offset-4 hover:underline focus-visible:outline focus-visible:outline-ring" onClick={() => onSelect(market.selectionToken)}>{label}</button>
        : <Link className="block underline-offset-4 hover:underline" to={`/markets?selectionToken=${encodeURIComponent(market.selectionToken)}`}>{label}</Link>}
    {market === null ? <p className="mt-2 text-[10px] text-muted-foreground">Investment details are not available yet.</p>
      : <div className="mt-2 text-[10px] text-muted-foreground">
        <p className="font-mono text-foreground">{market.price ? formatMoney({ amount: market.price.value, currency: market.price.currency }) : "Price unavailable"}</p>
        <p>{unverified && market.price !== null ? "Saved price · Freshness not checked" : availabilityLabel(market)}{market.changePercent !== null ? ` · ${market.changePercent}%` : ""}</p>
        {market.asOf ? <time dateTime={market.asOf}>{new Date(market.asOf).toLocaleString()}</time> : null}
      </div>}
  </>
}

function availabilityLabel(market: MarketProductRow): string {
  switch (market.availability) {
    case "current": return "Current"
    case "delayed": return "Delayed"
    case "previous_close": return "Previous close"
    case "unavailable": return "Unavailable"
  }
}
