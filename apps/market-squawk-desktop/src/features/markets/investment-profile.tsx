import { useQuery } from "@tanstack/react-query"

import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { groupDecimal } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import { parseInvestmentProfileResult, type InvestmentProfileResult, type InvestmentReferenceProfile } from "./investment-profile-schema"
import { sourceInstantUnixNanos } from "./market-history"

export function InvestmentProfile({ selectionToken, bootstrap, transport }: {
  selectionToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const profile = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "research", "Research.GetInvestmentProfile", { selectionToken }),
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const result = parseInvestmentProfileResult(await transport.query({ query: "investmentProfile", selectionToken }, { signal }), selectionToken)
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      return result
    },
  })
  const result = profile.data

  return <section className="rounded-xl border border-border bg-card/30 p-4" aria-label="Investment profile">
    <div className="flex items-center justify-between gap-3">
      <h2 className="text-base font-semibold">Profile</h2>
    </div>
    <div className="mt-1 min-h-5 text-xs leading-5">
    {profile.isError ? <div className="flex items-start justify-between gap-2">
      <p role="alert" className="text-destructive">{result
        ? "The profile could not be refreshed. Showing the last checked information."
        : "The profile could not be loaded. Try again, or search Markets to choose a fresh selection."}</p>
      <Button variant="outline" size="sm" disabled={profile.isFetching} onClick={() => void profile.refetch()}>Retry</Button>
    </div> : profile.isFetching ? <p role="status" className="text-muted-foreground">{result ? "Updating profile information…" : "Loading profile information…"}</p> : null}
    </div>
    <div className="min-h-[180px]">
    {result ? <>
      {result.state === "available" ? <ReferenceProfile profile={result.profile} />
        : <p role="status" className="mt-3 text-sm text-muted-foreground">{profileAvailability(result)}</p>}
      <p className="mt-3 text-xs text-muted-foreground">{profile.isError ? "Last checked information through" : "Information through"} <ProfileTime value={result.knowledgeAt} /></p>
    </> : null}
    </div>
  </section>
}

function ReferenceProfile({ profile }: { profile: InvestmentReferenceProfile }) {
  const assetLabels: Record<InvestmentReferenceProfile["assetClass"], string> = {
    equity: "Stock", fixed_income: "Fixed income", option: "Option", future: "Futures contract",
    foreign_exchange: "Currency", crypto: "Crypto", commodity: "Commodity", fund: "Fund", index: "Index", cash: "Cash",
  }
  return <>
    <dl className="grid grid-cols-2 gap-x-3 gap-y-3">
      {([
        ["Name", profile.displayName], ["Symbol", profile.symbol],
        ["Investment type", profile.exchangeTradedFund ? "Exchange-traded fund" : assetLabels[profile.assetClass]],
        ["Currency", profile.currency], ["Listing venue", profile.listingVenue],
        ["Standard trading lot", groupDecimal(String(profile.roundLotSize))],
      ] as const).map(([label, value]) => <div key={label} className={label === "Name" ? "col-span-2 min-w-0" : "min-w-0"}>
        <dt className="text-xs text-muted-foreground">{label}</dt>
        <dd className="mt-1 break-words text-sm leading-5">{value}</dd>
      </div>)}
    </dl>
    <details className="mt-3 border-t border-border pt-3">
      <summary className="cursor-pointer text-sm focus-visible:outline-ring">Record dates</summary>
      <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
        {([
          ["Applies from", profile.effectiveFrom], ["Applies until", profile.effectiveUntil],
          ["Known at", profile.knownAt], ["Record updated", profile.referenceUpdatedAt],
        ] as const).map(([label, value]) => <div key={label}>
          <dt className="text-muted-foreground">{label}</dt>
          <dd className="mt-1">{value === null ? "End date not reported" : <ProfileTime value={value} />}</dd>
        </div>)}
      </dl>
      <p className="mt-3 text-xs text-muted-foreground">This record does not confirm delisting or a replacement investment.</p>
    </details>
  </>
}

function ProfileTime({ value }: { value: string }) {
  return <time className="font-mono" dateTime={value} title={value}>{formatTimestamp(sourceInstantUnixNanos(value))}</time>
}

function profileAvailability(result: Exclude<InvestmentProfileResult, { state: "available" }>): string {
  if (result.state === "ambiguous") return "More than one reference record matches this investment. A single profile cannot be established."
  if (result.state === "missing") {
    switch (result.reason) {
      case "canonical_definition": return "A saved identity record is missing for this investment. Search Markets to choose a fresh selection."
      case "official_directory": return "A listing record is not available for this investment yet."
      case "official_membership": return "No listing record matches this investment at the information date."
    }
  }
  return result.reason === "reference_not_configured"
    ? "Profile information is unavailable. Review your connections in Settings."
    : "Profile information could not be checked right now. Use the refresh icon to try again."
}
