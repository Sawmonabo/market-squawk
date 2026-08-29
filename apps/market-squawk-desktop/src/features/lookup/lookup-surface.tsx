import * as React from "react"
import {
  ArrowRight,
  BriefcaseBusiness,
  Building2,
  ChartNoAxesCombined,
  Database,
  Gauge,
  Search,
  Settings2,
} from "lucide-react"
import { useNavigate } from "react-router-dom"

import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import type { ProductScope } from "@/app/query-client"
import type { ProductTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"

import {
  instrumentLookupDetailSchema,
  productLookupCategories,
  type ProductLookupCategory,
} from "./schemas"
import { useLookup, type ProductLookupMatch } from "./use-lookup"

const categoryLabels: Record<ProductLookupCategory, string> = {
  company: "Companies",
  dataset: "Research data",
  instrument: "Instruments",
  model: "Models",
  portfolio: "Portfolio",
  screen: "Saved screens",
  target: "Investment targets",
}

export function LookupSurface({
  transport,
  scope,
  autoFocus = false,
}: {
  transport: ProductTransport
  scope: ProductScope
  autoFocus?: boolean
}) {
  const navigate = useNavigate()
  const [text, setText] = React.useState("")
  const [categories, setCategories] = React.useState<ProductLookupCategory[]>([])
  const state = useLookup(transport, scope, text, categories)

  const toggle = (category: ProductLookupCategory) => {
    setCategories((current) =>
      current.includes(category)
        ? current.filter((value) => value !== category)
        : [...current, category],
    )
  }

  return (
    <div className="space-y-4">
      <div className="relative">
        <Search
          className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
          aria-hidden="true"
        />
        <Input
          autoFocus={autoFocus}
          value={text}
          maxLength={256}
          onChange={(event) => setText(event.target.value)}
          placeholder="Search a ticker, company, investment, or research item…"
          aria-label="Search your Market Squawk workspace"
          className="h-11 pl-10"
        />
      </div>

      <div className="flex flex-wrap gap-2" aria-label="Limit search categories">
        {productLookupCategories.map((category) => {
          const selected = categories.includes(category)
          return (
            <button
              key={category}
              type="button"
              aria-pressed={selected}
              onClick={() => toggle(category)}
              className={cn(
                "rounded-full border px-3 py-1 text-[10px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
                selected
                  ? "border-primary/60 bg-primary/15 text-primary"
                  : "border-border bg-card/40 text-muted-foreground hover:text-foreground",
              )}
            >
              {categoryLabels[category]}
            </button>
          )
        })}
      </div>

      <LookupResults
        state={state}
        query={text.trim()}
        onOpen={(match) => navigate(lookupRoute(match))}
      />
    </div>
  )
}

function LookupResults({
  state,
  query,
  onOpen,
}: {
  state: ReturnType<typeof useLookup>
  query: string
  onOpen: (match: ProductLookupMatch) => void
}) {
  if (state.status === "idle") {
    return (
      <div className="rounded-xl border border-dashed border-border bg-card/20 px-5 py-8 text-center">
        <Gauge className="mx-auto size-5 text-primary" aria-hidden="true" />
        <p className="mt-3 text-sm font-medium">Find something in your workspace</p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Enter at least two characters, such as a ticker, company, investment, research collection,
          or saved screen.
        </p>
      </div>
    )
  }
  if (state.status === "loading") {
    return (
      <div className="space-y-2" aria-label="Searching local workspace">
        <Skeleton className="h-16 rounded-lg" />
        <Skeleton className="h-16 rounded-lg" />
        <Skeleton className="h-16 rounded-lg" />
      </div>
    )
  }
  if (state.status === "unavailable") {
    return (
      <div role="alert" className="rounded-xl border border-destructive/40 bg-destructive/5 p-4">
        <p className="text-sm font-medium text-destructive">Search is unavailable</p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Try again. If the problem continues, review Logs &amp; Diagnostics.
        </p>
      </div>
    )
  }

  const data = state.data
  const grouped = groupMatches(data.matches)
  const unavailable = data.categories.filter(
    (category) => category.state === "unavailable",
  )
  if (data.matches.length === 0) {
    return (
      <div className="rounded-xl border border-border bg-card/25 p-5">
        <p className="text-sm font-medium">No matches for “{query}”</p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Try a ticker, company, investment name, research collection, model, or saved screen.
        </p>
        <UnavailableCategories categories={unavailable} />
      </div>
    )
  }

  return (
    <div className="space-y-4" aria-live="polite">
      {[...grouped.entries()].map(([category, matches]) => (
        <section key={category} aria-labelledby={`lookup-${category}`}>
          <div className="mb-2 flex items-center justify-between">
            <h3
              id={`lookup-${category}`}
              className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              {categoryLabels[category]}
            </h3>
            <span className="text-[10px] text-muted-foreground">{matches.length}</span>
          </div>
          <div className="grid gap-2">
            {matches.map((match) => (
              <button
                key={`${match.category}:${match.id}`}
                type="button"
                onClick={() => onOpen(match)}
                className="group flex w-full items-start gap-3 rounded-lg border border-border bg-card/40 p-3 text-left transition-colors hover:border-primary/40 hover:bg-card/70 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
              >
                <LookupIcon category={match.category} />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium">{friendlyLabel(match)}</span>
                  <span className="mt-1 block truncate font-mono text-[10px] text-muted-foreground">
                    {detailFor(match)}
                  </span>
                </span>
                <ArrowRight
                  className="mt-1 size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:text-primary"
                  aria-hidden="true"
                />
              </button>
            ))}
          </div>
        </section>
      ))}
      {data.truncated ? (
        <p className="text-[11px] text-muted-foreground">
          More matches exist. Narrow the words or select fewer categories.
        </p>
      ) : null}
      <UnavailableCategories categories={unavailable} />
    </div>
  )
}

function UnavailableCategories({
  categories,
}: {
  categories: Array<{ category: ProductLookupCategory; reason?: string }>
}) {
  if (categories.length === 0) return null
  return (
    <details className="mt-4 rounded-lg border border-border bg-background/40 px-3 py-2">
      <summary className="cursor-pointer text-[11px] font-medium text-muted-foreground">
        Some results are unavailable ({categories.length})
      </summary>
      <ul className="mt-2 space-y-2 text-[11px] leading-4 text-muted-foreground">
        {categories.map((category) => (
          <li key={category.category}>
            <strong className="text-foreground/75">{categoryLabels[category.category]}:</strong>{" "}
            Search is not available for this category right now.
          </li>
        ))}
      </ul>
    </details>
  )
}

function LookupIcon({ category }: { category: ProductLookupCategory }) {
  const Icon =
    category === "company"
      ? Building2
      : category === "dataset"
        ? Database
        : category === "instrument"
          ? ChartNoAxesCombined
          : category === "portfolio" || category === "target"
            ? BriefcaseBusiness
            : Settings2
  return (
    <span className="rounded-md border border-border bg-background/70 p-2">
      <Icon className="size-4 text-primary" aria-hidden="true" />
    </span>
  )
}

function friendlyLabel(match: ProductLookupMatch) {
  if (match.category === "instrument") {
    const parsed = instrumentLookupDetailSchema.safeParse(match.detail)
    if (parsed.success) return parsed.data.companyName ?? parsed.data.displayName
  }
  return match.label
}

function detailFor(match: ProductLookupMatch) {
  const detail = match.detail
  if (match.category === "company") {
    return isText(detail.sicDescription) ? detail.sicDescription : "Company"
  }
  if (match.category === "instrument") {
    const parsed = instrumentLookupDetailSchema.safeParse(detail)
    if (parsed.success) {
      const reason = parsed.data.matchReasons[0]
      const matched = reason
        ? `${reason.label}: ${reason.value}${reason.venueId ? ` · ${reason.venueId}` : ""}`
        : parsed.data.displayName
      return `${matched} · ${friendlyAssetClass(parsed.data.assetClass)} · ${parsed.data.quoteCurrency}`
    }
  }
  if (match.category === "dataset") {
    const rows = typeof detail.rowCount === "number" ? `${detail.rowCount.toLocaleString()} rows` : null
    return rows ?? "Research collection"
  }
  if (match.category === "screen") {
    return typeof detail.revision === "number" ? `Revision ${detail.revision}` : match.id
  }
  return categoryLabels[match.category]
}

export function lookupRoute(match: ProductLookupMatch) {
  if (match.destination?.kind === "market_instrument") {
    return `/markets?instrumentId=${encodeURIComponent(match.destination.instrumentId)}`
  }
  if (match.destination?.kind === "research_company") {
    return "/advanced/research-data"
  }
  if (match.category === "dataset") return "/advanced/research-data"
  if (match.category === "screen") return "/opportunities"
  if (match.category === "model") return "/advanced/models-forecasts"
  if (match.category === "portfolio") return "/portfolio"
  if (match.category === "target") return "/advanced/valuation-targets"
  return "/home"
}

function groupMatches(matches: ProductLookupMatch[]) {
  const grouped = new Map<ProductLookupCategory, ProductLookupMatch[]>()
  for (const match of matches) {
    const group = grouped.get(match.category)
    if (group) group.push(match)
    else grouped.set(match.category, [match])
  }
  return grouped
}

function isText(value: unknown): value is string {
  return typeof value === "string" && value.length > 0
}

function friendlyAssetClass(value: string) {
  const label = value.replaceAll("_", " ")
  return label.charAt(0).toUpperCase() + label.slice(1)
}
