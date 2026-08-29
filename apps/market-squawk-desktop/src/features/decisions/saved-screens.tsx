import { Filter, ListOrdered } from "lucide-react"

import { humanize } from "@/lib/formatters"

import type { SavedScreenView } from "./contracts"
import { StateLabel } from "./decision-boundaries"

export function SavedScreens({ screens }: { screens: SavedScreenView[] }) {
  return (
    <section aria-labelledby="saved-screens-heading" className="mt-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Reusable research screens
          </p>
          <h2 id="saved-screens-heading" className="mt-1 text-lg font-semibold">
            Saved screens
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Saved definitions use only information available by each chosen research cutoff.
          </p>
        </div>
        <StateLabel value={`${screens.length} loaded`} />
      </div>

      {screens.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-border p-6 text-sm text-muted-foreground">
          No saved screen revisions are present in this workspace.
        </div>
      ) : (
        <div className="mt-4 grid gap-3 xl:grid-cols-2">
          {screens.map((screen) => (
            <article
              key={`${screen.id}:${screen.revision}`}
              className="rounded-xl border border-border bg-card/45 p-4"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <h3 className="truncate text-sm font-semibold" title={screen.id}>
                    {screen.id}
                  </h3>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Revision {screen.revision} · up to {screen.maximumResults} candidates
                  </p>
                </div>
                <StateLabel value={screen.asOfSemantics} />
              </div>

              <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                <ScreenFact
                  icon={Filter}
                  label="Predicates"
                  value={`${screen.predicates.length}`}
                />
                <ScreenFact
                  icon={ListOrdered}
                  label="Ranking"
                  value={`${humanize(screen.ranking.binding.name)} · ${humanize(screen.ranking.direction)}`}
                />
              </dl>

              {screen.predicates.length > 0 && (
                <ul className="mt-4 grid gap-2" aria-label="Screen predicates">
                  {screen.predicates.map((predicate, index) => (
                    <li
                      key={`${predicate.binding.name}:${predicate.binding.version}:${index}`}
                      className="rounded-lg border border-border/70 bg-background/45 px-3 py-2 text-xs"
                    >
                      <span className="font-medium">
                        {humanize(predicate.binding.name)} v{predicate.binding.version}
                      </span>{" "}
                      <span className="text-muted-foreground">
                        {humanize(predicate.operator)} {predicate.threshold} · nulls {predicate.nullPolicy}
                      </span>
                    </li>
                  ))}
                </ul>
              )}

              <div className="mt-4 grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                <p>Coverage floor: {screen.constraints.minimumCoverage}</p>
                <p>Liquidity floor: {screen.constraints.minimumLiquidity}</p>
                <p className="sm:col-span-2">
                  Admitted quality: {screen.constraints.admittedDataQualities.map(humanize).join(", ")}
                </p>
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  )
}

function ScreenFact({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Filter
  label: string
  value: string
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-background/45 p-3">
      <Icon className="size-3.5 text-primary" aria-hidden="true" />
      <dt className="mt-2 text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 text-xs font-medium">{value}</dd>
    </div>
  )
}
