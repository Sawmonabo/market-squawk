import { Activity, AlertTriangle, CheckCircle2, Clock3 } from "lucide-react"

import { formatTimestamp } from "@/lib/time"

import type { ModelActivity } from "./models-contracts"

export function ModelJobActivity({
  activities,
  available,
  loading,
  error,
}: {
  activities: ModelActivity[]
  available: boolean
  loading: boolean
  error: string | null
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-center gap-3">
        <Activity className="size-4 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Research activity</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Recent model and forecast preparation.
          </p>
        </div>
      </div>
      {!available ? (
        <Message text="Research activity is unavailable in this workspace." />
      ) : loading ? (
        <Message text="Loading research activity…" />
      ) : error ? (
        <Message text="Research activity is unavailable right now. Try refreshing the page." />
      ) : activities.length === 0 ? (
        <Message text="No recent model or forecast activity." />
      ) : (
        <ul className="mt-4 grid gap-2">
          {activities.map((activity) => {
            const Icon =
              activity.state === "failed"
                ? AlertTriangle
                : activity.state === "completed"
                  ? CheckCircle2
                  : Clock3
            return (
              <li
                key={activity.activityToken}
                className="rounded-lg border border-border bg-background/25 p-3"
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="flex min-w-0 items-start gap-2.5">
                    <Icon
                      className="mt-0.5 size-3.5 shrink-0 text-muted-foreground"
                      aria-hidden="true"
                    />
                    <div className="min-w-0">
                      <p className="truncate text-xs font-medium">
                        {activity.label}
                      </p>
                      <p className="mt-1 text-xs leading-5 text-muted-foreground">
                        {activity.statusMessage}
                      </p>
                    </div>
                  </div>
                  <span className="rounded-full border border-border px-2 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                    {activity.state === "running"
                      ? "In progress"
                      : activity.state.charAt(0).toUpperCase() +
                        activity.state.slice(1)}
                  </span>
                </div>
                {activity.progressPercent ? (
                  <p className="mt-2 text-[11px] text-muted-foreground">
                    {activity.progressPercent}% complete
                  </p>
                ) : null}
                <p className="mt-1 text-[10px] text-muted-foreground">
                  Updated {formatTimestamp(activity.updatedAtUnixNanos)}
                </p>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}

function Message({ text }: { text: string }) {
  return <p className="mt-4 text-sm leading-6 text-muted-foreground">{text}</p>
}
