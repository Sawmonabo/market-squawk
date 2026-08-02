import type { DesktopBootstrap } from "@/lib/schemas"

export function VerificationPanel({
  bootstrap,
}: {
  bootstrap: DesktopBootstrap
}) {
  const verified = bootstrap.installation.state === "ready"
  const rows = [
    ["Market Squawk application", bootstrap.storage],
    ["Signed release", bootstrap.installation],
    ["Local model runtime", bootstrap.modelRuntime],
    ["Local MCP", bootstrap.mcp],
  ] as const

  return (
    <section className="rounded-xl border border-border bg-card/55">
      <header className="border-b border-border px-4 py-4">
        <h2 className="text-sm font-semibold">
          {verified ? "Installation verified" : "Installation status"}
        </h2>
        <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
          {bootstrap.installation.detail}
        </p>
      </header>
      <dl>
        {rows.map(([label, status]) => (
          <div
            key={label}
            className="flex min-h-11 items-center gap-3 border-b border-border/80 px-4 last:border-b-0"
          >
            <dt className="text-[11px] text-muted-foreground">{label}</dt>
            <dd className="ml-auto flex items-center gap-2 text-[10px] font-medium">
              <span
                className={
                  status.state === "ready"
                    ? "size-1.5 rounded-full bg-[var(--success)]"
                    : status.state === "unverified"
                      ? "size-1.5 rounded-full bg-[var(--warning)]"
                      : "size-1.5 rounded-full bg-muted-foreground/60"
                }
                aria-hidden="true"
              />
              {status.label}
            </dd>
          </div>
        ))}
      </dl>
    </section>
  )
}
