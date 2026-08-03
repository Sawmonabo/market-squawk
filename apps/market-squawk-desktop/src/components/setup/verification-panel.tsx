import { CheckCircle2, CircleAlert, ShieldCheck } from "lucide-react"

import type { DesktopBootstrap } from "@/lib/schemas"

export function VerificationPanel({
  bootstrap,
}: {
  bootstrap: DesktopBootstrap
}) {
  const installedReady = bootstrap.installation.state === "ready"
  const workspaceReady = bootstrap.storage.state === "ready"
  const verified = installedReady && workspaceReady
  const rows = [
    {
      label: "Installed release",
      status: bootstrap.installation,
      owner: "Installer authority",
    },
    {
      label: "Active workspace storage",
      status: bootstrap.storage,
      owner: "Workspace authority",
    },
    {
      label: "Local model runtime",
      status: bootstrap.modelRuntime,
      owner: "Model runtime authority",
    },
    {
      label: "Shared local MCP",
      status: bootstrap.mcp,
      owner: "MCP service authority",
    },
  ] as const

  return (
    <section className="rounded-xl border border-border bg-card/55">
      <header className="border-b border-border px-4 py-4">
        <div className="flex items-start gap-3">
          {verified ? (
            <ShieldCheck
              className="mt-0.5 size-4 shrink-0 text-emerald-400"
              aria-hidden="true"
            />
          ) : (
            <CircleAlert
              className="mt-0.5 size-4 shrink-0 text-amber-300"
              aria-hidden="true"
            />
          )}
          <div>
            <h2 className="text-sm font-semibold">
              {verified ? "Installation and workspace verified" : "Verification needs attention"}
            </h2>
            <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
              {verified
                ? "The installed release and active workspace are ready. Optional capabilities retain their own evidence below."
                : "Release or workspace evidence is not ready. Setup never treats optional availability or an accepted plan as verification."}
            </p>
          </div>
        </div>
      </header>
      <dl>
        {rows.map(({ label, status, owner }) => (
          <div
            key={label}
            className="flex min-h-14 items-center gap-3 border-b border-border/80 px-4 last:border-b-0"
          >
            <span className="flex size-5 shrink-0 items-center justify-center">
              {status.state === "ready" ? (
                <CheckCircle2 className="size-3.5 text-emerald-400" aria-hidden="true" />
              ) : status.state === "unverified" || status.state === "not_configured" ? (
                <CircleAlert className="size-3.5 text-amber-300" aria-hidden="true" />
              ) : (
                <span
                  className="size-1.5 rounded-full bg-muted-foreground/60"
                  aria-hidden="true"
                />
              )}
            </span>
            <div className="min-w-0">
              <dt className="text-[11px] text-foreground/90">{label}</dt>
              <dd className="mt-0.5 font-mono text-[8px] uppercase tracking-wider text-muted-foreground">
                {owner}
              </dd>
            </div>
            <div className="ml-auto max-w-32 text-right">
              <p className="text-[10px] font-medium">{status.label}</p>
              <p className="mt-0.5 line-clamp-2 text-[9px] leading-relaxed text-muted-foreground">
                {status.detail}
              </p>
            </div>
          </div>
        ))}
      </dl>
    </section>
  )
}
