import * as React from "react"
import {
  ArrowRight,
  Check,
  Clock3,
  Database,
  FileChartColumn,
  Network,
  Settings2,
  ShieldCheck,
  WalletCards,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { SetupFlow } from "@/components/setup/setup-flow"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

export function SetupOverview({
  bootstrap,
  transport,
  onRefresh,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onRefresh: () => void
}) {
  const [open, setOpen] = React.useState(false)
  if (open) {
    return (
      <SetupFlow
        bootstrap={bootstrap}
        transport={transport}
        onClose={() => setOpen(false)}
        onRefresh={onRefresh}
      />
    )
  }

  const capabilities = [
    {
      title: "Local storage",
      detail: "Use native private directories and controlled artifacts.",
      icon: Database,
      ready: bootstrap.storage.state === "ready",
    },
    {
      title: "Free data sources",
      detail: `${bootstrap.providerProfiles.length} supported provider setup paths available.`,
      icon: FileChartColumn,
      ready: bootstrap.providerSessions.some(
        (session) => session.next_action === "active",
      ),
    },
    {
      title: "Research and modeling",
      detail: bootstrap.modelRuntime.detail,
      icon: Settings2,
      ready: bootstrap.modelRuntime.state === "ready",
    },
    {
      title: "Portfolio workspace",
      detail: "Prepare private imports, reconciliation, and analytics.",
      icon: WalletCards,
      ready: false,
    },
    {
      title: "Paper execution",
      detail: "Start with central risk limits and no live-order authority.",
      icon: ShieldCheck,
      ready: bootstrap.paperModeEnabled,
    },
    {
      title: "Local MCP",
      detail: bootstrap.mcp.detail,
      icon: Network,
      ready: bootstrap.mcp.state === "ready",
    },
  ]

  return (
    <section className="rounded-xl border border-border bg-card/55 p-5">
      <h2 className="text-base font-semibold">Set up everything for me</h2>
      <p className="mt-2 max-w-3xl text-xs leading-relaxed text-muted-foreground">
        Apply the recommended private, zero-fee, and safety-first configuration.
        You can review every setting before Market Squawk makes it active.
      </p>
      <div className="mt-5 grid border-y border-border sm:grid-cols-2">
        {capabilities.map((capability) => (
          <div
            key={capability.title}
            className="flex gap-3 border-b border-border px-0 py-3 last:border-b-0 sm:odd:border-r sm:odd:pr-5 sm:even:pl-5 sm:[&:nth-last-child(-n+2)]:border-b-0"
          >
            <span className="mt-0.5 flex size-4 shrink-0 items-center justify-center rounded border border-border bg-background">
              {capability.ready ? (
                <Check className="size-2.5 text-emerald-400" aria-hidden="true" />
              ) : (
                <capability.icon
                  className="size-2.5 text-muted-foreground"
                  aria-hidden="true"
                />
              )}
            </span>
            <div>
              <h3 className="text-xs font-semibold">{capability.title}</h3>
              <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">
                {capability.detail}
              </p>
            </div>
          </div>
        ))}
      </div>
      <div className="mt-4 flex flex-wrap items-center gap-3">
        <Button type="button" onClick={() => setOpen(true)}>
          Continue with recommended setup
          <ArrowRight aria-hidden="true" />
        </Button>
        <Dialog>
          <DialogTrigger asChild>
            <Button type="button" variant="ghost" size="sm">
              Review advanced settings
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Advanced setup</DialogTitle>
              <DialogDescription>
                Change resource limits and installed model paths through the
                validated configuration file or environment contract. Provider
                authority and risk checks remain unchanged.
              </DialogDescription>
            </DialogHeader>
            <dl className="space-y-3 text-sm">
              <div>
                <dt className="text-xs text-muted-foreground">Data root</dt>
                <dd className="mt-1 break-all font-mono text-xs">
                  {bootstrap.dataRoot}
                </dd>
              </div>
              <div>
                <dt className="text-xs text-muted-foreground">Build</dt>
                <dd className="mt-1 text-xs">{bootstrap.buildProfile}</dd>
              </div>
            </dl>
          </DialogContent>
        </Dialog>
        <span className="ml-auto flex items-center gap-1.5 font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
          <Clock3 className="size-3" aria-hidden="true" />
          About 4 min
        </span>
      </div>
    </section>
  )
}

