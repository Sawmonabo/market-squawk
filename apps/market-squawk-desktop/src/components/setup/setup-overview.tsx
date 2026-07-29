import * as React from "react"
import {
  ArrowRight,
  Check,
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

  const stepFor = (id: DesktopBootstrap["setupSteps"][number]["id"]) =>
    bootstrap.setupSteps.find((step) => step.id === id)
  const capabilities = [
    {
      title: "Local storage",
      step: stepFor("storage"),
      icon: Database,
    },
    {
      title: "Free data sources",
      step: stepFor("sources"),
      icon: FileChartColumn,
    },
    {
      title: "Research workspace",
      step: stepFor("research"),
      icon: Settings2,
    },
    {
      title: "Portfolio workspace",
      step: stepFor("portfolio"),
      icon: WalletCards,
    },
    {
      title: "Paper execution",
      step: stepFor("paper"),
      icon: ShieldCheck,
    },
    {
      title: "Local MCP",
      step: stepFor("mcp"),
      icon: Network,
    },
  ]
  const firstIncomplete = bootstrap.setupSteps.find((step) => !step.complete)

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
              {capability.step?.complete ? (
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
                {capability.step?.detail ?? "Status is unavailable."}
              </p>
            </div>
          </div>
        ))}
      </div>
      <div className="mt-4 flex flex-wrap items-center gap-3">
        <Button type="button" onClick={() => setOpen(true)}>
          {firstIncomplete
            ? `Continue with ${firstIncomplete.label.toLowerCase()}`
            : "Review completed setup"}
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
        <span className="ml-auto font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
          Saved authority resumes automatically
        </span>
      </div>
    </section>
  )
}
