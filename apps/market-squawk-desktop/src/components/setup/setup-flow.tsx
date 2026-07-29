import * as React from "react"
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Circle,
  Database,
  FileCheck2,
  KeyRound,
  Network,
  ShieldCheck,
  Sparkles,
  WalletCards,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { ProviderStep } from "@/components/setup/provider-step"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

const steps = [
  { label: "System", icon: FileCheck2 },
  { label: "Storage", icon: Database },
  { label: "Sources", icon: KeyRound },
  { label: "Research", icon: Sparkles },
  { label: "Portfolio", icon: WalletCards },
  { label: "Paper", icon: ShieldCheck },
  { label: "MCP", icon: Network },
  { label: "Review", icon: Check },
]

export function SetupFlow({
  bootstrap,
  transport,
  onClose,
  onRefresh,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onClose: () => void
  onRefresh: () => void
}) {
  const [step, setStep] = React.useState(0)
  const current = steps[step]
  if (!current) {
    return null
  }

  return (
    <section className="rounded-xl border border-border bg-card/55 p-5">
      <div className="mb-5 flex items-center gap-3">
        <Button type="button" variant="ghost" size="icon-sm" onClick={onClose}>
          <ArrowLeft aria-hidden="true" />
          <span className="sr-only">Return to setup overview</span>
        </Button>
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-3 text-xs">
            <span className="font-mono uppercase tracking-wider text-muted-foreground">
              Step {step + 1} of {steps.length}
            </span>
            <span className="font-medium">{current.label}</span>
          </div>
          <Progress
            value={((step + 1) / steps.length) * 100}
            className="mt-2 h-1"
          />
        </div>
      </div>

      <ol className="mb-6 grid grid-cols-4 gap-2 lg:grid-cols-8" aria-label="Setup steps">
        {steps.map((item, index) => (
          <li
            key={item.label}
            className={
              index === step
                ? "flex items-center gap-1.5 text-[10px] font-medium text-foreground"
                : index < step
                  ? "flex items-center gap-1.5 text-[10px] text-emerald-400"
                  : "flex items-center gap-1.5 text-[10px] text-muted-foreground"
            }
          >
            {index < step ? (
              <Check className="size-3" aria-hidden="true" />
            ) : index === step ? (
              <item.icon className="size-3" aria-hidden="true" />
            ) : (
              <Circle className="size-3" aria-hidden="true" />
            )}
            <span>{item.label}</span>
          </li>
        ))}
      </ol>

      <div className="min-h-52">
        {step === 0 ? (
          <PlainStep
            title="Confirm this local application"
            description={`${bootstrap.storage.detail} The release status is ${bootstrap.installation.label.toLowerCase()}; Market Squawk will not treat it as signed until installation evidence is admitted.`}
          />
        ) : null}
        {step === 1 ? (
          <PlainStep
            title="Keep data on this computer"
            description={`Market Squawk will use ${bootstrap.dataRoot}. Provider credentials remain in the operating-system credential service or the explicitly unlocked encrypted fallback.`}
          />
        ) : null}
        {step === 2 ? (
          <ProviderStep
            profiles={bootstrap.providerProfiles}
            sessions={bootstrap.providerSessions}
            transport={transport}
            onChanged={onRefresh}
          />
        ) : null}
        {step === 3 ? (
          <PlainStep
            title="Prepare research and modeling"
            description={bootstrap.modelRuntime.detail}
          />
        ) : null}
        {step === 4 ? (
          <PlainStep
            title="Create a portfolio workspace"
            description="Import holdings and transactions when you are ready. Market Squawk preserves the source records and reconciles calculated totals before reporting performance."
          />
        ) : null}
        {step === 5 ? (
          <PlainStep
            title="Keep execution in paper mode"
            description="Paper execution uses the central risk checks, realistic fees, latency, slippage, partial fills, balances, and positions. It cannot place a live order."
          />
        ) : null}
        {step === 6 ? (
          <PlainStep
            title="Make local tools available"
            description={bootstrap.mcp.detail}
          />
        ) : null}
        {step === 7 ? (
          <PlainStep
            title="Review before activation"
            description="Review every source and safety choice. Market Squawk activates only the capabilities whose owning Rust authority reports complete."
          />
        ) : null}
      </div>

      <div className="mt-6 flex items-center justify-between border-t border-border pt-4">
        <Button
          type="button"
          variant="ghost"
          onClick={() => setStep((value) => Math.max(0, value - 1))}
          disabled={step === 0}
        >
          <ArrowLeft aria-hidden="true" />
          Back
        </Button>
        <Button
          type="button"
          onClick={() =>
            step === steps.length - 1
              ? onClose()
              : setStep((value) => Math.min(steps.length - 1, value + 1))
          }
        >
          {step === steps.length - 1 ? "Finish review" : "Continue"}
          {step === steps.length - 1 ? (
            <Check aria-hidden="true" />
          ) : (
            <ArrowRight aria-hidden="true" />
          )}
        </Button>
      </div>
    </section>
  )
}

function PlainStep({
  title,
  description,
}: {
  title: string
  description: string
}) {
  return (
    <div className="mx-auto max-w-2xl py-8 text-center">
      <h2 className="text-xl font-semibold">{title}</h2>
      <p className="mt-3 text-sm leading-6 text-muted-foreground">{description}</p>
    </div>
  )
}

