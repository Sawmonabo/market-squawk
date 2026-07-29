import * as React from "react"
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Circle,
  CircleAlert,
  Database,
  FileCheck2,
  KeyRound,
  Network,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  type LucideIcon,
  WalletCards,
} from "lucide-react"

import { ProviderStep } from "@/components/setup/provider-step"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import type {
  DesktopBootstrap,
  SetupStep,
} from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

const stepIcons: Record<SetupStep["id"], LucideIcon> = {
  system: FileCheck2,
  storage: Database,
  sources: KeyRound,
  research: Sparkles,
  portfolio: WalletCards,
  paper: ShieldCheck,
  mcp: Network,
  review: Check,
}

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
  const steps = bootstrap.setupSteps
  const [stepIndex, setStepIndex] = React.useState(() => {
    const firstIncomplete = steps.findIndex((step) => !step.complete)
    return firstIncomplete < 0 ? steps.length - 1 : firstIncomplete
  })
  const current = steps[stepIndex]
  if (!current) {
    return null
  }
  const completed = steps.filter((step) => step.complete).length

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
              {completed} of {steps.length} ready
            </span>
            <span className="font-medium">
              Step {stepIndex + 1}: {current.label}
            </span>
          </div>
          <Progress
            value={(completed / steps.length) * 100}
            className="mt-2 h-1"
          />
        </div>
      </div>

      <ol
        className="mb-6 grid grid-cols-4 gap-2 lg:grid-cols-8"
        aria-label="Setup steps"
      >
        {steps.map((item, index) => {
          const Icon = stepIcons[item.id]
          const selected = index === stepIndex
          return (
            <li key={item.id}>
              <button
                type="button"
                onClick={() => setStepIndex(index)}
                aria-current={selected ? "step" : undefined}
                className={
                  selected
                    ? "flex w-full items-center gap-1.5 text-left text-[10px] font-medium text-foreground"
                    : item.complete
                      ? "flex w-full items-center gap-1.5 text-left text-[10px] text-emerald-400"
                      : "flex w-full items-center gap-1.5 text-left text-[10px] text-muted-foreground hover:text-foreground"
                }
              >
                {item.complete ? (
                  <Check className="size-3" aria-hidden="true" />
                ) : selected ? (
                  <Icon className="size-3" aria-hidden="true" />
                ) : (
                  <Circle className="size-3" aria-hidden="true" />
                )}
                <span>{item.label}</span>
              </button>
            </li>
          )
        })}
      </ol>

      <div className="min-h-64">
        {current.id === "sources" ? (
          <div className="space-y-5">
            <StepStatus step={current} />
            <ProviderStep
              profiles={bootstrap.providerProfiles}
              sessions={bootstrap.providerSessions}
              transport={transport}
              onChanged={onRefresh}
            />
          </div>
        ) : current.id === "research" ? (
          <LocalCapabilityStep
            step={current}
            profileId="local.files"
            bootstrap={bootstrap}
            onRefresh={onRefresh}
          />
        ) : current.id === "portfolio" ? (
          <LocalCapabilityStep
            step={current}
            profileId="local.portfolio-imports"
            bootstrap={bootstrap}
            onRefresh={onRefresh}
          />
        ) : current.id === "paper" ? (
          <PaperStep
            step={current}
            bootstrap={bootstrap}
            onRefresh={onRefresh}
          />
        ) : current.id === "mcp" ? (
          <McpStep step={current} instruction={bootstrap.mcpClient} />
        ) : current.id === "review" ? (
          <ReviewStep steps={steps} onRefresh={onRefresh} />
        ) : (
          <PlainStep
            step={current}
            supplemental={
              current.id === "storage"
                ? `Effective data directory: ${bootstrap.dataRoot}`
                : bootstrap.installation.detail
            }
          />
        )}
      </div>

      <div className="mt-6 flex items-center justify-between border-t border-border pt-4">
        <Button
          type="button"
          variant="ghost"
          onClick={() => setStepIndex((value) => Math.max(0, value - 1))}
          disabled={stepIndex === 0}
        >
          <ArrowLeft aria-hidden="true" />
          Back
        </Button>
        <Button
          type="button"
          onClick={() =>
            stepIndex === steps.length - 1
              ? onClose()
              : setStepIndex((value) => Math.min(steps.length - 1, value + 1))
          }
        >
          {stepIndex === steps.length - 1
            ? current.complete
              ? "Finish setup"
              : "Close review"
            : "Continue"}
          {stepIndex === steps.length - 1 ? (
            <Check aria-hidden="true" />
          ) : (
            <ArrowRight aria-hidden="true" />
          )}
        </Button>
      </div>
    </section>
  )
}

function McpStep({
  step,
  instruction,
}: {
  step: SetupStep
  instruction: DesktopBootstrap["mcpClient"]
}) {
  const clientConfiguration = instruction
    ? {
        mcpServers: {
          "market-squawk": {
            command: instruction.program,
            args: instruction.arguments,
            env: instruction.environment,
          },
        },
      }
    : null

  return (
    <div className="mx-auto max-w-2xl space-y-5 py-4">
      <StepStatus step={step} />
      {clientConfiguration ? (
        <section className="space-y-4 rounded-lg border border-border bg-background/35 p-4">
          <div>
            <h3 className="text-sm font-semibold">
              Generated local client configuration
            </h3>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              The installed capability is verified, but the service is stopped
              and no MCP client has been configured automatically.
            </p>
            <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
              Advanced policy supplied only through environment variables must
              also be added to your MCP client.
            </p>
          </div>
          <pre className="max-h-64 overflow-auto rounded-md border border-border bg-black/35 p-3 font-mono text-[11px] leading-relaxed text-foreground/85">
            {JSON.stringify(clientConfiguration, null, 2)}
          </pre>
          <div className="flex gap-3 rounded-md border border-amber-400/25 bg-amber-400/5 p-3">
            <CircleAlert
              className="mt-0.5 size-4 shrink-0 text-amber-300"
              aria-hidden="true"
            />
            <p className="text-xs leading-relaxed text-muted-foreground">
              Close Market Squawk before your MCP client starts this service.
              The desktop and MCP process use the same local workspace and must
              not own it at the same time.
            </p>
          </div>
        </section>
      ) : null}
    </div>
  )
}

function PlainStep({
  step,
  supplemental,
}: {
  step: SetupStep
  supplemental: string
}) {
  return (
    <div className="mx-auto max-w-2xl space-y-5 py-6">
      <StepStatus step={step} />
      <p className="rounded-lg border border-border bg-background/35 p-4 font-mono text-xs text-foreground/80">
        {supplemental}
      </p>
    </div>
  )
}

function StepStatus({ step }: { step: SetupStep }) {
  return (
    <div>
      <div className="flex items-center gap-2">
        <span
          className={
            step.complete
              ? "size-2 rounded-full bg-emerald-400"
              : step.state === "blocked"
                ? "size-2 rounded-full bg-amber-400"
                : "size-2 rounded-full bg-primary"
          }
          aria-hidden="true"
        />
        <h2 className="text-lg font-semibold">{step.label}</h2>
      </div>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">
        {step.detail}
      </p>
      {step.blockingReason ? (
        <div className="mt-4 flex gap-3 rounded-lg border border-amber-400/25 bg-amber-400/5 p-4">
          <CircleAlert
            className="mt-0.5 size-4 shrink-0 text-amber-300"
            aria-hidden="true"
          />
          <div>
            <p className="text-xs font-medium text-foreground">
              {step.blockingReason}
            </p>
            {step.recovery ? (
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                {step.recovery}
              </p>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  )
}

function LocalCapabilityStep({
  step,
  profileId,
  bootstrap,
  onRefresh,
}: {
  step: SetupStep
  profileId: string
  bootstrap: DesktopBootstrap
  onRefresh: () => void
}) {
  const profile = bootstrap.providerProfiles.find(
    (candidate) => candidate.id === profileId,
  )
  const session = bootstrap.providerSessions.find(
    (candidate) => candidate.surface_id === profileId,
  )

  return (
    <div className="mx-auto max-w-2xl space-y-5 py-4">
      <StepStatus step={step} />
      <section className="rounded-lg border border-border bg-background/35 p-4">
        <h3 className="text-sm font-semibold">
          {profile?.display_name ?? step.label}
        </h3>
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
          {profile?.coverage ?? "This local capability is not installed."}
        </p>
        <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
          Private import history:{" "}
          {session?.next_action === "active" ? "recorded" : "none recorded"}
        </p>
        <div className="mt-4 rounded-md border border-border bg-black/25 p-3">
          <p className="text-xs text-muted-foreground">
            Private data imports are optional. Import history is reported
            separately and never controls whether the installed capability is
            ready.
          </p>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <Button type="button" size="sm" variant="outline" onClick={onRefresh}>
            <RefreshCw aria-hidden="true" />
            Refresh status
          </Button>
        </div>
      </section>
    </div>
  )
}

function PaperStep({
  step,
  bootstrap,
  onRefresh,
}: {
  step: SetupStep
  bootstrap: DesktopBootstrap
  onRefresh: () => void
}) {
  const profile = bootstrap.providerProfiles.find(
    (candidate) => candidate.id === "local.paper-execution",
  )

  return (
    <div className="mx-auto max-w-2xl space-y-5 py-4">
      <StepStatus step={step} />
      <section className="rounded-lg border border-border bg-background/35 p-4">
        <h3 className="text-sm font-semibold">
          {profile?.display_name ?? step.label}
        </h3>
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
          {profile?.coverage ?? "This local capability is not installed."}
        </p>
        <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
          Paper services: {step.complete ? "available" : "unavailable"}
        </p>
        <div className="mt-4 rounded-md border border-border bg-black/25 p-3">
          <p className="text-xs text-muted-foreground">
            Paper execution starts stopped. Every start, order, cancellation,
            reconciliation, and kill-switch action remains typed, paper-only,
            and subject to central risk authority.
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="mt-4"
          onClick={onRefresh}
        >
          <RefreshCw aria-hidden="true" />
          Refresh status
        </Button>
      </section>
    </div>
  )
}

function ReviewStep({
  steps,
  onRefresh,
}: {
  steps: SetupStep[]
  onRefresh: () => void
}) {
  return (
    <div className="mx-auto max-w-2xl space-y-5 py-4">
      <StepStatus step={steps[steps.length - 1]!} />
      <ul className="divide-y divide-border rounded-lg border border-border bg-background/35">
        {steps.slice(0, -1).map((step) => (
          <li key={step.id} className="flex items-start gap-3 px-4 py-3">
            {step.complete ? (
              <Check
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
              <p className="text-xs font-medium">{step.label}</p>
              <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                {step.complete
                  ? step.detail
                  : (step.blockingReason ?? step.detail)}
              </p>
            </div>
          </li>
        ))}
      </ul>
      <Button type="button" size="sm" variant="outline" onClick={onRefresh}>
        <RefreshCw aria-hidden="true" />
        Refresh all status
      </Button>
    </div>
  )
}
