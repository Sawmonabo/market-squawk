import * as React from "react"
import {
  ArrowLeft,
  ArrowRight,
  Check,
  CheckCircle2,
  Circle,
  CircleAlert,
  RefreshCw,
} from "lucide-react"
import { Link } from "react-router-dom"

import { ProviderStep } from "@/components/setup/provider-step"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import type { DesktopBootstrap } from "@/lib/schemas"
import type {
  ProductTransport,
  SetupPlanSelection,
} from "@/lib/transport"

import type {
  EvidenceMap,
  EvidenceTone,
  StepEvidence,
} from "./setup-evidence-model"
import {
  stepPresentation,
  type PlanStep,
  type PlanStepId,
} from "./setup-plan"
import { plainToken } from "./setup-copy"

export function SetupChecklist({
  bootstrap,
  transport,
  steps,
  selection,
  evidence,
  refreshing,
  refreshError,
  onRefresh,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  steps: PlanStep[]
  selection: SetupPlanSelection
  evidence: EvidenceMap
  refreshing: boolean
  refreshError: string | null
  onRefresh: () => void
}) {
  const [stepIndex, setStepIndex] = React.useState(0)
  const current = steps[stepIndex]
  if (!current) return null
  const currentEvidence = evidence[current.id]
  const completeCount = Object.values(evidence).filter((item) => item.complete).length
  const finishLater =
    current.safeSkip === "capability_remains_installed_and_available" &&
    !currentEvidence.complete

  return (
    <div className="mt-5">
      <div className="flex flex-wrap items-center gap-3">
        <div className="min-w-48 flex-1">
          <div className="flex items-center justify-between gap-3 text-xs">
            <span className="font-mono uppercase tracking-wider text-muted-foreground">
              {completeCount} of {steps.length} owner checks complete
            </span>
            <span className="font-medium">
              Step {stepIndex + 1}: {stepPresentation[current.id].label}
            </span>
          </div>
          <Progress value={(completeCount / steps.length) * 100} className="mt-2 h-1" />
        </div>
        <Button type="button" size="sm" variant="outline" onClick={onRefresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh owner evidence
        </Button>
      </div>

      {refreshError ? (
        <p role="status" className="mt-3 text-[11px] leading-relaxed text-amber-300">
          Some owner evidence is unavailable: {refreshError}
        </p>
      ) : null}

      <ol className="mt-5 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6" aria-label="Setup steps">
        {steps.map((step, index) => {
          const itemEvidence = evidence[step.id]
          const Icon = stepPresentation[step.id].icon
          const selected = index === stepIndex
          return (
            <li key={step.id}>
              <button
                type="button"
                onClick={() => setStepIndex(index)}
                aria-current={selected ? "step" : undefined}
                className={
                  selected
                    ? "flex min-h-14 w-full items-center gap-2 rounded-md border border-primary/45 bg-primary/5 px-2.5 py-2 text-left text-[10px] font-medium"
                    : "flex min-h-14 w-full items-center gap-2 rounded-md border border-border bg-background/25 px-2.5 py-2 text-left text-[10px] text-muted-foreground hover:text-foreground"
                }
              >
                {itemEvidence.complete ? (
                  <Check className="size-3 shrink-0 text-emerald-400" aria-hidden="true" />
                ) : itemEvidence.tone === "loading" ? (
                  <RefreshCw className="size-3 shrink-0 animate-spin" aria-hidden="true" />
                ) : selected ? (
                  <Icon className="size-3 shrink-0 text-primary" aria-hidden="true" />
                ) : (
                  <Circle className="size-3 shrink-0" aria-hidden="true" />
                )}
                <span>{stepPresentation[step.id].label}</span>
              </button>
            </li>
          )
        })}
      </ol>

      <div className="mt-5 grid gap-5 xl:grid-cols-[1fr_0.75fr]">
        <section className="rounded-lg border border-border bg-background/30 p-5">
          <div className="flex items-start gap-3">
            <span className="flex size-8 shrink-0 items-center justify-center rounded-md border border-border bg-background">
              {React.createElement(stepPresentation[current.id].icon, {
                className: "size-4 text-primary",
                "aria-hidden": true,
              })}
            </span>
            <div>
              <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
                Step {stepIndex + 1} of {steps.length}
              </p>
              <h3 className="mt-1 text-base font-semibold">{stepPresentation[current.id].label}</h3>
              <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                {plainToken(current.outcome)}
              </p>
            </div>
          </div>

          <div className="mt-5">
            <EvidenceNotice evidence={currentEvidence} />
          </div>

          <dl className="mt-5 grid gap-3 sm:grid-cols-2">
            <ChecklistFact label="Required input" value={plainToken(current.requiredInput)} />
            <ChecklistFact
              label="Official external contact"
              value={
                current.externalContacts.length
                  ? current.externalContacts.map(plainToken).join(" · ")
                  : "None"
              }
            />
            <ChecklistFact
              label="Reversible local change"
              value={
                current.reversibleLocalChange
                  ? plainToken(current.reversibleLocalChange)
                  : "No local change declared"
              }
            />
            <ChecklistFact
              label="Expected time and disk"
              value={`${current.expectedActiveMinutes} active min · ${plainToken(current.diskImpact)}`}
            />
          </dl>

          <StepPrimaryAction
            step={current}
            selection={selection}
            bootstrap={bootstrap}
            transport={transport}
            onRefresh={onRefresh}
          />
        </section>

        <section className="rounded-lg border border-border bg-background/20 p-5">
          <h3 className="text-sm font-semibold">Current evidence and recovery</h3>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            Readiness comes from the service owner named by this step. Refreshing or restarting
            re-runs the owner check; plan acceptance and operation availability are not evidence.
          </p>
          <dl className="mt-4 space-y-3 text-[11px]">
            <DetailRow label="Evidence state" value={evidenceLabel(currentEvidence.tone)} />
            <DetailRow
              label="Plan disposition"
              value={
                current.disposition === "included"
                  ? "Included in the accepted plan"
                  : "Skipped in this setup run; installed and available when setup resumes"
              }
            />
            <DetailRow label="Owner detail" value={currentEvidence.detail} />
          </dl>
          {finishLater ? (
            <div className="mt-4 rounded-md border border-amber-400/25 bg-amber-400/5 p-3">
              <p className="text-xs font-medium text-amber-300">Safe to leave unfinished</p>
              <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                The capability remains installed and available when setup resumes. It is still
                unfinished and is never counted as complete until its owner evidence passes.
              </p>
            </div>
          ) : !currentEvidence.complete ? (
            <div className="mt-4 rounded-md border border-border bg-black/20 p-3 text-[11px] leading-relaxed text-muted-foreground">
              This step is not safely skippable. You may close setup without mutation, but the
              checklist will keep this gap visible.
            </div>
          ) : null}
        </section>
      </div>

      <div className="mt-5 flex items-center justify-between border-t border-border pt-4">
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
          variant={finishLater ? "outline" : "default"}
          onClick={() => setStepIndex((value) => Math.min(steps.length - 1, value + 1))}
          disabled={stepIndex === steps.length - 1}
        >
          {finishLater ? "Leave unfinished and continue" : "Next step"}
          <ArrowRight aria-hidden="true" />
        </Button>
      </div>
    </div>
  )
}

function StepPrimaryAction({
  step,
  selection,
  bootstrap,
  transport,
  onRefresh,
}: {
  step: PlanStep
  selection: SetupPlanSelection
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onRefresh: () => void
}) {
  const action = primaryAction(step.id, selection)
  return (
    <div className="mt-5 border-t border-border pt-4">
      {step.id === "public_and_zero_fee_providers" ? (
        <div className="mb-5">
          <ProviderStep
            profiles={bootstrap.providerProfiles}
            sessions={bootstrap.providerSessions}
            transport={transport}
            onChanged={onRefresh}
          />
        </div>
      ) : null}
      <div className="flex flex-wrap items-center gap-2">
        <Button asChild size="sm">
          <Link to={action.to}>{action.label}</Link>
        </Button>
        {step.id === "file_and_portfolio_import" ? (
          <Button asChild size="sm" variant="outline">
            <Link to="/advanced/research-data">Open owned-file research</Link>
          </Button>
        ) : null}
        {step.id === "paper_and_risk" ? (
          <Button asChild size="sm" variant="outline">
            <Link to="/advanced/risk-recommendation-policy">Review central risk</Link>
          </Button>
        ) : null}
      </div>
    </div>
  )
}

function primaryAction(step: PlanStepId, selection: SetupPlanSelection) {
  switch (step) {
    case "goals_and_starter_plan":
      return { to: "/home", label: "Return to plan controls" }
    case "storage_retention_time_and_disk":
      return { to: "/system/settings", label: "Open Settings" }
    case "public_and_zero_fee_providers":
      return { to: "/connections/sources", label: "Open Connections & Sources" }
    case "file_and_portfolio_import":
      return selection.goals.includes("research_investments") &&
        !selection.goals.includes("manage_portfolio")
        ? { to: "/advanced/research-data", label: "Open Research imports" }
        : { to: "/portfolio", label: "Open Portfolio import" }
    case "model_runtime":
      return { to: "/advanced/models-forecasts", label: "Open Models" }
    case "paper_and_risk":
      return { to: "/paper-execution", label: "Open Paper Execution" }
    case "claude_code":
      return { to: "/system/ai-connections", label: "Open Claude Code setup" }
    case "codex":
      return { to: "/system/ai-connections", label: "Open Codex setup" }
    case "backup":
      return { to: "/system/backup-recovery", label: "Open Backup & Recovery" }
    case "review":
      return { to: "/home", label: "Open capability review" }
    case "first_useful_result":
      return { to: "/home", label: "Open Home" }
  }
}

function EvidenceNotice({ evidence }: { evidence: StepEvidence }) {
  const Icon = evidence.complete
    ? CheckCircle2
    : evidence.tone === "loading"
      ? RefreshCw
      : CircleAlert
  const className = evidence.complete
    ? "border-emerald-400/25 bg-emerald-400/5"
    : evidence.tone === "unavailable" || evidence.tone === "degraded"
      ? "border-amber-400/25 bg-amber-400/5"
      : "border-border bg-black/20"
  return (
    <div className={`flex items-start gap-3 rounded-lg border p-4 ${className}`}>
      <Icon
        className={`mt-0.5 size-4 shrink-0 ${
          evidence.complete ? "text-emerald-400" : "text-amber-300"
        } ${evidence.tone === "loading" ? "animate-spin" : ""}`}
        aria-hidden="true"
      />
      <div>
        <p className="text-xs font-medium">{evidence.headline}</p>
        <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
          {evidence.detail}
        </p>
      </div>
    </div>
  )
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid grid-cols-[105px_1fr] gap-3">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="break-words text-foreground/85">{value}</dd>
    </div>
  )
}

function ChecklistFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border bg-black/15 p-3">
      <dt className="font-mono text-[8px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1.5 break-words text-[11px] leading-relaxed text-foreground/85">{value}</dd>
    </div>
  )
}

function evidenceLabel(tone: EvidenceTone) {
  switch (tone) {
    case "loading":
      return "Loading — no completion assumed"
    case "ready":
      return "Complete from current owner evidence"
    case "recorded":
      return "Recorded fact, not capability completion"
    case "unfinished":
      return "Installed and unfinished"
    case "degraded":
      return "Partial or degraded evidence"
    case "unavailable":
      return "Owner evidence unavailable"
  }
}
