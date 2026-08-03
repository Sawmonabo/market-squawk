import {
  ArrowLeft,
  Bot,
  Check,
  Database,
  FileInput,
  Gauge,
  HardDrive,
  KeyRound,
  Laptop,
  ListChecks,
  RefreshCw,
  SearchCheck,
  ShieldCheck,
  Sparkles,
  type LucideIcon,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import type {
  SetupPlanPreview,
  SetupPlanStatus,
} from "@/lib/schemas"
import { plainToken } from "./setup-copy"

import type {
  SetupGoal,
  SetupPlanSelection,
  SetupStarterPlan,
} from "@/lib/transport"

export type PlanStep = SetupPlanPreview["plan"]["steps"][number]
export type PlanStepId = PlanStep["id"]

const goalCopy: Record<SetupGoal, { label: string; detail: string }> = {
  everything_recommended: {
    label: "Everything recommended",
    detail: "Install and configure the complete safety-first local experience.",
  },
  explore_public_markets: {
    label: "Explore public markets",
    detail: "Use public and zero-fee sources with explicit coverage limits.",
  },
  research_investments: {
    label: "Research investments",
    detail: "Build durable datasets, screens, dossiers, and evidence trails.",
  },
  manage_portfolio: {
    label: "Manage a portfolio",
    detail: "Import holdings and transactions with reconciliation evidence.",
  },
  build_and_evaluate_models: {
    label: "Build and evaluate models",
    detail: "Use the managed local runtime and admitted model bundles.",
  },
  practice_paper_execution: {
    label: "Practice paper execution",
    detail: "Keep a stopped-by-default paper account behind central risk.",
  },
  use_claude_code: {
    label: "Use Claude Code",
    detail: "Connect and verify Claude Code through its own protected identity.",
  },
  use_codex: {
    label: "Use Codex",
    detail: "Connect and verify Codex separately through the shared service.",
  },
}

const starterPlanCopy: Record<SetupStarterPlan, string> = {
  everything_recommended: "Everything recommended",
  public_markets: "Public markets",
  research: "Investment research",
  portfolio: "Portfolio management",
  models: "Models and forecasts",
  paper_practice: "Paper practice",
  ai_clients: "AI clients",
}

export const stepPresentation: Record<
  PlanStepId,
  { label: string; icon: LucideIcon }
> = {
  goals_and_starter_plan: { label: "Goals & plan", icon: ListChecks },
  storage_retention_time_and_disk: { label: "Storage", icon: HardDrive },
  public_and_zero_fee_providers: { label: "Providers", icon: KeyRound },
  file_and_portfolio_import: { label: "Import", icon: FileInput },
  model_runtime: { label: "Model runtime", icon: Sparkles },
  paper_and_risk: { label: "Paper & risk", icon: ShieldCheck },
  claude_code: { label: "Claude Code", icon: Bot },
  codex: { label: "Codex", icon: Laptop },
  backup: { label: "Backup", icon: Database },
  review: { label: "Review", icon: SearchCheck },
  first_useful_result: { label: "First result", icon: Gauge },
}

export const defaultSelection: SetupPlanSelection = {
  goals: ["everything_recommended"],
  starterPlan: "everything_recommended",
}

export function PlanBuilder({
  status,
  selection,
  onSelectionChange,
  preview,
  previewing,
  previewError,
  confirmed,
  accepting,
  onConfirmedChange,
  onPreview,
  onAccept,
  onDiscardPreview,
  onRefreshStatus,
}: {
  status: SetupPlanStatus | null
  selection: SetupPlanSelection
  onSelectionChange: (selection: SetupPlanSelection) => void
  preview: SetupPlanPreview | null
  previewing: boolean
  previewError: string | null
  confirmed: boolean
  accepting: boolean
  onConfirmedChange: (confirmed: boolean) => void
  onPreview: () => void
  onAccept: () => void
  onDiscardPreview: () => void
  onRefreshStatus: () => void
}) {
  if (!status) return null

  if (preview) {
    return (
      <PlanPreview
        preview={preview}
        confirmed={confirmed}
        accepting={accepting}
        error={previewError}
        onConfirmedChange={onConfirmedChange}
        onAccept={onAccept}
        onBack={onDiscardPreview}
        onRefreshStatus={onRefreshStatus}
      />
    )
  }

  const toggleGoal = (goal: SetupGoal) => {
    if (goal === "everything_recommended") {
      onSelectionChange({ goals: [goal], starterPlan: "everything_recommended" })
      return
    }
    const withoutRecommended = selection.goals.filter(
      (item) => item !== "everything_recommended",
    )
    const next = withoutRecommended.includes(goal)
      ? withoutRecommended.length === 1
        ? withoutRecommended
        : withoutRecommended.filter((item) => item !== goal)
      : [...withoutRecommended, goal]
    onSelectionChange({
      goals: next,
      starterPlan: starterMatchesGoals(selection.starterPlan, next)
        ? selection.starterPlan
        : recommendedStarterFor(next),
    })
  }

  return (
    <div className="mt-5 grid gap-6 xl:grid-cols-[1.35fr_0.8fr]">
      <section aria-labelledby="setup-goals-heading">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Step 1 of 2
        </p>
        <h3 id="setup-goals-heading" className="mt-1 text-sm font-semibold">
          What do you want Market Squawk to help with?
        </h3>
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
          Everything recommended is selected by default. Choose a narrower closed goal set only
          when you intentionally want to skip an installed capability in this setup run and
          resume it from this checklist.
        </p>
        <div className="mt-4 grid gap-2 sm:grid-cols-2">
          {status.catalog.goals.map((goal) => {
            const copy = goalCopy[goal]
            const selected = selection.goals.includes(goal)
            return (
              <label
                key={goal}
                className={
                  selected
                    ? "flex cursor-pointer gap-3 rounded-lg border border-primary/50 bg-primary/5 p-3"
                    : "flex cursor-pointer gap-3 rounded-lg border border-border bg-background/30 p-3 hover:border-foreground/25"
                }
              >
                <input
                  type="checkbox"
                  checked={selected}
                  onChange={() => toggleGoal(goal)}
                  className="mt-0.5 size-3.5 accent-primary"
                />
                <span>
                  <span className="block text-xs font-medium">{copy.label}</span>
                  <span className="mt-1 block text-[10px] leading-relaxed text-muted-foreground">
                    {copy.detail}
                  </span>
                </span>
              </label>
            )
          })}
        </div>
      </section>

      <section aria-labelledby="starter-plan-heading">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Step 2 of 2
        </p>
        <h3 id="starter-plan-heading" className="mt-1 text-sm font-semibold">
          Choose a starter plan
        </h3>
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
          A starter plan sets the order and defaults. The next screen is an immutable preview;
          requesting it does not accept anything.
        </p>
        <fieldset className="mt-4 space-y-2">
          <legend className="sr-only">Starter plan</legend>
          {status.catalog.starterPlans.map((starterPlan) => (
            <label
              key={starterPlan}
              className={
                starterMatchesGoals(starterPlan, selection.goals)
                  ? "flex cursor-pointer items-center gap-2 rounded-md border border-border bg-background/30 px-3 py-2.5 text-xs"
                  : "flex cursor-not-allowed items-center gap-2 rounded-md border border-border/60 bg-background/15 px-3 py-2.5 text-xs text-muted-foreground/60"
              }
            >
              <input
                type="radio"
                name="starter-plan"
                value={starterPlan}
                checked={selection.starterPlan === starterPlan}
                onChange={() => onSelectionChange({ ...selection, starterPlan })}
                disabled={!starterMatchesGoals(starterPlan, selection.goals)}
                className="size-3.5 accent-primary"
              />
              <span>{starterPlanCopy[starterPlan]}</span>
              {starterPlan === status.catalog.recommendedStarterPlan ? (
                <span className="ml-auto font-mono text-[8px] uppercase tracking-wider text-primary">
                  Recommended
                </span>
              ) : !starterMatchesGoals(starterPlan, selection.goals) ? (
                <span className="ml-auto text-[9px]">Needs a matching goal</span>
              ) : null}
            </label>
          ))}
        </fieldset>
        <p className="mt-3 text-[10px] leading-relaxed text-muted-foreground">
          A starter is available only when at least one selected goal matches it. Everything
          recommended is an all-inclusive goal and cannot be mixed with a narrower starter.
        </p>
        {selection.goals.length === 0 ? (
          <p role="alert" className="mt-3 text-xs text-amber-300">
            Choose at least one goal before requesting a preview.
          </p>
        ) : null}
        {previewError ? <InlineError message={previewError} /> : null}
        <Button
          type="button"
          className="mt-4 w-full"
          onClick={onPreview}
          disabled={previewing || selection.goals.length === 0}
        >
          {previewing ? (
            <RefreshCw className="animate-spin" aria-hidden="true" />
          ) : (
            <ListChecks aria-hidden="true" />
          )}
          Create exact preview
        </Button>
      </section>
    </div>
  )
}

function PlanPreview({
  preview,
  confirmed,
  accepting,
  error,
  onConfirmedChange,
  onAccept,
  onBack,
  onRefreshStatus,
}: {
  preview: SetupPlanPreview
  confirmed: boolean
  accepting: boolean
  error: string | null
  onConfirmedChange: (confirmed: boolean) => void
  onAccept: () => void
  onBack: () => void
  onRefreshStatus: () => void
}) {
  const expired = previewExpired(preview)
  return (
    <div className="mt-5 space-y-5">
      <section className="rounded-lg border border-primary/30 bg-primary/5 p-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
              Immutable setup preview
            </p>
            <h3 className="mt-1 text-sm font-semibold">Review before acceptance</h3>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              This preview changes nothing. It is bound to the workspace, revision, and digest
              shown below and expires automatically.
            </p>
          </div>
          <dl className="grid grid-cols-2 gap-x-5 gap-y-2 text-[10px]">
            <PreviewFact label="Revision" value={preview.currentRevision} />
            <PreviewFact label="Plan revision" value={preview.plan.revision} />
            <PreviewFact label="Issued" value={formatUnixSeconds(preview.issuedAtUnixSeconds)} />
            <PreviewFact label="Expires" value={formatUnixSeconds(preview.expiresAtUnixSeconds)} />
          </dl>
        </div>
        <div className="mt-4 grid gap-3 sm:grid-cols-3">
          <SummaryFact
            label="Expected active time"
            value={`${preview.expectedTime.expectedActiveMinutes} minutes`}
            detail={
              preview.expectedTime.includesExternalWait
                ? "External account or provider wait may take longer."
                : "No external waiting time is included."
            }
          />
          <SummaryFact
            label="First useful result target"
            value={`${preview.expectedTime.firstUseTargetMinutes} minutes`}
            detail="Only real owner output satisfies this target."
          />
          <SummaryFact
            label="Workspace soft limit"
            value={formatBytes(preview.expectedDisk.workspaceSoftLimitBytes)}
            detail={preview.expectedDisk.includedImpacts.map(plainToken).join(" · ")}
          />
        </div>
      </section>

      <details className="rounded-lg border border-border bg-background/30 p-4">
        <summary className="cursor-pointer text-xs font-medium">
          Exact preview scope and digests
        </summary>
        <p className="mt-2 text-[10px] leading-relaxed text-muted-foreground">
          These full values bind acceptance to one workspace, immutable preview, plan, and
          confirmation digest. They are selectable for copying into an audit or support record.
        </p>
        <dl className="mt-4 grid gap-3 sm:grid-cols-2">
          <ExactEvidence label="Owner workspace" value={preview.ownerWorkspace} />
          <ExactEvidence label="Preview ID" value={preview.previewId} />
          <ExactEvidence label="Plan SHA-256" value={preview.planDigest} />
          <ExactEvidence label="Preview SHA-256" value={preview.previewSha256} />
        </dl>
      </details>

      <ol className="grid gap-3 lg:grid-cols-2" aria-label="Exact setup plan">
        {preview.plan.steps.map((step, index) => (
          <PlanStepCard key={step.id} step={step} index={index} />
        ))}
      </ol>

      <section className="grid gap-4 lg:grid-cols-2">
        <AggregateList
          title="Included capabilities"
          empty="No capability is included."
          values={preview.includedCapabilities.map(plainToken)}
        />
        <AggregateList
          title="Official external contacts"
          empty="No external contact is declared."
          values={preview.externalContacts.map(plainToken)}
        />
        <AggregateList
          title="Reversible local changes"
          empty="No reversible local change is declared."
          values={preview.reversibleLocalChanges.map(plainToken)}
        />
        <AggregateList
          title="Safe to leave unfinished"
          empty="No step is marked safe to skip in this setup run."
          values={preview.safeSkipSteps.map(setupStepLabel)}
          footer="These capabilities remain installed and unfinished. They never become complete until owner evidence proves the work."
        />
      </section>

      {expired ? (
        <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-4">
          <p className="text-xs font-medium text-amber-300">This preview has expired</p>
          <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
            Refresh the setup status and create a new immutable preview. Nothing was accepted.
          </p>
          <Button type="button" size="sm" variant="outline" className="mt-3" onClick={onRefreshStatus}>
            <RefreshCw aria-hidden="true" />
            Refresh status
          </Button>
        </div>
      ) : (
        <label className="flex items-start gap-3 rounded-lg border border-border bg-background/35 p-4 text-xs leading-relaxed">
          <input
            type="checkbox"
            checked={confirmed}
            onChange={(event) => onConfirmedChange(event.target.checked)}
            disabled={accepting}
            className="mt-0.5 size-3.5 accent-primary"
          />
          <span>
            <span className="block font-medium">I reviewed this exact plan.</span>
            <span className="mt-1 block text-muted-foreground">
              I understand plan acceptance records these choices only. It does not contact a
              provider, complete a capability, start paper execution, or prove a first result.
            </span>
          </span>
        </label>
      )}

      {error ? <InlineError message={error} /> : null}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4">
        <Button type="button" variant="ghost" onClick={onBack} disabled={accepting}>
          <ArrowLeft aria-hidden="true" />
          Back to choices
        </Button>
        <Button
          type="button"
          onClick={onAccept}
          disabled={!confirmed || accepting || expired}
        >
          {accepting ? (
            <RefreshCw className="animate-spin" aria-hidden="true" />
          ) : (
            <Check aria-hidden="true" />
          )}
          Accept this exact plan
        </Button>
      </div>
    </div>
  )
}

function PlanStepCard({ step, index }: { step: PlanStep; index: number }) {
  const presentation = stepPresentation[step.id]
  const Icon = presentation.icon
  return (
    <li className="rounded-lg border border-border bg-background/30 p-4">
      <div className="flex items-start gap-3">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-md border border-border bg-background">
          <Icon className="size-3.5 text-primary" aria-hidden="true" />
        </span>
        <div className="min-w-0">
          <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
            Step {index + 1} · {presentation.label}
          </p>
          <p className="mt-1 text-xs font-medium">{plainToken(step.outcome)}</p>
        </div>
      </div>
      <dl className="mt-4 space-y-2 text-[10px] leading-relaxed">
        <DetailRow label="Required input" value={plainToken(step.requiredInput)} />
        <DetailRow
          label="Official contact"
          value={
            step.externalContacts.length > 0
              ? step.externalContacts.map(plainToken).join(" · ")
              : "None"
          }
        />
        <DetailRow
          label="Reversible change"
          value={
            step.reversibleLocalChange
              ? plainToken(step.reversibleLocalChange)
              : "No local change declared"
          }
        />
        <DetailRow
          label="Time / disk"
          value={`${step.expectedActiveMinutes} min · ${plainToken(step.diskImpact)}`}
        />
        <DetailRow
          label="Skip in this setup run"
          value={
            step.safeSkip === "capability_remains_installed_and_available"
              ? "Allowed; capability remains installed and unfinished"
              : "Not safely skippable"
          }
        />
      </dl>
      <details className="mt-3 border-t border-border pt-3">
        <summary className="cursor-pointer font-mono text-[8px] uppercase tracking-wider text-muted-foreground">
          Exact contract identities
        </summary>
        <p className="mt-2 break-all font-mono text-[9px] leading-relaxed text-muted-foreground">
          outcome={step.outcome} · input={step.requiredInput} · disk={step.diskImpact} ·
          safe-skip={step.safeSkip}
        </p>
      </details>
    </li>
  )
}

function starterMatchesGoals(
  starter: SetupStarterPlan,
  goals: SetupGoal[],
) {
  switch (starter) {
    case "everything_recommended":
      return goals.length === 1 && goals[0] === "everything_recommended"
    case "public_markets":
      return goals.includes("explore_public_markets")
    case "research":
      return goals.includes("research_investments")
    case "portfolio":
      return goals.includes("manage_portfolio")
    case "models":
      return goals.includes("build_and_evaluate_models")
    case "paper_practice":
      return goals.includes("practice_paper_execution")
    case "ai_clients":
      return goals.includes("use_claude_code") || goals.includes("use_codex")
  }
}

function recommendedStarterFor(goals: SetupGoal[]): SetupStarterPlan {
  if (goals.length === 1 && goals[0] === "everything_recommended") {
    return "everything_recommended"
  }
  if (goals.includes("manage_portfolio")) return "portfolio"
  if (goals.includes("build_and_evaluate_models")) return "models"
  if (goals.includes("practice_paper_execution")) return "paper_practice"
  if (goals.includes("research_investments")) return "research"
  if (goals.includes("explore_public_markets")) return "public_markets"
  return "ai_clients"
}

function InlineError({ message }: { message: string }) {
  return (
    <p
      role="alert"
      className="mt-4 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-xs leading-relaxed text-destructive"
    >
      {message}
    </p>
  )
}

function PreviewFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 font-mono text-foreground">{value}</dd>
    </div>
  )
}

function ExactEvidence({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded-md border border-border bg-black/20 p-3">
      <dt className="font-mono text-[8px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1.5 select-all break-all font-mono text-[10px] leading-relaxed text-foreground/90">
        {value}
      </dd>
    </div>
  )
}

function SummaryFact({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="rounded-md border border-border bg-background/40 p-3">
      <p className="font-mono text-[8px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1.5 text-xs font-medium">{value}</p>
      <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function AggregateList({
  title,
  values,
  empty,
  footer,
}: {
  title: string
  values: string[]
  empty: string
  footer?: string
}) {
  return (
    <section className="rounded-lg border border-border bg-background/30 p-4">
      <h3 className="text-xs font-semibold">{title}</h3>
      {values.length ? (
        <ul className="mt-3 space-y-2 text-[10px] leading-relaxed text-muted-foreground">
          {values.map((value) => (
            <li key={value} className="flex gap-2">
              <span className="mt-1.5 size-1 shrink-0 rounded-full bg-primary" aria-hidden="true" />
              <span className="break-all">{value}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-3 text-[10px] leading-relaxed text-muted-foreground">{empty}</p>
      )}
      {footer ? (
        <p className="mt-3 border-t border-border pt-3 text-[10px] leading-relaxed text-amber-300">
          {footer}
        </p>
      ) : null}
    </section>
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

function formatBytes(value: string) {
  try {
    const bytes = BigInt(value)
    const gib = 1024n ** 3n
    const tenths = (bytes * 10n) / gib
    return `${tenths / 10n}.${tenths % 10n} GiB (${value} bytes)`
  } catch {
    return `${value} bytes`
  }
}

export function formatUnixSeconds(value: string) {
  const seconds = Number(value)
  if (!Number.isSafeInteger(seconds)) return `${value} Unix seconds`
  const date = new Date(seconds * 1_000)
  return Number.isNaN(date.valueOf()) ? `${value} Unix seconds` : date.toLocaleString()
}

export function previewExpired(preview: SetupPlanPreview) {
  try {
    return BigInt(preview.expiresAtUnixSeconds) <= BigInt(Math.floor(Date.now() / 1_000))
  } catch {
    return true
  }
}

function setupStepLabel(value: string) {
  if (value in stepPresentation) {
    return stepPresentation[value as PlanStepId].label
  }
  return plainToken(value)
}
