import * as React from "react"
import {
  ArrowRight,
  CheckCircle2,
  CircleAlert,
  ListChecks,
  RefreshCw,
  Settings2,
} from "lucide-react"

import { SetupFlow } from "@/components/setup/setup-flow"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  setupPlanStatusSchema,
  type DesktopBootstrap,
  type SetupPlanStatus,
} from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

type PlanRead =
  | { state: "loading"; status: null; error: null }
  | { state: "ready"; status: SetupPlanStatus; error: null }
  | { state: "error"; status: SetupPlanStatus | null; error: string }

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
  const [planRead, setPlanRead] = React.useState<PlanRead>({
    state: "loading",
    status: null,
    error: null,
  })

  const refreshPlanStatus = React.useCallback(async () => {
    setPlanRead((current) =>
      current.status
        ? current
        : { state: "loading", status: null, error: null },
    )
    try {
      const result = await transport.query({ query: "setupPlanStatus" })
      const status = setupPlanStatusSchema.parse(result.data)
      setPlanRead({ state: "ready", status, error: null })
      return status
    } catch (error) {
      const message = messageFrom(error)
      setPlanRead((current) => ({
        state: "error",
        status: current.status,
        error: message,
      }))
      throw error
    }
  }, [transport])

  React.useEffect(() => {
    void refreshPlanStatus().catch(() => undefined)
  }, [refreshPlanStatus])

  if (open) {
    return (
      <SetupFlow
        bootstrap={bootstrap}
        transport={transport}
        planStatus={planRead.status}
        planStatusLoading={planRead.state === "loading"}
        planStatusError={planRead.error}
        onClose={() => setOpen(false)}
        onRefresh={onRefresh}
        onRefreshPlanStatus={refreshPlanStatus}
      />
    )
  }

  const accepted = planRead.status?.acceptedPlan ?? null
  const finishLater =
    accepted?.plan.steps.filter(
      (step) => step.disposition === "available_to_finish_later",
    ).length ?? 0

  return (
    <section className="rounded-xl border border-border bg-card/55 p-5">
      <div className="flex items-start gap-3">
        <span className="rounded-lg border border-border bg-background p-2">
          <ListChecks className="size-4 text-primary" aria-hidden="true" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Guided setup
          </p>
          <h2 className="mt-1 text-base font-semibold">
            Set up everything for me
          </h2>
          <p className="mt-2 max-w-3xl text-xs leading-relaxed text-muted-foreground">
            Start with the complete private, zero-fee, and safety-first plan.
            You choose the goals, inspect one immutable preview, and explicitly
            accept it before any plan becomes active.
          </p>
        </div>
      </div>

      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
        {planRead.state === "loading" ? (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <RefreshCw className="size-3.5 animate-spin" aria-hidden="true" />
            Reading the durable setup authority…
          </div>
        ) : planRead.error && !accepted ? (
          <SetupNotice
            tone="attention"
            title="Setup status is unavailable"
            detail={`${planRead.error} No plan or completion state is inferred while the owner is unavailable.`}
          />
        ) : accepted ? (
          <div className="flex items-start gap-3">
            <CheckCircle2
              className="mt-0.5 size-4 shrink-0 text-emerald-400"
              aria-hidden="true"
            />
            <div>
              <p className="text-xs font-medium">Accepted plan recorded</p>
              <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                Revision {accepted.revision} is the chosen configuration. Its
                eleven steps still use live owner evidence; acceptance never
                marks a capability complete.
              </p>
              <p className="mt-2 font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
                {finishLater === 0
                  ? "Complete recommended plan"
                  : `${finishLater} capabilities skipped in this setup run`}
              </p>
            </div>
          </div>
        ) : (
          <SetupNotice
            tone="neutral"
            title="No setup plan has been accepted"
            detail="The recommended plan is selected by default. Previewing or closing setup does not accept it."
          />
        )}
      </div>

      {planRead.error && accepted ? (
        <p
          role="status"
          className="mt-3 text-[11px] leading-relaxed text-amber-300"
        >
          The last recorded plan is shown, but refresh failed: {planRead.error}
        </p>
      ) : null}

      <div className="mt-4 flex flex-wrap items-center gap-3">
        <Button type="button" onClick={() => setOpen(true)}>
          {accepted ? "Resume guided setup" : "Review recommended plan"}
          <ArrowRight aria-hidden="true" />
        </Button>
        {planRead.state === "error" ? (
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => void refreshPlanStatus().catch(() => undefined)}
          >
            <RefreshCw aria-hidden="true" />
            Retry status
          </Button>
        ) : null}
        <Dialog>
          <DialogTrigger asChild>
            <Button type="button" variant="ghost" size="sm">
              <Settings2 aria-hidden="true" />
              Review advanced settings
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Advanced setup</DialogTitle>
              <DialogDescription>
                The guided plan remains the safest default. Resource limits,
                retention, and workspace settings stay available through the
                typed Settings workspace.
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
              <div>
                <dt className="text-xs text-muted-foreground">Workspace</dt>
                <dd className="mt-1 break-all font-mono text-xs">
                  {bootstrap.runtime.workspaceId}
                </dd>
              </div>
            </dl>
          </DialogContent>
        </Dialog>
        <span className="ml-auto font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
          Safe to close · owner evidence resumes
        </span>
      </div>
    </section>
  )
}

function SetupNotice({
  tone,
  title,
  detail,
}: {
  tone: "neutral" | "attention"
  title: string
  detail: string
}) {
  const Icon = tone === "attention" ? CircleAlert : ListChecks
  return (
    <div className="flex items-start gap-3">
      <Icon
        className={
          tone === "attention"
            ? "mt-0.5 size-4 shrink-0 text-amber-300"
            : "mt-0.5 size-4 shrink-0 text-muted-foreground"
        }
        aria-hidden="true"
      />
      <div>
        <p className="text-xs font-medium">{title}</p>
        <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
          {detail}
        </p>
      </div>
    </div>
  )
}

function messageFrom(error: unknown) {
  return error instanceof Error
    ? error.message
    : "The setup authority could not be read."
}
