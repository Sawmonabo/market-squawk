import * as React from "react"
import { z } from "zod"
import {
  ArrowLeft,
  CheckCircle2,
  CircleAlert,
  RefreshCw,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  setupPlanPreviewSchema,
  setupPlanReceiptSchema,
  type DesktopBootstrap,
  type SetupPlanPreview,
  type SetupPlanStatus,
} from "@/lib/schemas"
import type {
  ProductTransport,
  SetupPlanSelection,
} from "@/lib/transport"

import { SetupChecklist } from "./setup-checklist"
import { useOwnerEvidence } from "./setup-evidence"
import {
  defaultSelection,
  PlanBuilder,
  previewExpired,
} from "./setup-plan"

type SetupPlanReceipt = z.infer<typeof setupPlanReceiptSchema>
type FlowView = "builder" | "checklist"

export function SetupFlow({
  bootstrap,
  transport,
  planStatus,
  planStatusLoading,
  planStatusError,
  onClose,
  onRefresh,
  onRefreshPlanStatus,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  planStatus: SetupPlanStatus | null
  planStatusLoading: boolean
  planStatusError: string | null
  onClose: () => void
  onRefresh: () => void
  onRefreshPlanStatus: () => Promise<SetupPlanStatus>
}) {
  const [view, setView] = React.useState<FlowView>(
    planStatus?.acceptedPlan ? "checklist" : "builder",
  )
  const [selection, setSelection] =
    React.useState<SetupPlanSelection>(defaultSelection)
  const [preview, setPreview] = React.useState<SetupPlanPreview | null>(null)
  const [previewError, setPreviewError] = React.useState<string | null>(null)
  const [previewing, setPreviewing] = React.useState(false)
  const [confirmed, setConfirmed] = React.useState(false)
  const [accepting, setAccepting] = React.useState(false)
  const [receipt, setReceipt] = React.useState<SetupPlanReceipt | null>(null)
  const initialized = React.useRef(false)

  React.useEffect(() => {
    if (!planStatus || initialized.current) return
    initialized.current = true
    if (planStatus.acceptedPlan) {
      setSelection(planStatus.acceptedPlan.plan.selection)
      setView("checklist")
    } else {
      setSelection({
        goals: ["everything_recommended"],
        starterPlan: planStatus.catalog.recommendedStarterPlan,
      })
    }
  }, [planStatus])

  const acceptedPlan = planStatus?.acceptedPlan?.plan ?? null
  const evidence = useOwnerEvidence(
    bootstrap,
    transport,
    acceptedPlan !== null && view === "checklist",
    acceptedPlan?.steps ?? null,
  )

  async function requestPreview() {
    if (!planStatus || selection.goals.length === 0) return
    setPreviewing(true)
    setPreviewError(null)
    setConfirmed(false)
    setReceipt(null)
    try {
      const result = await transport.query({
        query: "setupPlanPreview",
        expectedRevision: planStatus.currentRevision,
        selection,
      })
      setPreview(setupPlanPreviewSchema.parse(result.data))
    } catch (error) {
      setPreview(null)
      setPreviewError(messageFrom(error, "The exact setup preview could not be created."))
    } finally {
      setPreviewing(false)
    }
  }

  async function acceptPreview() {
    if (!preview || !confirmed) return
    if (previewExpired(preview)) {
      setPreviewError(
        "This immutable preview expired before acceptance. Refresh setup status and create a new preview.",
      )
      return
    }
    setAccepting(true)
    setPreviewError(null)
    try {
      const result = await transport.operationsControl(
        {
          action: "applySetupPlan",
          previewId: preview.previewId,
          previewSha256: preview.previewSha256,
        },
        true,
      )
      const acceptedReceipt = setupPlanReceiptSchema.parse(result.data)
      setReceipt(acceptedReceipt)
      setConfirmed(false)
      setPreview(null)
      await onRefreshPlanStatus()
      onRefresh()
      setView("checklist")
    } catch (error) {
      setPreviewError(
        messageFrom(
          error,
          "The setup plan was not accepted. Refresh the status and create a new preview.",
        ),
      )
    } finally {
      setAccepting(false)
    }
  }

  return (
    <section className="rounded-xl border border-border bg-card/55 p-5">
      <header className="flex items-start gap-3 border-b border-border pb-5">
        <Button type="button" variant="ghost" size="icon-sm" onClick={onClose}>
          <ArrowLeft aria-hidden="true" />
          <span className="sr-only">Return to setup overview</span>
        </Button>
        <div className="min-w-0 flex-1">
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Permanent guided setup
          </p>
          <h2 className="mt-1 text-lg font-semibold">
            {view === "builder" ? "Choose and inspect your plan" : "Finish the eleven setup steps"}
          </h2>
          <p className="mt-1 max-w-3xl text-xs leading-relaxed text-muted-foreground">
            Closing or going back never changes setup. Owner evidence is read again after refresh
            and restart; skipped or unavailable work never becomes complete.
          </p>
        </div>
        {acceptedPlan ? (
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => {
              setPreview(null)
              setConfirmed(false)
              setView(view === "builder" ? "checklist" : "builder")
            }}
          >
            {view === "builder" ? "Back to checklist" : "Change goals or plan"}
          </Button>
        ) : null}
      </header>

      {receipt ? (
        <div
          role="status"
          className="mt-4 flex items-start gap-3 rounded-lg border border-emerald-400/25 bg-emerald-400/5 p-4"
        >
          <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-emerald-400" aria-hidden="true" />
          <div>
            <p className="text-xs font-medium">Setup plan accepted</p>
            <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
              Revision {receipt.revision} was recorded. This receipt proves the chosen plan only;
              every capability below still requires its own owner evidence.
            </p>
          </div>
        </div>
      ) : null}

      {planStatusLoading && !planStatus ? (
        <SetupLoading label="Reading setup catalog and accepted plan…" />
      ) : planStatusError && !planStatus ? (
        <SetupUnavailable
          title="Setup authority is unavailable"
          detail={`${planStatusError} No plan is assumed and no capability is marked complete.`}
          action={
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => void onRefreshPlanStatus().catch(() => undefined)}
            >
              <RefreshCw aria-hidden="true" />
              Retry setup status
            </Button>
          }
        />
      ) : view === "builder" ? (
        <PlanBuilder
          status={planStatus}
          selection={selection}
          onSelectionChange={(next) => {
            setSelection(next)
            setPreview(null)
            setConfirmed(false)
            setPreviewError(null)
          }}
          preview={preview}
          previewing={previewing}
          previewError={previewError ?? planStatusError}
          confirmed={confirmed}
          accepting={accepting}
          onConfirmedChange={setConfirmed}
          onPreview={() => void requestPreview()}
          onAccept={() => void acceptPreview()}
          onDiscardPreview={() => {
            setPreview(null)
            setConfirmed(false)
            setPreviewError(null)
          }}
          onRefreshStatus={() => void onRefreshPlanStatus().catch(() => undefined)}
        />
      ) : acceptedPlan ? (
        <SetupChecklist
          bootstrap={bootstrap}
          transport={transport}
          steps={acceptedPlan.steps}
          selection={acceptedPlan.selection}
          evidence={evidence.map}
          refreshing={evidence.refreshing}
          refreshError={evidence.error}
          onRefresh={() => {
            onRefresh()
            void evidence.refresh()
          }}
        />
      ) : (
        <SetupUnavailable
          title="Accept a plan before starting the checklist"
          detail="Preview the exact recommended plan, review its contacts and local changes, then confirm acceptance explicitly."
          action={
            <Button type="button" size="sm" onClick={() => setView("builder")}>
              Review plan
            </Button>
          }
        />
      )}
    </section>
  )
}

function SetupLoading({ label }: { label: string }) {
  return (
    <div className="grid min-h-64 place-items-center" aria-live="polite">
      <p className="flex items-center gap-2 text-xs text-muted-foreground">
        <RefreshCw className="size-3.5 animate-spin" aria-hidden="true" />
        {label}
      </p>
    </div>
  )
}

function SetupUnavailable({
  title,
  detail,
  action,
}: {
  title: string
  detail: string
  action: React.ReactNode
}) {
  return (
    <div className="mx-auto my-8 max-w-2xl rounded-lg border border-amber-400/25 bg-amber-400/5 p-5">
      <div className="flex items-start gap-3">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden="true" />
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
          <div className="mt-4">{action}</div>
        </div>
      </div>
    </div>
  )
}


function messageFrom(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback
}
