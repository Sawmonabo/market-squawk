import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import { CheckCircle2, KeyRound, RefreshCw, ShieldCheck, TriangleAlert } from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseGovernanceAuthorization,
  parseGovernancePreview,
  parseGovernancePrincipals,
  parseGovernanceReceipt,
  type GovernanceAuthorizationView,
  type GovernancePreviewView,
  type GovernanceReceiptView,
} from "./contracts"
import { StateLabel } from "./decision-boundaries"

type ReviewDisposition = "activate" | "reject" | "needs_changes"
type InvalidationKind =
  | "corporate_action"
  | "model"
  | "data"
  | "reference_mark"
  | "assumption"

type DecisionGovernanceProposal =
  | {
      kind: "review"
      targetId: string
      targetRevision: number
      disposition: ReviewDisposition
      note: string
    }
  | {
      kind: "invalidation"
      targetId: string
      targetRevision: number
      invalidationKind: InvalidationKind
      note: string
    }

export function TargetGovernanceWorkflow({
  transport,
  scope,
  targetId,
  targetRevision,
  onCommitted,
}: {
  transport: ProductTransport
  scope: ProductScope
  targetId: string
  targetRevision: number
  onCommitted: () => void
}) {
  const [action, setAction] = React.useState<"review" | "invalidation">("review")
  const [disposition, setDisposition] = React.useState<ReviewDisposition>("activate")
  const [invalidationKind, setInvalidationKind] =
    React.useState<InvalidationKind>("assumption")
  const [note, setNote] = React.useState("")
  const [preview, setPreview] = React.useState<GovernancePreviewView | null>(null)
  const [principalId, setPrincipalId] = React.useState("")
  const [credential, setCredential] = React.useState("")
  const [authorizations, setAuthorizations] = React.useState<GovernanceAuthorizationView[]>([])
  const [receipt, setReceipt] = React.useState<GovernanceReceiptView | null>(null)

  const principals = useQuery({
    queryKey: productKeys.operation(scope, "governance", "principals", {}),
    queryFn: async () =>
      parseGovernancePrincipals(
        await transport.governanceQuery({ query: "principals" }),
      ),
  })
  const previewAction = useMutation({
    mutationFn: async (proposal: DecisionGovernanceProposal) =>
      parseGovernancePreview(
        await transport.decisionControl(
          {
            action: "previewGovernanceAction",
            proposal,
          },
          true,
        ),
      ),
    onSuccess: (value) => {
      setPreview(value)
      setAuthorizations([])
      setPrincipalId("")
      setCredential("")
      setReceipt(null)
    },
  })
  const authenticate = useMutation({
    mutationFn: async (input: { previewId: string; principalId: string; credential: string }) =>
      parseGovernanceAuthorization(
        await transport.governanceControl(
          {
            action: "authenticateAction",
            ...input,
          },
          true,
        ),
      ),
    onSuccess: (value) => {
      setAuthorizations((current) => [
        ...current.filter((entry) => entry.principalId !== value.principalId),
        value,
      ])
      setPrincipalId("")
    },
    onSettled: () => setCredential(""),
  })
  const commit = useMutation({
    mutationFn: async (input: { previewId: string; authorizationHandles: string[] }) =>
      parseGovernanceReceipt(
        await transport.decisionControl(
          {
            action: "commitGovernanceAction",
            ...input,
          },
          true,
        ),
      ),
    onSuccess: (value) => {
      setReceipt(value)
      onCommitted()
    },
  })

  const eligiblePrincipals = (principals.data ?? []).filter(
    (principal) =>
      preview?.eligiblePrincipalIds.includes(principal.principalId) &&
      !authorizations.some(
        (authorization) => authorization.principalId === principal.principalId,
      ),
  )
  const proposal: DecisionGovernanceProposal =
    action === "review"
      ? { kind: "review", targetId, targetRevision, disposition, note: note.trim() }
      : {
          kind: "invalidation",
          targetId,
          targetRevision,
          invalidationKind,
          note: note.trim(),
        }

  const resetWorkflow = () => {
    setPreview(null)
    setPrincipalId("")
    setCredential("")
    setAuthorizations([])
    setReceipt(null)
  }

  return (
    <section className="rounded-xl border border-primary/25 bg-primary/5 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-primary">
            Guided governance action
          </p>
          <h4 className="mt-1 text-sm font-semibold">Review or invalidate this revision</h4>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            Review the proposed action before authorization. Market Squawk records who approved it
            and when.
          </p>
        </div>
        <StateLabel value={`revision ${targetRevision}`} />
      </div>

      {!preview && (
        <div className="mt-4 grid gap-3">
          <fieldset className="grid gap-2 sm:grid-cols-2">
            <legend className="text-xs font-medium">Action</legend>
            <label className="rounded-lg border border-border bg-background/60 p-3 text-xs">
              <input
                type="radio"
                name={`decision-action-${targetId}-${targetRevision}`}
                checked={action === "review"}
                onChange={() => setAction("review")}
              />{" "}
              Review this target revision
            </label>
            <label className="rounded-lg border border-border bg-background/60 p-3 text-xs">
              <input
                type="radio"
                name={`decision-action-${targetId}-${targetRevision}`}
                checked={action === "invalidation"}
                onChange={() => setAction("invalidation")}
              />{" "}
              Mark this judgment as no longer valid
            </label>
          </fieldset>

          {action === "review" ? (
            <label className="text-xs font-medium">
              Review disposition
              <select
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm"
                value={disposition}
                onChange={(event) => setDisposition(event.target.value as ReviewDisposition)}
              >
                <option value="activate">Activate after governance review</option>
                <option value="needs_changes">Request changes</option>
                <option value="reject">Reject this revision</option>
              </select>
            </label>
          ) : (
            <label className="text-xs font-medium">
              Reason for invalidation
              <select
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm"
                value={invalidationKind}
                onChange={(event) =>
                  setInvalidationKind(event.target.value as InvalidationKind)
                }
              >
                <option value="assumption">Assumption breach</option>
                <option value="corporate_action">Corporate action</option>
                <option value="model">Model or forecast change</option>
                <option value="data">New information became available</option>
                <option value="reference_mark">Reference mark change</option>
              </select>
            </label>
          )}

          <label className="text-xs font-medium">
            Decision note
            <Input
              value={note}
              onChange={(event) => setNote(event.target.value)}
              className="mt-2"
              maxLength={4096}
              autoComplete="off"
              placeholder="State the reason for this action"
            />
          </label>
          <Button
            type="button"
            className="w-fit"
            disabled={note.trim().length === 0 || previewAction.isPending}
            onClick={() => previewAction.mutate(proposal)}
          >
            {previewAction.isPending ? <RefreshCw className="animate-spin" /> : <ShieldCheck />}
            Preview governed action
          </Button>
        </div>
      )}

      {previewAction.isError && (
        <GovernanceError
          title="Governance preview could not be prepared"
          detail="Review the proposed action and try again. Check Logs if the problem continues."
        />
      )}

      {preview && !receipt && (
        <div className="mt-4 grid gap-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <PreviewCard preview={preview} />
            <Button type="button" variant="outline" size="sm" onClick={resetWorkflow}>
              Discard preview
            </Button>
          </div>
          {principals.isPending ? (
            <p className="text-xs text-muted-foreground">Loading eligible principals…</p>
          ) : principals.isError ? (
            <GovernanceError
              title="Eligible reviewers could not be loaded"
              detail="Market Squawk could not retrieve the eligible reviewers. Check Logs for details."
            />
          ) : authorizations.length < preview.distinctPrincipalCount ? (
            <div className="grid gap-3 rounded-lg border border-border bg-background/50 p-3">
              <p className="text-xs leading-5 text-muted-foreground">
                Authenticate {preview.distinctPrincipalCount - authorizations.length} more distinct
                principal{preview.distinctPrincipalCount - authorizations.length === 1 ? "" : "s"}.
                Credentials are used only to verify this action and are cleared after each attempt.
              </p>
              <label className="text-xs font-medium">
                Eligible principal
                <select
                  className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm"
                  value={principalId}
                  onChange={(event) => setPrincipalId(event.target.value)}
                >
                  <option value="">Select an eligible principal</option>
                  {eligiblePrincipals.map((principal) => (
                    <option key={principal.principalId} value={principal.principalId}>
                      {principal.displayName} · {principal.roles.map(humanize).join(", ")}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-xs font-medium">
                Authentication credential
                <Input
                  type="password"
                  value={credential}
                  onChange={(event) => setCredential(event.target.value)}
                  className="mt-2"
                  autoComplete="current-password"
                />
              </label>
              <Button
                type="button"
                variant="outline"
                className="w-fit"
                disabled={
                  principalId.length === 0 || credential.length === 0 || authenticate.isPending
                }
                onClick={() =>
                  authenticate.mutate({ previewId: preview.previewId, principalId, credential })
                }
              >
                <KeyRound />
                Authenticate selected principal
              </Button>
              {authenticate.isError && (
                <GovernanceError
                  title="Reviewer could not be verified"
                  detail="Check the selected reviewer and credential, then try again. Detailed errors are available in Logs."
                />
              )}
            </div>
          ) : null}

          {authorizations.length > 0 && (
            <ul className="grid gap-2" aria-label="Authenticated governance principals">
              {authorizations.map((authorization) => (
                <li
                  key={authorization.authorizationHandle}
                  className="flex items-center gap-2 rounded-lg border border-border bg-background/50 px-3 py-2 text-xs"
                >
                  <CheckCircle2 className="size-4 text-primary" aria-hidden="true" />
                  {principals.data?.find(
                    (principal) => principal.principalId === authorization.principalId,
                  )?.displayName ?? "Authorized reviewer"}{" "}
                  approved this action
                </li>
              ))}
            </ul>
          )}

          {authorizations.length >= preview.distinctPrincipalCount && (
            <Button
              type="button"
              className="w-fit"
              disabled={commit.isPending}
              onClick={() =>
                commit.mutate({
                  previewId: preview.previewId,
                  authorizationHandles: authorizations.map(
                    (authorization) => authorization.authorizationHandle,
                  ),
                })
              }
            >
              {commit.isPending ? <RefreshCw className="animate-spin" /> : <ShieldCheck />}
              Record governance decision
            </Button>
          )}
          {commit.isError && (
            <GovernanceError
              title="Governance decision was not recorded"
              detail="Market Squawk could not save this decision. Try again, and check Logs if the problem continues."
            />
          )}
        </div>
      )}

      {receipt && (
        <div className="mt-4 rounded-lg border border-primary/30 bg-background/60 p-3">
          <div className="flex items-center gap-2 text-xs font-medium">
            <CheckCircle2 className="size-4 text-primary" aria-hidden="true" />
            Governance decision recorded
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {receipt.authorizedPrincipals.length} authorized reviewer
            {receipt.authorizedPrincipals.length === 1 ? "" : "s"} recorded. The resulting target
            remains research-only and cannot place an order.
          </p>
          <Button type="button" variant="outline" size="sm" className="mt-3" onClick={resetWorkflow}>
            Start another governed action
          </Button>
        </div>
      )}
    </section>
  )
}

function PreviewCard({ preview }: { preview: GovernancePreviewView }) {
  return (
    <div className="rounded-lg border border-border bg-background/50 p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs font-medium">Governance action ready for review</p>
        </div>
        <StateLabel value={`expires ${formatTimestamp(preview.expiresAt)}`} />
      </div>
      <p className="mt-3 text-xs text-muted-foreground">
        Requires {preview.distinctPrincipalCount} distinct principal
        {preview.distinctPrincipalCount === 1 ? "" : "s"} with roles {preview.requiredRoles.map(humanize).join(", ")}.
      </p>
      <ul className="mt-3 flex flex-wrap gap-2">
        {preview.effects.map((effect) => (
          <StateLabel key={effect.kind} value={effect.kind} />
        ))}
      </ul>
    </div>
  )
}

function GovernanceError({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert variant="destructive" className="mt-3">
      <TriangleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}
