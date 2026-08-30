import * as React from "react"
import { BadgeCheck, CircleAlert, KeyRound, Landmark, ShieldCheck } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { humanize } from "@/lib/formatters"

import type {
  FairValueApproval,
  FairValueClassification,
  FairValueGovernanceProposal,
  FairValueInput,
  FairValueMeasurement,
  GovernanceActionPreview,
  GovernanceAuthorization,
  GovernancePrincipal,
} from "./fair-value-contracts"

const MAX_RATIONALE_LENGTH = 4_096

interface GovernanceRequestState {
  preview: GovernanceActionPreview | null
  authorizations: GovernanceAuthorization[]
  busy: boolean
  error: string | null
}

/**
 * A guided UI over the service-owned governance ceremony. It never accepts actor names, role
 * claims, client timestamps, audit IDs, or serialized action JSON. The caller constructs only a
 * bounded business proposal; the service canonicalizes it, derives authority from authenticated
 * principals, and issues opaque native-held authorization handles.
 */
export function FairValueGovernanceWorkflow({
  measurement,
  classification,
  inputs,
  approvals,
  principals,
  state,
  onPreview,
  onAuthenticate,
  onCommit,
}: {
  measurement: FairValueMeasurement
  classification: FairValueClassification | undefined
  inputs: FairValueInput[] | undefined
  approvals: FairValueApproval[] | undefined
  principals: GovernancePrincipal[] | undefined
  state: GovernanceRequestState
  onPreview: (proposal: FairValueGovernanceProposal) => void
  onAuthenticate: (principalId: string, credential: string) => void
  onCommit: () => void
}) {
  const [kind, setKind] = React.useState<ProposalKind>("approve")
  const [expiry, setExpiry] = React.useState("")
  const [requestedHierarchy, setRequestedHierarchy] = React.useState<"level_2" | "level_3">(
    "level_2",
  )
  const [justification, setJustification] = React.useState("")
  const [approvalToken, setApprovalToken] = React.useState("")
  const [reason, setReason] = React.useState("")
  const [marketInputToken, setMarketInputToken] = React.useState("")
  const [conclusion, setConclusion] = React.useState<"accessible" | "inaccessible">(
    "accessible",
  )
  const [effectiveFrom, setEffectiveFrom] = React.useState("")
  const [effectiveUntil, setEffectiveUntil] = React.useState("")
  const [rationale, setRationale] = React.useState("")
  const [selectedPrincipalId, setSelectedPrincipalId] = React.useState("")
  const [credential, setCredential] = React.useState("")

  const marketInputs = React.useMemo(
    () =>
      (inputs ?? []).filter((input) => input.marketInputToken !== null),
    [inputs],
  )
  const activeApprovals = (approvals ?? []).filter(
    (approval) => approval.status !== "revoked",
  )
  const selectedMarketInput = marketInputs.find(
    (input) => input.marketInputToken === marketInputToken,
  )
  const selectedPrincipal = principals?.find(
    (principal) => principal.principalId === selectedPrincipalId,
  )
  const authorizedPrincipalIds = new Set(
    state.authorizations.map((authorization) => authorization.principalId),
  )
  const needsMoreAuthorizations =
    state.preview !== null &&
    authorizedPrincipalIds.size < state.preview.distinctPrincipalCount
  const canCommit =
    state.preview !== null &&
    !needsMoreAuthorizations &&
    state.authorizations.every(
      (authorization) => authorization.previewId === state.preview?.previewId,
    )

  React.useEffect(() => {
    setCredential("")
    setSelectedPrincipalId("")
  }, [state.preview?.previewId])

  const submitProposal = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const proposal = proposalFor({
      kind,
      measurement,
      classification,
      expiry,
      requestedHierarchy,
      justification,
      approvalToken,
      reason,
      selectedMarketInput,
      conclusion,
      effectiveFrom,
      effectiveUntil,
      rationale,
    })
    if (proposal) onPreview(proposal)
  }

  const authenticate = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!selectedPrincipal || credential.length === 0 || state.busy) return
    onAuthenticate(selectedPrincipal.principalId, credential)
    setCredential("")
  }

  return (
    <section
      aria-labelledby="fair-value-governance-heading"
      className="rounded-xl border border-primary/25 bg-primary/[0.035] p-4"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            Guided governance
          </p>
          <h3 id="fair-value-governance-heading" className="mt-1 text-base font-semibold">
            Review, authorize, and record a decision
          </h3>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Market Squawk ties this review to the saved measurement, its classification, and an
            authenticated reviewer. Approval authority cannot be self-declared.
          </p>
        </div>
        <div className="rounded-lg border border-primary/20 bg-background/45 px-3 py-2 text-[10px] leading-4 text-muted-foreground">
          <p>{classification ? humanize(classification.hierarchy) : "Classification needed"}</p>
          <p>{measurement.inputCount.toLocaleString()} supporting input{measurement.inputCount === 1 ? "" : "s"}</p>
        </div>
      </div>

      {!classification ? (
        <Alert className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Classification evidence is required</AlertTitle>
          <AlertDescription>
            Load or evaluate the saved measurement before proposing an approval or override.
            Governance cannot infer a decision from a valuation method or market-depth label alone.
          </AlertDescription>
        </Alert>
      ) : null}

      <form className="mt-4 grid gap-4" onSubmit={submitProposal}>
        <fieldset disabled={state.busy}>
          <legend className="text-xs font-medium">1. Prepare a proposal</legend>
          <div className="mt-2 grid gap-3 lg:grid-cols-2">
            <Field label="Governance action" htmlFor="fair-value-governance-action">
              <select
                id="fair-value-governance-action"
                value={kind}
                onChange={(event) => setKind(event.target.value as ProposalKind)}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="approve">Approve current classification</option>
                <option value="override">Propose Level 2 / Level 3 override</option>
                <option value="revoke">Revoke approval</option>
                <option value="market_access">Assess market access</option>
              </select>
            </Field>
            <ProposalBoundary kind={kind} />
          </div>
          {kind === "approve" || kind === "override" ? (
            <ClassificationProposalFields
              kind={kind}
              classification={classification}
              expiry={expiry}
              requestedHierarchy={requestedHierarchy}
              justification={justification}
              onExpiry={setExpiry}
              onRequestedHierarchy={setRequestedHierarchy}
              onJustification={setJustification}
            />
          ) : null}
          {kind === "revoke" ? (
            <RevocationFields
              approvals={activeApprovals}
              approvalToken={approvalToken}
              reason={reason}
              onApprovalToken={setApprovalToken}
              onReason={setReason}
            />
          ) : null}
          {kind === "market_access" ? (
            <MarketAccessFields
              inputs={marketInputs}
              selectedInputToken={marketInputToken}
              conclusion={conclusion}
              effectiveFrom={effectiveFrom}
              effectiveUntil={effectiveUntil}
              rationale={rationale}
              onInputToken={setMarketInputToken}
              onConclusion={setConclusion}
              onEffectiveFrom={setEffectiveFrom}
              onEffectiveUntil={setEffectiveUntil}
              onRationale={setRationale}
            />
          ) : null}
        </fieldset>
        <div>
          <Button
            type="submit"
            disabled={
              state.busy ||
              !canPreview({
                kind,
                classification,
                expiry,
                justification,
                approvalToken,
                reason,
                selectedMarketInput,
                effectiveFrom,
                effectiveUntil,
                rationale,
              })
            }
          >
            <FilePreviewIcon kind={kind} />
            Review proposed action
          </Button>
        </div>
      </form>

      {state.error ? (
        <Alert variant="destructive" className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Governance action was not accepted</AlertTitle>
          <AlertDescription>{state.error}</AlertDescription>
        </Alert>
      ) : null}

      {state.preview ? (
        <div className="mt-5 border-t border-primary/20 pt-4">
          <PreviewReview preview={state.preview} />
          <form className="mt-4 grid gap-3" onSubmit={authenticate}>
            <Field label="2. Authenticate an eligible principal" htmlFor="fair-value-governance-principal">
              <select
                id="fair-value-governance-principal"
                value={selectedPrincipalId}
                onChange={(event) => {
                  setSelectedPrincipalId(event.target.value)
                }}
                disabled={state.busy || principals === undefined}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="">
                  {principals === undefined ? "Loading admitted principals…" : "Select an eligible principal"}
                </option>
                {(principals ?? [])
                  .filter(
                    (principal) =>
                      state.preview?.eligiblePrincipalIds.includes(principal.principalId) &&
                      !authorizedPrincipalIds.has(principal.principalId),
                  )
                  .map((principal) => (
                    <option key={principal.principalId} value={principal.principalId}>
                      {principal.displayName} · {principal.roles.map(humanize).join(", ")}
                    </option>
                  ))}
              </select>
            </Field>
            <Field label="Reauthentication credential" htmlFor="fair-value-governance-credential">
              <Input
                id="fair-value-governance-credential"
                type="password"
                value={credential}
                onChange={(event) => setCredential(event.target.value)}
                autoComplete="current-password"
                placeholder="Used once; never stored or shown"
                disabled={state.busy || !selectedPrincipal}
              />
              <FieldMessage>
                The credential is used once for this authorization and then cleared from the form.
              </FieldMessage>
            </Field>
            <div>
              <Button
                type="submit"
                variant="outline"
                disabled={state.busy || !selectedPrincipal || credential.length === 0}
              >
                <KeyRound aria-hidden="true" />
                Authenticate selected principal
              </Button>
            </div>
          </form>
          <AuthorizationProgress preview={state.preview} authorizations={state.authorizations} />
          <div className="mt-4 flex flex-wrap gap-2">
            <Button type="button" disabled={state.busy || !canCommit} onClick={onCommit}>
              <BadgeCheck aria-hidden="true" />
              Record action
            </Button>
            {needsMoreAuthorizations ? (
              <p className="self-center text-[10px] text-muted-foreground">
                {state.preview.distinctPrincipalCount - authorizedPrincipalIds.size} distinct
                authentication{state.preview.distinctPrincipalCount - authorizedPrincipalIds.size === 1 ? "" : "s"} still required.
              </p>
            ) : null}
          </div>
        </div>
      ) : null}
    </section>
  )
}

type ProposalKind = FairValueGovernanceProposal["kind"]

function ClassificationProposalFields({
  kind,
  classification,
  expiry,
  requestedHierarchy,
  justification,
  onExpiry,
  onRequestedHierarchy,
  onJustification,
}: {
  kind: "approve" | "override"
  classification: FairValueClassification | undefined
  expiry: string
  requestedHierarchy: "level_2" | "level_3"
  justification: string
  onExpiry: (value: string) => void
  onRequestedHierarchy: (value: "level_2" | "level_3") => void
  onJustification: (value: string) => void
}) {
  return (
    <div className="mt-3 grid gap-3 lg:grid-cols-2">
      <Field label="Current classification" htmlFor="fair-value-governance-decision">
        <Input
          id="fair-value-governance-decision"
          readOnly
          value={classification ? humanize(classification.hierarchy) : "Classification not loaded"}
        />
      </Field>
      <Field label="Approval / override expiry" htmlFor="fair-value-governance-expiry">
        <Input
          id="fair-value-governance-expiry"
          type="datetime-local"
          value={expiry}
          onChange={(event) => onExpiry(event.target.value)}
          required
        />
      </Field>
      {kind === "override" ? (
        <>
          <Field label="Requested hierarchy" htmlFor="fair-value-governance-hierarchy">
            <select
              id="fair-value-governance-hierarchy"
              value={requestedHierarchy}
              onChange={(event) =>
                onRequestedHierarchy(event.target.value as "level_2" | "level_3")
              }
              className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="level_2">Level 2</option>
              <option value="level_3">Level 3</option>
            </select>
            <FieldMessage>Overrides cannot create or promote a Level 1 classification.</FieldMessage>
          </Field>
          <Field label="Override justification" htmlFor="fair-value-governance-justification">
            <textarea
              id="fair-value-governance-justification"
              value={justification}
              onChange={(event) => onJustification(event.target.value)}
              maxLength={MAX_RATIONALE_LENGTH}
              required
              className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            />
          </Field>
        </>
      ) : null}
    </div>
  )
}

function RevocationFields({
  approvals,
  approvalToken,
  reason,
  onApprovalToken,
  onReason,
}: {
  approvals: FairValueApproval[]
  approvalToken: string
  reason: string
  onApprovalToken: (value: string) => void
  onReason: (value: string) => void
}) {
  return (
    <div className="mt-3 grid gap-3 lg:grid-cols-2">
      <Field label="Approval to revoke" htmlFor="fair-value-governance-approval">
        <select
          id="fair-value-governance-approval"
          value={approvalToken}
          onChange={(event) => onApprovalToken(event.target.value)}
          required
          className="h-9 w-full rounded-md border border-input bg-background px-3 font-mono text-[10px] outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <option value="">Select an approval</option>
          {approvals.map((approval) => (
            <option key={approval.approvalToken} value={approval.approvalToken}>
              {approval.approvedBy} · {humanize(approval.status)} · {formatDate(approval.approvedAt)}
            </option>
          ))}
        </select>
      </Field>
      <Field label="Revocation reason" htmlFor="fair-value-governance-reason">
        <textarea
          id="fair-value-governance-reason"
          value={reason}
          onChange={(event) => onReason(event.target.value)}
          maxLength={MAX_RATIONALE_LENGTH}
          required
          className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        />
      </Field>
    </div>
  )
}

function MarketAccessFields({
  inputs,
  selectedInputToken,
  conclusion,
  effectiveFrom,
  effectiveUntil,
  rationale,
  onInputToken,
  onConclusion,
  onEffectiveFrom,
  onEffectiveUntil,
  onRationale,
}: {
  inputs: FairValueInput[]
  selectedInputToken: string
  conclusion: "accessible" | "inaccessible"
  effectiveFrom: string
  effectiveUntil: string
  rationale: string
  onInputToken: (value: string) => void
  onConclusion: (value: "accessible" | "inaccessible") => void
  onEffectiveFrom: (value: string) => void
  onEffectiveUntil: (value: string) => void
  onRationale: (value: string) => void
}) {
  return (
    <div className="mt-3 grid gap-3 lg:grid-cols-2">
      <Field label="Market input" htmlFor="fair-value-governance-market-input">
        <select
          id="fair-value-governance-market-input"
          value={selectedInputToken}
          onChange={(event) => onInputToken(event.target.value)}
          required
          className="h-9 w-full rounded-md border border-input bg-background px-3 font-mono text-[10px] outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <option value="">Select a current market input</option>
          {inputs.map((input) => (
            <option key={input.marketInputToken} value={input.marketInputToken ?? ""}>
              {input.evidence.label} · {humanize(input.significance)}
            </option>
          ))}
        </select>
        <FieldMessage>
          Market accessibility is a business assessment and is not inferred from data availability.
        </FieldMessage>
      </Field>
      <Field label="Accessibility conclusion" htmlFor="fair-value-governance-access">
        <select
          id="fair-value-governance-access"
          value={conclusion}
          onChange={(event) => onConclusion(event.target.value as "accessible" | "inaccessible")}
          className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <option value="accessible">Accessible</option>
          <option value="inaccessible">Inaccessible</option>
        </select>
      </Field>
      <Field label="Effective from" htmlFor="fair-value-governance-effective-from">
        <Input
          id="fair-value-governance-effective-from"
          type="datetime-local"
          value={effectiveFrom}
          onChange={(event) => onEffectiveFrom(event.target.value)}
          required
        />
      </Field>
      <Field label="Effective until" htmlFor="fair-value-governance-effective-until">
        <Input
          id="fair-value-governance-effective-until"
          type="datetime-local"
          value={effectiveUntil}
          onChange={(event) => onEffectiveUntil(event.target.value)}
          required
        />
      </Field>
      <Field label="Assessment rationale" htmlFor="fair-value-governance-rationale">
        <textarea
          id="fair-value-governance-rationale"
          value={rationale}
          onChange={(event) => onRationale(event.target.value)}
          maxLength={MAX_RATIONALE_LENGTH}
          required
          className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        />
      </Field>
    </div>
  )
}

function PreviewReview({ preview }: { preview: GovernanceActionPreview }) {
  return (
    <div className="rounded-lg border border-primary/25 bg-background/45 p-3">
      <div className="flex items-center gap-2">
        <Landmark className="size-4 text-primary" aria-hidden="true" />
        <h4 className="text-sm font-semibold">2. Review proposed action</h4>
      </div>
      <dl className="mt-3 grid gap-3 text-xs md:grid-cols-2">
        <Fact label="Required roles" value={preview.requiredRoles.map(humanize).join(", ")} />
        <Fact label="Reviewers required" value={String(preview.distinctPrincipalCount)} />
        <Fact label="Review expires" value={formatDate(preview.expiresAt)} />
      </dl>
      <ul className="mt-3 space-y-1 border-t border-border pt-3 text-[11px] text-muted-foreground">
        {preview.effects.map((effect) => (
          <li key={effect.kind}>• {humanize(effect.kind)}</li>
        ))}
      </ul>
    </div>
  )
}

function AuthorizationProgress({
  preview,
  authorizations,
}: {
  preview: GovernanceActionPreview
  authorizations: GovernanceAuthorization[]
}) {
  const distinct = new Map(
    authorizations
      .filter((authorization) => authorization.previewId === preview.previewId)
      .map((authorization) => [authorization.principalId, authorization]),
  )
  return (
    <div className="mt-4 rounded-lg border border-border bg-background/35 p-3">
      <div className="flex items-center gap-2">
        <ShieldCheck className="size-4 text-primary" aria-hidden="true" />
        <p className="text-xs font-semibold">One-use authorization progress</p>
      </div>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
        {distinct.size} of {preview.distinctPrincipalCount} required reviewer
        {preview.distinctPrincipalCount === 1 ? "" : "s"} have reauthenticated this proposal.
      </p>
      {distinct.size > 0 ? (
        <ul className="mt-2 space-y-1 text-[9px] text-muted-foreground">
          {[...distinct.values()].map((authorization, index) => (
            <li key={authorization.principalId}>
              Reviewer {index + 1} authorized · expires {formatDate(authorization.expiresAt)}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  )
}

function ProposalBoundary({ kind }: { kind: ProposalKind }) {
  const text =
    kind === "approve"
      ? "Approves the current saved classification without changing its supporting information."
      : kind === "override"
        ? "Creates a separate expiring Level 2 or Level 3 judgment; it cannot promote to Level 1."
        : kind === "revoke"
          ? "Records a revocation while preserving the original approval history."
          : "Records reporting-entity market access; it does not claim trading-data or execution quality."
  return <p className="rounded-md border border-border bg-background/35 p-3 text-[10px] leading-4 text-muted-foreground">{text}</p>
}

function Field({
  label,
  htmlFor,
  children,
}: {
  label: string
  htmlFor: string
  children: React.ReactNode
}) {
  return (
    <label htmlFor={htmlFor} className="block text-xs font-medium">
      {label}
      <div className="mt-1.5">{children}</div>
    </label>
  )
}

function FieldMessage({ children }: { children: React.ReactNode }) {
  return <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{children}</p>
}

function Fact({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div>
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs">{value}</dd>
    </div>
  )
}

function FilePreviewIcon({ kind }: { kind: ProposalKind }) {
  return kind === "market_access" ? <ShieldCheck aria-hidden="true" /> : <Landmark aria-hidden="true" />
}

function canPreview({
  kind,
  classification,
  expiry,
  justification,
  approvalToken,
  reason,
  selectedMarketInput,
  effectiveFrom,
  effectiveUntil,
  rationale,
}: {
  kind: ProposalKind
  classification: FairValueClassification | undefined
  expiry: string
  justification: string
  approvalToken: string
  reason: string
  selectedMarketInput: FairValueInput | undefined
  effectiveFrom: string
  effectiveUntil: string
  rationale: string
}) {
  if (kind === "approve") return classification !== undefined && validDateTime(expiry)
  if (kind === "override") {
    return classification !== undefined && validDateTime(expiry) && validText(justification)
  }
  if (kind === "revoke") return approvalToken.length > 0 && validText(reason)
  return (
    selectedMarketInput !== undefined &&
    validDateTime(effectiveFrom) &&
    validDateTime(effectiveUntil) &&
    toIso(effectiveUntil)! > toIso(effectiveFrom)! &&
    validText(rationale)
  )
}

function proposalFor({
  kind,
  measurement,
  classification,
  expiry,
  requestedHierarchy,
  justification,
  approvalToken,
  reason,
  selectedMarketInput,
  conclusion,
  effectiveFrom,
  effectiveUntil,
  rationale,
}: {
  kind: ProposalKind
  measurement: FairValueMeasurement
  classification: FairValueClassification | undefined
  expiry: string
  requestedHierarchy: "level_2" | "level_3"
  justification: string
  approvalToken: string
  reason: string
  selectedMarketInput: FairValueInput | undefined
  conclusion: "accessible" | "inaccessible"
  effectiveFrom: string
  effectiveUntil: string
  rationale: string
}): FairValueGovernanceProposal | null {
  if (kind === "approve" && classification && validDateTime(expiry)) {
    return {
      kind,
      measurementToken: measurement.measurementToken,
      classificationToken: classification.classificationToken,
      expiresAt: toIso(expiry)!,
    }
  }
  if (kind === "override" && classification && validDateTime(expiry) && validText(justification)) {
    return {
      kind,
      measurementToken: measurement.measurementToken,
      classificationToken: classification.classificationToken,
      requestedHierarchy,
      justification: justification.trim(),
      expiresAt: toIso(expiry)!,
    }
  }
  if (kind === "revoke" && approvalToken && validText(reason)) {
    return { kind, approvalToken, reason: reason.trim() }
  }
  const selectedMarketInputToken = selectedMarketInput?.marketInputToken
  if (
    kind === "market_access" &&
    selectedMarketInputToken &&
    validDateTime(effectiveFrom) &&
    validDateTime(effectiveUntil) &&
    toIso(effectiveUntil)! > toIso(effectiveFrom)! &&
    validText(rationale)
  ) {
    return {
      kind,
      marketInputToken: selectedMarketInputToken,
      conclusion,
      effectiveFrom: toIso(effectiveFrom)!,
      effectiveUntil: toIso(effectiveUntil)!,
      rationale: rationale.trim(),
    }
  }
  return null
}

function validText(value: string) {
  const normalized = value.trim()
  return normalized.length > 0 && normalized.length <= MAX_RATIONALE_LENGTH && !/[\u0000-\u001F\u007F]/.test(normalized)
}

function validDateTime(value: string) {
  return toIso(value) !== null
}

function toIso(value: string) {
  const parsed = new Date(value)
  return Number.isFinite(parsed.valueOf()) ? parsed.toISOString() : null
}

function formatDate(value: string) {
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(parsed) : value
}
