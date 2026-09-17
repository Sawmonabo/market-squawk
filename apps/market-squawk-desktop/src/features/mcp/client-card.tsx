import {
  Bot,
  CheckCircle2,
  CircleAlert,
  KeyRound,
  Laptop,
  ShieldAlert,
} from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

import {
  actionLabel,
  availableActions,
  formatObservedAt,
  statePresentation,
  type McpClientAction,
  type McpClientView,
} from "./contracts"

export function ClientCard({
  client,
  disabled,
  onAction,
}: {
  client: McpClientView
  disabled: boolean
  onAction: (action: McpClientAction) => void
}) {
  const state = statePresentation(client.state)
  const actions = availableActions(client)
  const Icon = client.client === "claude_code" ? Bot : Laptop

  return (
    <article className="rounded-xl border border-border bg-card/45 p-5">
      <header className="flex items-start justify-between gap-4">
        <div className="flex min-w-0 items-start gap-3">
          <span className="rounded-lg border border-border bg-background p-2">
            <Icon className="size-5 text-primary" aria-hidden="true" />
          </span>
          <div className="min-w-0">
            <h2 className="text-lg font-semibold">{client.label}</h2>
            <p className="mt-1 truncate font-mono text-[10px] text-muted-foreground">
              {client.clientVersion ?? "Version unavailable"}
            </p>
          </div>
        </div>
        <StateBadge label={state.label} tone={state.tone} />
      </header>

      <p className="mt-4 text-xs leading-relaxed text-muted-foreground">
        {state.detail}
      </p>

      {client.blocker ? (
        <Alert variant="destructive" className="mt-4">
          <ShieldAlert aria-hidden="true" />
          <AlertTitle>Connection blocked</AlertTitle>
          <AlertDescription>{client.blocker}</AlertDescription>
        </Alert>
      ) : null}

      {client.service.priorCredentialCleanupPending ? (
        <Alert className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Credential cleanup pending</AlertTitle>
          <AlertDescription>
            The new generation is authoritative. Market Squawk will retry removal of the retired
            protected generation during service recovery.
          </AlertDescription>
        </Alert>
      ) : null}

      {client.service.credentialRotationRecoveryPending &&
      !client.service.priorCredentialCleanupPending ? (
        <Alert variant="destructive" className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Credential recovery required</AlertTitle>
          <AlertDescription>
            A protected credential change was interrupted before activation. Restart Market Squawk
            to reconcile the recorded replacement safely.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        <EvidenceCard
          icon={KeyRound}
          label="Credential & receipt"
          headline={client.receipt ? "Owned receipt present" : "No owned receipt"}
          detail={
            client.receipt
              ? "The client credential remains protected by native secret storage and is not exposed here."
              : "No Market Squawk-owned client credential is active."
          }
        >
          {client.receipt ? (
            <dl className="mt-3 space-y-2 border-t border-border pt-3 text-[11px]">
              <EvidenceRow
                label="Observed"
                value={formatObservedAt(client.receipt.observedAtUnixSeconds)}
              />
              <EvidenceRow label="Command SHA-256" value={client.receipt.commandSha256} mono />
            </dl>
          ) : null}
          <dl className="mt-3 space-y-2 border-t border-border pt-3 text-[11px]">
            <EvidenceRow
              label="Service access"
              value={client.service.accessRevoked ? "Revoked" : "Active"}
            />
            <EvidenceRow
              label="Credential generation"
              value={String(client.service.credentialGeneration)}
            />
            <EvidenceRow
              label="Client identity"
              value={shortIdentity(client.service.clientId)}
              mono
            />
          </dl>
        </EvidenceCard>

        <EvidenceCard
          icon={client.verification ? CheckCircle2 : CircleAlert}
          label="Protocol verification"
          headline={client.verification ? "Safe read verified" : "Not yet verified"}
          detail={
            client.verification
              ? `Verified ${formatObservedAt(client.verification.verifiedAtUnixSeconds)}`
              : "Run verification after connecting to prove the real protocol path."
          }
        >
          {client.verification ? (
            <dl className="mt-3 space-y-2 border-t border-border pt-3 text-[11px]">
              <EvidenceRow label="Protocol" value={client.verification.protocolVersion} />
              <EvidenceRow label="Server" value={client.verification.serverName} />
              <EvidenceRow label="Safe read" value={client.verification.safeReadTool} mono />
              <EvidenceRow
                label="Surface"
                value={`${client.verification.toolCount} tools · ${client.verification.resourceCount} resources`}
              />
              <EvidenceRow
                label="Tool domains"
                value={client.verification.toolDomains.join(", ")}
              />
              <EvidenceRow
                label="Resources"
                value={client.verification.resourceNames.join(", ")}
              />
              <EvidenceRow
                label="Session identity"
                value={client.verification.clientInfoName}
                mono
              />
            </dl>
          ) : null}
          <dl className="mt-3 space-y-2 border-t border-border pt-3 text-[11px]">
            <EvidenceRow
              label="Active requests"
              value={`${client.service.activeRequests} of ${client.service.maximumActiveRequests}`}
            />
            <EvidenceRow
              label="Admitted / limited"
              value={`${client.service.admittedRequests} / ${client.service.rateLimitedRequests}`}
            />
            <EvidenceRow
              label="Observed relay starts"
              value={String(client.service.observedRelayInitializations)}
            />
            <EvidenceRow
              label="Last activity"
              value={
                client.service.lastActivityUnixSeconds === null
                  ? "No requests observed"
                  : formatObservedAt(client.service.lastActivityUnixSeconds)
              }
            />
          </dl>
        </EvidenceCard>
      </div>

      <div className="mt-5 flex flex-wrap gap-2 border-t border-border pt-4">
        {actions.map((action) => (
          <Button
            key={action}
            size="sm"
            variant={
              action === "disconnect" || action === "revokeCredential"
                ? "destructive"
                : "outline"
            }
            disabled={disabled}
            onClick={() => onAction(action)}
          >
            {actionLabel(action)}
          </Button>
        ))}
        {actions.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            {noActionMessage(client)}
          </p>
        ) : null}
      </div>
    </article>
  )
}

function noActionMessage(client: McpClientView) {
  if (client.service.credentialRotationRecoveryPending) {
    return "Restart Market Squawk to finish protected credential recovery before changing this connection."
  }
  if (client.state === "conflict") {
    return "Market Squawk will not replace this unowned entry. Resolve the named entry in the client, then refresh."
  }
  if (client.state === "absent") {
    return `Install ${client.label} to connect it to the shared service.`
  }
  return "Update the client to a supported version, then refresh discovery."
}

function shortIdentity(value: string) {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}

function EvidenceCard({
  icon: Icon,
  label,
  headline,
  detail,
  children,
}: {
  icon: typeof KeyRound
  label: string
  headline: string
  detail: string
  children?: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-border bg-background/40 p-4">
      <p className="flex items-center gap-2 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        {label}
      </p>
      <p className="mt-3 text-sm font-medium">{headline}</p>
      <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">{detail}</p>
      {children}
    </section>
  )
}

function EvidenceRow({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="grid gap-1">
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className={cn("break-all text-foreground/85", mono && "font-mono")}>{value}</dd>
    </div>
  )
}

function StateBadge({
  label,
  tone,
}: {
  label: string
  tone: "ready" | "attention" | "muted"
}) {
  return (
    <span
      className={cn(
        "shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-medium",
        tone === "ready" &&
          "border-[color-mix(in_oklab,var(--success)_45%,transparent)] bg-[color-mix(in_oklab,var(--success)_10%,transparent)] text-[var(--success)]",
        tone === "attention" && "border-amber-500/35 bg-amber-500/10 text-amber-300",
        tone === "muted" && "border-border bg-muted text-muted-foreground",
      )}
    >
      {label}
    </span>
  )
}
