import * as React from "react"
import { CheckCircle2, KeyRound, LoaderCircle } from "lucide-react"
import { MacroSelectionFields, macroSelection } from "./macro-selection-fields"
import { messageFrom } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

import type { ProviderBootstrap, ProviderProfile, ProviderSession } from "@/lib/schemas"
import type { ProviderOnboardingRequest, SystemTransport } from "@/lib/transport"
import {
  lifecycleControls,
  parseSourceLifecycleReceipt,
  type SourceEvidence,
  type LifecycleControl,
} from "./source-evidence"

type ActivationKind = ProviderBootstrap["setup"][number]["activationKind"]

export type ConnectionActivity = {
  providerId: string
  sessionId: string
  kind: "verification" | "publication"
}

type Props = {
  connections: ProviderBootstrap
  sources: SourceEvidence[]
  selectedProvider: string
  onSelect: (provider: string) => void
  transport: SystemTransport
  onChanged: () => Promise<void>
  onActivity: (activity: ConnectionActivity | null) => void
  publicationPending: boolean
}

export function ConnectionSetup({
  connections,
  sources,
  selectedProvider,
  onSelect,
  transport,
  onChanged,
  onActivity,
  publicationPending,
}: Props) {
  const profile = connections.profiles.find((profile) => profile.id === selectedProvider)
  const setup = connections.setup.find((setup) => setup.surfaceId === selectedProvider)
  const selectedSession = connections.sessions.find((session) =>
    setup?.savedConfigurationSessionId
      ? session.session_id === setup.savedConfigurationSessionId
      : session.surface_id === profile?.id,
  )
  return (
    <section className="mb-5 rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-center gap-2">
        <KeyRound className="size-4 text-primary" aria-hidden="true" />
        <h2 className="text-lg font-semibold">Set up a connection</h2>
      </div>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
        Choose a provider to continue its saved setup. Market Squawk reuses credentials
        already stored securely and checks access before using them. Public
        connections need no account or key.
      </p>
      <Label htmlFor="connection-provider" className="mt-5 block">Provider</Label>
      <select
        id="connection-provider"
        value={selectedProvider}
        onChange={(event) => onSelect(event.target.value)}
        className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
      >
        <option value="">Choose a provider</option>
        {connections.profiles.map((profile) => <option key={profile.id} value={profile.id}>
          {profile.display_name}
        </option>)}
      </select>
      {profile ? <SelectedConnection
        key={profile.id}
        profile={profile}
        session={selectedSession}
        activationKind={setup?.activationKind ?? null}
        savedConfigurationSessionId={setup?.savedConfigurationSessionId ?? null}
        source={sources.find((source) => source.id === profile.id)}
        fallback={connections.encryptedFileFallback}
        transport={transport}
        onChanged={onChanged}
        onActivity={onActivity}
        publicationPending={publicationPending}
      /> : null}
    </section>
  )
}

function SelectedConnection({
  profile,
  session,
  activationKind,
  savedConfigurationSessionId,
  source,
  fallback,
  transport,
  onChanged,
  onActivity,
  publicationPending,
}: {
  profile: ProviderProfile
  session?: ProviderSession
  activationKind: ActivationKind
  savedConfigurationSessionId: string | null
  source?: SourceEvidence
  fallback: ProviderBootstrap["encryptedFileFallback"]
  transport: SystemTransport
  onChanged: () => Promise<void>
  onActivity: (activity: ConnectionActivity | null) => void
  publicationPending: boolean
}) {
  const [pending, setPending] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [notice, setNotice] = React.useState<string | null>(null)
  const [confirmation, setConfirmation] = React.useState<"cancel" | "renew" | "unlink" | LifecycleControl | null>(null)
  const [oauth, setOAuth] = React.useState<string | null>(null)
  const [cancelling, setCancelling] = React.useState(false)
  const alive = React.useRef(true)
  React.useEffect(() => {
    alive.current = true
    return () => {
      alive.current = false
    }
  }, [])
  const noKey = profile.credential_requirement === "not_required"
  const next = session?.next_action
  const needsSecret = next === "import_secret" || next === "import_replacement"
  const canActivate = !savedConfigurationSessionId && session && activationKind && ["verify_and_activate", "verify_and_cutover"].includes(next ?? "")
  const needsContact = profile.administrative_contact_requirement === "required_non_secret"
  const controls = source ? lifecycleControls(source).filter((control) => control.action !== "remove") : []
  const active = source?.operationalState === "active"
  const saved = session?.credential_stored === true
  const canRestart = !session || (!saved && (next === "start_new_session" || next === "none"))
  const recovered = Array.isArray(session?.recovery) ? session.recovery.filter((value): value is string => typeof value === "string") : []
  const run = async (label: string, request: ProviderOnboardingRequest) => {
    setPending(label)
    setError(null)
    setNotice(null)
    const monitorsWork = "sessionId" in request && [
      "activate",
      "verifySaved",
      "restoreSaved",
      "resumePublication",
      "schwabOAuth",
    ].includes(request.action)
    if (monitorsWork && "sessionId" in request) {
      onActivity({
        providerId: profile.id,
        sessionId: request.sessionId,
        kind: "verification",
      })
    }
    let publicationPending = false
    try {
      const result = await transport.onboard(request)
      publicationPending = ["activate", "verifySaved", "resumePublication"].includes(request.action)
        && typeof result === "object"
        && result !== null
        && "publication_pending" in result
        && result.publication_pending === true
      if (monitorsWork && "sessionId" in request) {
        onActivity(publicationPending ? {
          providerId: profile.id,
          sessionId: request.sessionId,
          kind: "publication",
        } : null)
      }
      if (alive.current) {
        if (request.action === "schwabOAuth"
          && typeof result === "object"
          && result !== null
          && "state" in result
          && typeof result.state === "string") {
          setOAuth(result.state)
        }
        if (publicationPending) setNotice("Connection saved. Review data import progress below; saved progress remains available when you return.")
        else setNotice("Saved connection state refreshed.")
      }
    } catch (failure) {
      if (alive.current) setError(messageFrom(failure))
    }
    finally {
      if (monitorsWork && !publicationPending) onActivity(null)
      try {
        await onChanged()
      } catch {
        if (alive.current) setError("Saved state could not be refreshed. Use Refresh before continuing.")
      }
      if (alive.current) setPending(null)
    }
  }
  const runControl = async (control: LifecycleControl) => {
    setConfirmation(null)
    setPending(control.label)
    setError(null)
    setNotice(null)
    try {
      const receipt = parseSourceLifecycleReceipt(await transport.sourceControl(control.action, control.request, true), control.action, control.request)
      if (alive.current) {
        setNotice(receipt.disposition === "applied" || receipt.disposition === "replay"
          ? "Connection state saved."
          : "The connection needs attention. Review its refreshed state before continuing.")
      }
    } catch (failure) {
      if (alive.current) setError(messageFrom(failure))
    }
    finally {
      await onChanged().catch(() => undefined)
      if (alive.current) setPending(null)
    }
  }
  const cancel = async () => {
    if (!session) return
    setConfirmation(null)
    setCancelling(true)
    try {
      await transport.onboard({ action: "cancel", sessionId: session.session_id })
      if (alive.current) setNotice("Connection stopped. Refresh its saved state before starting again.")
    }
    catch (failure) {
      if (alive.current) setError(messageFrom(failure))
    }
    finally {
      await onChanged().catch(() => undefined)
      if (alive.current) setCancelling(false)
    }
  }
  return (
    <div className="mt-5 border-t border-border pt-5">
      <p className="text-sm leading-6 text-muted-foreground">
        {profile.coverage}
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-3">
        <Fact
          label="Credential"
          value={noKey ? "No key needed" : saved ? "Saved credential recorded" : "Not stored for this setup"}
        />
        <Fact label="Verification" value={verificationLabel(session)} />
        <Fact
          label="Connection"
          value={active ? "Connected" : source?.storedData ? "Saved data available" : "Not connected"}
        />
      </dl>
      {saved && !active ? <p className="mt-3 text-xs text-muted-foreground">
        Your saved credential will be checked through this provider's connection service. You do not need to sign up again or re-enter it.
      </p> : null}
      {noKey ? <p className="mt-3 text-xs text-muted-foreground">
        This connection uses public data. No signup or credential import is required.
      </p> : null}
      {pending ? <p role="status" className="mt-4 flex items-center gap-2 text-sm">
        <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
        {pending}
        … Saved steps are retained if this window closes.
      </p> : null}
      {publicationPending ? (
        <p role="status" className="mt-4 flex items-center gap-2 text-sm">
          <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
          Data import continues in the background. Saved progress is retained.
        </p>
      ) : null}
      {notice ? <p role="status" className="mt-4 text-sm text-emerald-300">
        {notice}
      </p> : null}
      {error ? <p role="alert" className="mt-4 text-sm text-red-400">
        {error}
      </p> : null}
      {canRestart ? <form
        className="mt-5 space-y-4"
        onSubmit={(event) => {
          event.preventDefault()
          const data = new FormData(event.currentTarget)
          void run("Preparing saved setup", {
            action: "start",
            surfaceId: profile.id,
            ...(needsContact ? { organization: field(data, "organization"), administrativeEmail: field(data, "email") } : {})
          })
        }}
      >
        {needsContact ? <div className="grid gap-3 sm:grid-cols-2">
          <Field name="organization" label="Organization or application name" maxLength={128} />
          <Field
            name="email"
            label="Contact email"
            type="email"
            maxLength={128}
          />
        </div> : null}
        <Button disabled={pending !== null || cancelling}>
          {session ? "Start a new setup" : saved ? "Continue saved setup" : "Set up this connection"}
        </Button>
      </form> : null}
      {fallback === "locked" ? <form
        className="mt-5 space-y-3"
        onSubmit={(event) => {
          event.preventDefault()
          const form = event.currentTarget
          const secret = field(new FormData(form), "unlock")
          form.reset()
          void run("Unlocking secure storage", { action: "unlockFallback", secret })
        }}
      >
        <p className="text-sm">Unlock encrypted credential storage to use saved credentials.</p>
        <Field
          name="unlock"
          label="Secure storage password"
          type="password"
          maxLength={8192}
        />
        <Button disabled={pending !== null}>Unlock storage</Button>
      </form> : null}
      {fallback === "ready" ? <Button
        className="mt-4"
        variant="outline"
        disabled={pending !== null || cancelling}
        onClick={() => void run("Locking secure storage", { action: "lockFallback" })}
      >
        Lock credential storage
      </Button> : null}
      {session && needsSecret ? <CredentialForm
        profile={profile}
        pending={pending !== null || cancelling}
        onSubmit={(secret) => run("Saving credential securely", { action: "submitSecret", sessionId: session.session_id, secret })}
      /> : null}
      {session && next === "complete_oauth_authorization" ? <div className="mt-5 space-y-3">
        <p className="text-sm">
          Authorize read-only market data on Schwab's official page, then return here. Your saved application credential stays protected.
        </p>
        {oauth ? <p role="status" className="text-xs text-muted-foreground">
          {oauth === "active"
            ? "Authorization saved. Connection verification can continue."
            : oauth === "exchanging_authorization"
              ? "Completing authorization…"
              : "Return here after completing the official authorization."}
        </p> : null}
        <div className="flex flex-wrap gap-2">
          <Button
            disabled={pending !== null}
            onClick={() => void run("Opening official authorization", { action: "schwabOAuth", sessionId: session.session_id, lifecycleAction: "begin" })}
          >
            Authorize with Schwab
          </Button>
          <Button
            variant="outline"
            disabled={pending !== null}
            onClick={() => void run("Checking authorization", { action: "schwabOAuth", sessionId: session.session_id, lifecycleAction: "continue" })}
          >
            I finished authorization
          </Button>
        </div>
      </div> : null}
      {session && savedConfigurationSessionId ? <div className="mt-5 space-y-3">
        <p className="text-sm text-muted-foreground">
          This connection has a saved data selection. Verification and resume keep that exact selection and its retained evidence.
        </p>
        <div className="flex flex-wrap gap-2">
          <Button
            disabled={pending !== null || cancelling}
            onClick={() => void run("Verifying saved connection", { action: "verifySaved", sessionId: savedConfigurationSessionId })}
          >
            Verify saved connection
          </Button>
          <Button
            variant="outline"
            disabled={pending !== null || cancelling}
            onClick={() => void run("Resuming saved connection", { action: "restoreSaved", sessionId: savedConfigurationSessionId })}
          >
            Resume saved connection
          </Button>
        </div>
      </div> : null}
      {session && activationKind === null && controls.length === 0 && profile.id !== "schwab.trader-api-market-data" ? <p className="mt-4 text-sm text-muted-foreground">
        The current installed capability has no activation action for this connection. Review its provider requirements and current status before continuing.
      </p> : null}
      {canActivate && session && activationKind ? <ActivationForm
        key={`${session.session_id}:${activationKind}`}
        kind={activationKind}
        pending={pending !== null || cancelling}
        saved={saved}
        onSubmit={(request) => run("Verifying and connecting", { action: "activate", sessionId: session.session_id, request })}
      /> : null}
      {session && ["reconcile_cleanup", "renew_credential", "refresh_evidence", "resolve_rights", "start_new_session"].includes(next ?? "") ? <div className="mt-5 space-y-3">
        <p className="text-sm">
          {recoveryLabel(next)}
        </p>
        {recovered.map((line) => <p className="text-xs text-muted-foreground" key={line}>
          {line}
        </p>)}
        {next === "reconcile_cleanup" ? <Button
          variant="outline"
          disabled={pending !== null}
          onClick={() => void run("Reconciling saved setup", { action: "cleanup", sessionId: session.session_id })}
        >
          Reconcile saved setup
        </Button> : null}
      </div> : null}
      {controls.length > 0 ? <div className="mt-5 border-t border-border pt-4">
        <p className="mb-3 text-sm">Connection controls</p>
        <div className="flex flex-wrap gap-2">
          {controls.map((control) => <Button
            key={control.action}
            variant="outline"
            disabled={pending !== null || cancelling}
            onClick={() => control.destructive ? setConfirmation(control) : void runControl(control)}
          >
            {control.label === "Run doctor" ? "Verify saved credential" : control.label}
          </Button>)}
        </div>
      </div> : null}
      {session && profile.id === "schwab.trader-api-market-data" ? <div className="mt-4 flex gap-2">
        <Button
          variant="outline"
          disabled={pending !== null}
          onClick={() => void run("Cancelling authorization", { action: "schwabOAuth", sessionId: session.session_id, lifecycleAction: "cancel" })}
        >
          Cancel authorization
        </Button>
        <Button variant="outline" disabled={pending !== null} onClick={() => setConfirmation("unlink")}>
          Unlink Schwab authorization
        </Button>
      </div> : null}
      {source?.storedData ? <p className="mt-5 flex items-center gap-2 text-sm">
        <CheckCircle2 className="size-4 text-emerald-300" aria-hidden="true" />
        {source.storedData.rowCount.toLocaleString()}
        saved observations are available.
      </p> : null}
      {session ? <div className="mt-5 flex flex-wrap gap-2 border-t border-border pt-4">
        <Button
          variant="outline"
          disabled={pending !== null || cancelling}
          onClick={() => void run("Refreshing saved setup", { action: "resume", sessionId: session.session_id })}
        >
          Resume saved setup
        </Button>
        {activationKind === "treasury_fiscal" || activationKind === "treasury_daily_rates" ? <Button
          variant="outline"
          disabled={pending !== null || cancelling || next !== "active"}
          onClick={() => void run("Resuming data import", { action: "resumePublication", sessionId: session.session_id })}
        >
          Resume data import
        </Button> : null}
        {saved && ["active", "renew_credential"].includes(next ?? "") ? <Button
          variant="outline"
          disabled={pending !== null || cancelling}
          onClick={() => setConfirmation("renew")}
        >
          Replace credential
        </Button> : null}
        <Button variant="outline" disabled={cancelling} onClick={() => setConfirmation("cancel")}>
          {cancelling ? "Stopping…" : "Disconnect / cancel setup"}
        </Button>
      </div> : null}
      <details className="mt-5 text-xs text-muted-foreground">
        <summary className="cursor-pointer">Provider information</summary>
        {!saved && !noKey ? <p className="mt-3 leading-5">
          {profile.handoff_instruction}
        </p> : <p className="mt-3 leading-5">
          Use the official provider page only when you need to manage your provider account. Ordinary setup continues here.
        </p>}
        <div className="mt-3 flex flex-wrap gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() => void transport.openOfficialProviderPage(profile.id).catch((failure) => setError(messageFrom(failure)))}
          >
            Official provider page
          </Button>
        </div>
      </details>
      <Dialog open={confirmation !== null} onOpenChange={(open) => {
        if (!open) setConfirmation(null)
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {confirmation === "cancel"
                ? "Disconnect this connection?"
                : confirmation === "renew"
                  ? "Replace the saved credential?"
                  : confirmation === "unlink"
                    ? "Unlink Schwab authorization?"
                    : "Change this connection?"}
            </DialogTitle>
            <DialogDescription>
              {confirmation === "cancel"
                ? "This stops this provider's connection and cancels its setup or import. "
                  + "Stored financial data is retained. Any required credential cleanup remains resumable."
                : confirmation === "unlink"
                  ? "This revokes the saved Schwab authorization and stops its account market-data access. "
                    + "Stored financial data is retained."
                  : confirmation === "renew"
                    ? "Continue only when the provider has issued a replacement. "
                      + "Market Squawk keeps its existing lifecycle and cutover checks."
                    : "This changes the current connection state. "
                      + "Verification may stop an active connection until you start it again."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmation(null)}>Keep current state</Button>
            <Button
              onClick={() => {
                const action = confirmation
                setConfirmation(null)
                if (action === "cancel") {
                  void cancel()
                } else if (action === "unlink" && session) {
                  void run("Unlinking authorization", {
                    action: "schwabOAuth",
                    sessionId: session.session_id,
                    lifecycleAction: "unlink",
                  })
                } else if (action === "renew" && session) {
                  void run("Preparing credential replacement", {
                    action: "renew",
                    sessionId: session.session_id,
                  })
                } else if (action && typeof action !== "string") {
                  void runControl(action)
                }
              }}
            >
              Confirm change
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function CredentialForm({ profile, pending, onSubmit }: { profile: ProviderProfile; pending: boolean; onSubmit: (secret: string) => Promise<void> }) {
  const alpaca = profile.id === "alpaca.basic-market-data"
  const schwab = profile.id === "schwab.trader-api-market-data"
  const direct = profile.id === "coinbase.exchange-direct-market-data"
  return <form
    className="mt-5 space-y-3"
    onSubmit={(event) => {
      event.preventDefault()
      const form = event.currentTarget
      const data = new FormData(form)
      const secret = alpaca ? JSON.stringify({
        version: 1,
        key_id: field(data, "key"),
        secret_key: field(data, "secret"),
        trading_api_environment: "paper"
      }) : schwab ? JSON.stringify({ version: 1, app_key: field(data, "key"), app_secret: field(data, "secret") }) : direct ? JSON.stringify({
        version: 1,
        api_key: field(data, "key"),
        signing_secret: field(data, "secret"),
        passphrase: field(data, "passphrase")
      }) : field(data, "key")
      form.reset()
      void onSubmit(secret)
    }}
  >
    <p className="text-sm">Enter a credential only when this setup needs one. Values are cleared from the form when submitted.</p>
    <Field
      name="key"
      label={alpaca ? "Paper API key ID" : schwab ? "Application key" : "API key"}
      type="password"
      maxLength={4096}
    />
    {alpaca || schwab || direct ? <Field
      name="secret"
      label={schwab ? "Application secret" : "Secret key"}
      type="password"
      maxLength={4096}
    /> : null}
    {direct ? <Field
      name="passphrase"
      label="API passphrase"
      type="password"
      maxLength={1024}
    /> : null}
    <Button disabled={pending}>Save credential securely</Button>
  </form>
}

function ActivationForm({ kind, pending, saved, onSubmit }: {
  kind: NonNullable<ActivationKind>
  pending: boolean
  saved: boolean
  onSubmit: (request: Record<string, unknown>) => Promise<void>
}) {
  const [formError, setFormError] = React.useState<string | null>(null)
  const year = new Date().getUTCFullYear()
  const [seriesCount, setSeriesCount] = React.useState(1)
  return <form
    className="mt-5 space-y-4"
    onSubmit={(event) => {
      event.preventDefault()
      setFormError(null)
      try {
        const data = new FormData(event.currentTarget)
        const request: Record<string, unknown> = { kind }
        if (kind === "bea" || kind === "census") request.selection = macroSelection(kind, data)
        if (kind === "sec") request.cik = field(data, "cik")
        if (kind === "treasury_fiscal") request.page_size = Number(field(data, "pageSize"))
        if (kind === "eia_electricity_price") {
          request.start_period = field(data, "startPeriod")
          request.end_period = field(data, "endPeriod")
        }
        if (kind === "fred_alfred") {
          request.provider_dataset = [
            field(data, "history"),
            "series-observations",
            field(data, "series"),
            field(data, "vintageStart"),
            field(data, "vintageEnd"),
          ].join(":")
        }
        if (kind === "bls") {
          request.start_year = Number(field(data, "startYear"))
          request.end_year = Number(field(data, "endYear"))
          request.series = Array.from({ length: seriesCount }, (_, index) => ({
            series_id: field(data, `series-${index}`),
            title: field(data, `title-${index}`),
            unit: field(data, `unit-${index}`),
            frequency: field(data, `frequency-${index}`),
            seasonal_adjustment: field(data, `adjustment-${index}`),
            measure: field(data, `measure-${index}`)
          }))
        }
        void onSubmit(request)
      } catch (failure) {
        setFormError(messageFrom(failure))
      }
    }}
  >
    {formError ? <p role="alert" className="text-sm text-red-400">
      {formError}
    </p> : null}
    {kind === "bea" || kind === "census" ? <MacroSelectionFields provider={kind} /> : null}
    {kind === "sec" ? <Field
      name="cik"
      label="Company CIK (10 digits)"
      pattern="[0-9]{10}"
      maxLength={10}
    /> : null}
    {kind === "treasury_fiscal" || kind === "treasury_daily_rates" ? <p className="text-sm text-muted-foreground">
      Import the complete published Treasury rate history. Accepted progress is retained for restart and resume.
    </p> : null}
    {kind === "treasury_fiscal" ? <details>
      <summary className="cursor-pointer text-xs text-muted-foreground">Request size</summary>
      <Field
        name="pageSize"
        label="Rows per request"
        type="number"
        defaultValue="1000"
        min={1}
        max={10000}
      />
    </details> : null}
    {kind === "eia_electricity_price" ? <div className="grid gap-3 sm:grid-cols-2">
      <Field name="startPeriod" label="First month (up to 24 months)" type="month" />
      <Field name="endPeriod" label="Last month" type="month" />
    </div> : null}
    {kind === "fred_alfred" ? <div className="grid gap-3 sm:grid-cols-2">
      <div>
        <Label htmlFor="setup-history">History</Label>
        <select
          id="setup-history"
          name="history"
          className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
        >
          <option value="alfred">Historical vintages</option>
          <option value="fred">Current observations</option>
        </select>
      </div>
      <Field
        name="series"
        label="Economic series ID"
        defaultValue="UNRATE"
        pattern="[A-Za-z0-9_.-]{1,120}"
        maxLength={120}
      />
      <Field
        name="vintageStart"
        label="First vintage date"
        type="date"
        defaultValue="1776-07-04"
      />
      <Field
        name="vintageEnd"
        label="Last vintage date"
        type="date"
        defaultValue="9999-12-31"
      />
    </div> : null}
    {kind === "bls" ? <>
      <div className="grid gap-3 sm:grid-cols-2">
        <Field
          name="startYear"
          label="First year"
          type="number"
          defaultValue={String(year - 2)}
          min={1900}
          max={year}
        />
        <Field
          name="endYear"
          label="Last year"
          type="number"
          defaultValue={String(year)}
          min={1900}
          max={year}
        />
      </div>
      {Array.from({ length: seriesCount }, (_, i) => <fieldset key={i} className="grid gap-3 border-t border-border pt-3 sm:grid-cols-2">
        <legend className="text-sm">
          Verified series
          {i + 1}
        </legend>
        <Field name={`series-${i}`} label="Series ID" defaultValue={i === 0 ? "LNS14000000" : ""} />
        <Field name={`title-${i}`} label="Title" defaultValue={i === 0 ? "Unemployment Rate" : ""} />
        <Field name={`unit-${i}`} label="Unit" defaultValue={i === 0 ? "percent" : ""} />
        <Field name={`frequency-${i}`} label="Frequency" defaultValue={i === 0 ? "monthly" : ""} />
        <Field
          name={`adjustment-${i}`}
          label="Seasonal adjustment"
          defaultValue={i === 0 ? "seasonally-adjusted" : ""}
        />
        <Field name={`measure-${i}`} label="Measure" defaultValue={i === 0 ? "unemployment-rate" : ""} />
      </fieldset>)}
      <Button
        type="button"
        variant="outline"
        disabled={seriesCount >= 1000}
        onClick={() => setSeriesCount((count) => count + 1)}
      >
        Add verified series
      </Button>
    </> : null}
    <Button disabled={pending}>
      {saved ? "Verify saved credential and connect" : "Verify and connect"}
    </Button>
  </form>
}

function Field({ name, label, ...props }: React.ComponentProps<typeof Input> & { name: string; label: string }) {
  const id = `connection-${name}`
  return <div>
    <Label htmlFor={id}>
      {label}
    </Label>
    <Input
      id={id}
      name={name}
      required
      autoComplete="off"
      spellCheck={false}
      maxLength={256}
      className="mt-2"
      {...props}
    />
  </div>
}

function field(data: FormData, name: string): string {
  return String(data.get(name) ?? "")
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div>
    <dt className="text-xs text-muted-foreground">
      {label}
    </dt>
    <dd className="mt-1 text-sm font-medium">
      {value}
    </dd>
  </div>
}

function verificationLabel(session?: ProviderSession): string {
  if (!session) return "Not checked"
  if (session.next_action === "active") return "Setup accepted"
  if (session.next_action === "complete_oauth_authorization") return "Authorization needed"
  if (session.credential_stored) return "Saved credential needs checking"
  return "Setup needs attention"
}

function recoveryLabel(next: string | undefined): string {
  switch (next) {
    case "reconcile_cleanup": return "The previous attempt needs reconciliation. Continue its saved cleanup before replacing credentials or reconnecting."
    case "renew_credential": return "The previous verification has expired. Recheck the connection or supply a replacement credential when required."
    case "refresh_evidence": return "The provider's connection requirements changed. Refresh saved setup and review the provider guidance."
    case "resolve_rights": return "This provider's access requirements need attention before it can connect."
    default: return "This attempt has ended. You can begin a new setup when ready."
  }
}
