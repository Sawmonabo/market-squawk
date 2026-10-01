import * as React from "react"
import { KeyRound, LoaderCircle } from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import type { SecretAccessPolicy, SecretAccessStatus } from "@/lib/schemas"
import type { CredentialAccessRequest } from "@/lib/transport"

const SECONDS_PER_DAY = 86_400

type Props = {
  status: SecretAccessStatus
  onSubmit: (request: CredentialAccessRequest) => Promise<SecretAccessStatus>
  disabled?: boolean
}

export function ApplicationLock({ status, onSubmit, disabled = false }: Props) {
  const id = React.useId()
  const [enabled, setEnabled] = React.useState(status.enabled)
  const [remember, setRemember] = React.useState(status.rememberInKeychain)
  const [periodic, setPeriodic] = React.useState(status.reauthenticateAfterSeconds !== null)
  const [days, setDays] = React.useState(daysFrom(status))
  const [pending, setPending] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const [notice, setNotice] = React.useState<string | null>(null)
  const inFlight = React.useRef(false)
  const ready = status.access === "ready"
  const remembered = status.rememberInKeychain || status.rememberedAccessAvailable
  const busy = disabled || pending

  const resetDraft = React.useCallback((next: SecretAccessStatus) => {
    setEnabled(next.enabled)
    setRemember(next.rememberInKeychain)
    setPeriodic(next.reauthenticateAfterSeconds !== null)
    setDays(daysFrom(next))
  }, [])

  React.useEffect(() => {
    resetDraft(status)
  }, [status.enabled, status.rememberInKeychain, status.reauthenticateAfterSeconds, resetDraft])

  const submit = async (request: CredentialAccessRequest) => {
    if (disabled || inFlight.current) return
    inFlight.current = true
    setPending(true)
    setError(null)
    setNotice(null)
    try {
      const next = await onSubmit(request)
      resetDraft(next)
      setNotice(request.action === "forgetRememberedAccess"
        ? next.rememberedAccessAvailable ? "Remembered access is still saved. Review the current access state." : "Remembered access forgotten. Your saved connection credentials are retained."
        : request.action === "lockAccess"
          ? next.access === "locked" ? "Saved connection credentials are locked." : "Access state refreshed."
          : request.action === "unlockAccess"
            ? next.access === "ready" ? "Saved connection credentials are available." : "Access still needs attention."
            : next.enabled ? "Application lock settings saved." : "Application lock is off. Saved connections will reopen automatically.")
    } catch (failure) {
      setError(messageFrom(failure))
    } finally {
      inFlight.current = false
      setPending(false)
    }
  }

  const savePolicy = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const secret = takeSecret(event.currentTarget)
    if (busy || !ready) return
    setError(null)
    setNotice(null)
    let seconds: number | null = null
    if (enabled && periodic) {
      const count = /^\d+$/.test(days) ? Number(days) : Number.NaN
      seconds = count * SECONDS_PER_DAY
      if (!Number.isSafeInteger(count) || count <= 0 || !Number.isSafeInteger(seconds)) {
        setError("Enter a positive whole number of days.")
        return
      }
    }
    if (enabled && !status.enabled && !secret) {
      setError("Choose a password before enabling application lock.")
      return
    }
    const policy: SecretAccessPolicy = {
      enabled,
      rememberInKeychain: enabled && remember,
      reauthenticateAfterSeconds: enabled ? seconds : null,
    }
    void submit({ action: "configureAccess", policy, ...(enabled && !status.enabled ? { secret } : {}) })
  }

  return (
    <section className="mt-6 rounded-xl border border-border bg-card/35 p-5" aria-labelledby={`${id}-heading`}>
      <div className="flex gap-3">
        <KeyRound className="mt-0.5 size-5 text-primary" aria-hidden="true" />
        <div>
          <h2 id={`${id}-heading`} className="font-semibold">Application lock</h2>
          <p className="mt-1 text-sm leading-6 text-muted-foreground">
            {status.enabled
              ? ready ? "Application lock is on. Saved connection credentials are available." : "Application lock is on. Saved connection credentials need unlocking."
              : "Application lock is off. Saved connections reopen automatically across launches."}
          </p>
          {!ready ? <p className="mt-1 text-sm leading-6 text-muted-foreground">
            {status.access === "recovery_required" ? "Credential access needs recovery. " : ""}
            Ordinary screens and saved results remain available. Unlock here to use saved connection credentials.
          </p> : null}
        </div>
      </div>

      {!ready ? (
        <form className="mt-4 flex max-w-xl flex-wrap items-end gap-3" onSubmit={(event) => {
          event.preventDefault()
          const secret = takeSecret(event.currentTarget)
          if (!busy && secret) void submit({ action: "unlockAccess", secret })
        }}>
          <div className="min-w-48 flex-1">
            <Label htmlFor={`${id}-unlock`}>Application lock password</Label>
            <Input id={`${id}-unlock`} name="secret" type="password" autoComplete="current-password"
              spellCheck={false} required disabled={busy} className="mt-2" />
          </div>
          <Button disabled={busy}>Unlock</Button>
        </form>
      ) : null}

      <form className="mt-5 space-y-4" onSubmit={savePolicy}>
        <fieldset disabled={busy || !ready} className="space-y-4 disabled:opacity-60">
          <legend className="sr-only">Application lock preferences</legend>
          <Check id={`${id}-enabled`} checked={enabled} onChange={setEnabled}>Enable application lock</Check>
          {enabled ? <div className="space-y-4 border-l border-border pl-4">
            {!status.enabled ? <div className="max-w-md">
              <Label htmlFor={`${id}-password`}>Choose an application lock password</Label>
              <Input id={`${id}-password`} name="secret" type="password" autoComplete="new-password"
                spellCheck={false} required className="mt-2" />
            </div> : null}
            <Check id={`${id}-remember`} checked={remember} onChange={setRemember}>Remember access in the OS keychain</Check>
            <p className="text-xs leading-5 text-muted-foreground">Remembered access lets saved connections reopen automatically until you lock them or your chosen interval expires.</p>
            <Check id={`${id}-periodic`} checked={periodic} onChange={setPeriodic}>Require the password again after a chosen number of days</Check>
            {periodic ? <div className="max-w-xs">
              <Label htmlFor={`${id}-days`}>Reauthentication interval (whole days)</Label>
              <Input id={`${id}-days`} type="number" inputMode="numeric" min={1} step={1} required
                value={days} onChange={(event) => setDays(event.target.value)} className="mt-2" />
            </div> : <p className="text-xs text-muted-foreground">No automatic reauthentication interval is set.</p>}
          </div> : null}
          <Button type="submit">Save lock settings</Button>
        </fieldset>
      </form>

      {(status.enabled && ready) || remembered ? <div className="mt-5 flex flex-wrap gap-3 border-t border-border pt-4">
        {status.enabled && ready ? <Button variant="outline" disabled={busy} onClick={() => void submit({ action: "lockAccess" })}>Lock</Button> : null}
        {remembered ? <Button variant="outline" disabled={busy} onClick={() => void submit({ action: "forgetRememberedAccess" })}>Forget remembered access</Button> : null}
      </div> : null}
      {status.enabled ? <p className="mt-3 text-xs text-muted-foreground">
        {status.rememberedAccessAvailable ? "Remembered access is available from the OS keychain." : remembered ? "Remembered access is currently unavailable. You can unlock here or forget it." : "No remembered access is saved."}
        {" "}Locking leaves saved results available. Forgetting remembered access keeps your provider credentials.
      </p> : null}
      <div aria-live="polite">
        {pending ? <p role="status" className="mt-3 flex items-center gap-2 text-sm"><LoaderCircle className="size-4 animate-spin" aria-hidden="true" />Updating access…</p> : null}
        {notice ? <p role="status" className="mt-3 text-sm text-emerald-300">{notice}</p> : null}
      </div>
      {error ? <p role="alert" className="mt-3 text-sm text-destructive">{error}</p> : null}
    </section>
  )
}

function Check({ id, checked, onChange, children }: {
  id: string
  checked: boolean
  onChange: (checked: boolean) => void
  children: React.ReactNode
}) {
  return <div className="flex items-center gap-2">
    <input id={id} type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)}
      className="size-4 accent-primary focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary" />
    <Label htmlFor={id}>{children}</Label>
  </div>
}

function daysFrom(status: SecretAccessStatus): string {
  return status.reauthenticateAfterSeconds === null ? "" : String(status.reauthenticateAfterSeconds / SECONDS_PER_DAY)
}

function takeSecret(form: HTMLFormElement): string {
  const field = form.elements.namedItem("secret")
  if (!(field instanceof HTMLInputElement)) return ""
  const secret = field.value
  field.value = ""
  return secret
}
