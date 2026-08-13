import * as React from "react"
import {
  ArrowUpRight,
  CircleCheck,
  ExternalLink,
  LoaderCircle,
} from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { CredentialField } from "@/components/setup/credential-field"
import { Button } from "@/components/ui/button"
import {
  type ProviderProfile,
  type ProviderSession,
} from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

const NATIVE_SOURCE_PROFILES = new Set([
  "coinbase.public-market-data",
  "coinbase.exchange-direct-market-data",
  "kraken.spot-public-market-data",
])
const SECRET_ACTIONS = new Set([
  "complete_provider_handoff",
  "import_secret",
  "import_replacement",
])
const ACTIVATION_ACTIONS = new Set([
  "verify_and_activate",
  "verify_and_cutover",
])

export function ProviderStep({
  profiles,
  sessions,
  transport,
  onChanged,
}: {
  profiles: ProviderProfile[]
  sessions: ProviderSession[]
  transport: ProductTransport
  onChanged: () => void
}) {
  const [pending, setPending] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [localSessions, setLocalSessions] = React.useState<
    Record<string, ProviderSession>
  >({})
  const sourceProfiles = profiles.filter(
    (profile) => !profile.id.startsWith("local."),
  )

  React.useEffect(() => {
    setLocalSessions((current) => {
      const retained = Object.entries(current).filter(([surfaceId, local]) => {
        const authoritative = sessions.find(
          (candidate) => candidate.surface_id === surfaceId,
        )
        if (!authoritative) return true
        const parentCaughtUp =
          authoritative.session_id === local.session_id &&
          authoritative.state === local.state &&
          authoritative.next_action === local.next_action &&
          authoritative.credential_stored === local.credential_stored
        return !parentCaughtUp && authoritative.next_action !== "active"
      })
      return retained.length === Object.keys(current).length
        ? current
        : Object.fromEntries(retained)
    })
  }, [sessions])

  const sessionFor = (profile: ProviderProfile) => {
    const authoritative = sessions.find(
      (candidate) => candidate.surface_id === profile.id,
    )
    const local = localSessions[profile.id]
    if (authoritative?.next_action === "active") return authoritative
    return local ?? authoritative
  }

  const remember = (session: ProviderSession) => {
    setLocalSessions((current) => ({
      ...current,
      [session.surface_id]: session,
    }))
  }

  const activateSource = async (
    profile: ProviderProfile,
    session: ProviderSession,
  ) => {
    if (!ACTIVATION_ACTIONS.has(session.next_action)) {
      return
    }
    setPending(profile.id)
    setError(null)
    try {
      const activation = await transport.onboard({
        action: "activate",
        sessionId: session.session_id,
        request: { kind: "source" },
      })
      if (
        activation.profile !== profile.id ||
        activation.session_id !== session.session_id
      ) {
        throw new Error("Provider activation returned a different setup identity.")
      }
      remember({
        ...session,
        state: "active_scoped",
        next_action: "active",
      })
      onChanged()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  const startNativeSource = async (profile: ProviderProfile) => {
    setPending(profile.id)
    setError(null)
    try {
      const session = await transport.onboard({
        action: "start",
        surfaceId: profile.id,
      })
      remember(session)
      if (ACTIVATION_ACTIONS.has(session.next_action)) {
        await activateSource(profile, session)
      }
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  const continueAfterCredential = async (
    profile: ProviderProfile,
    session: ProviderSession,
  ) => {
    remember(session)
    await activateSource(profile, session)
  }

  const runBrowserAction = async (
    profile: ProviderProfile,
    action: () => Promise<void>,
  ) => {
    setPending(profile.id)
    setError(null)
    try {
      await action()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  const openOfficialPage = async (profile: ProviderProfile) => {
    await runBrowserAction(profile, () =>
      transport.openOfficialProviderPage(profile.id),
    )
  }

  const openProtectedSetup = async (profile: ProviderProfile) => {
    await runBrowserAction(profile, () =>
      transport.openProtectedProviderSetup(profile.id),
    )
  }

  return (
    <div className="space-y-3">
      <div>
        <h2 className="text-base font-semibold">Connect free data sources</h2>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
          Public live feeds connect in the desktop. Research providers use the
          existing protected local setup when they need detailed series, date,
          contact, or rights evidence. Provider account pages open only in your
          normal browser.
        </p>
      </div>
      <div className="grid gap-3 lg:grid-cols-2">
        {sourceProfiles.map((profile) => {
          const session = sessionFor(profile)
          const setupReady = session?.next_action === "active"
          const nativeSource = NATIVE_SOURCE_PROFILES.has(profile.id)
          const needsSecret =
            session && SECRET_ACTIONS.has(session.next_action)
          const canActivate =
            session && ACTIVATION_ACTIONS.has(session.next_action)
          return (
            <article
              key={profile.id}
              className="rounded-lg border border-border bg-background/35 p-4"
            >
              <div className="flex items-start gap-3">
                <div className="min-w-0 flex-1">
                  <h3 className="text-sm font-semibold">{profile.display_name}</h3>
                  <p className="mt-1 line-clamp-2 text-xs leading-relaxed text-muted-foreground">
                    {profile.coverage}
                  </p>
                </div>
                {setupReady ? (
                  <span className="flex items-center gap-1 text-[10px] font-medium text-emerald-400">
                    <CircleCheck className="size-3.5" aria-hidden="true" />
                    {session?.credential_stored ? "Credentials stored" : "Setup verified"}
                  </span>
                ) : null}
              </div>

              <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 border-t border-border/70 pt-3 text-[10px]">
                <ProviderFact label="Account" value={profile.account_requirement} />
                <ProviderFact label="Credential" value={profile.credential_requirement} />
                <ProviderFact label="Cost" value={profile.zero_fee} />
                <ProviderFact label="Quality ceiling" value={profile.quality_ceiling} />
              </dl>

              {setupReady ? (
                <p className="mt-3 text-[10px] leading-relaxed text-muted-foreground">
                  Completed setup does not start a source or authorize trading. Sources shows any
                  server-owned checks required before an explicit Start.
                </p>
              ) : null}

              {!setupReady ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  {nativeSource && !session ? (
                    <Button
                      type="button"
                      size="sm"
                      onClick={() => startNativeSource(profile)}
                      disabled={pending === profile.id}
                    >
                      {pending === profile.id ? (
                        <LoaderCircle
                          className="animate-spin"
                          aria-hidden="true"
                        />
                      ) : null}
                      Connect source
                    </Button>
                  ) : null}
                  {session && canActivate ? (
                    <Button
                      type="button"
                      size="sm"
                      onClick={() => activateSource(profile, session)}
                      disabled={pending === profile.id}
                    >
                      Verify and activate
                    </Button>
                  ) : null}
                  {!nativeSource || (session && !needsSecret && !canActivate) ? (
                    <Button
                      type="button"
                      size="sm"
                      onClick={() => openProtectedSetup(profile)}
                      disabled={pending === profile.id}
                    >
                      <ExternalLink aria-hidden="true" />
                      Continue protected setup
                    </Button>
                  ) : null}
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => openOfficialPage(profile)}
                    disabled={pending === profile.id}
                  >
                    Official page
                    <ArrowUpRight aria-hidden="true" />
                  </Button>
                </div>
              ) : null}

              {session && !setupReady ? (
                <p className="mt-3 text-[10px] text-muted-foreground">
                  Next: {plainAction(session.next_action)}
                </p>
              ) : null}

              {session && needsSecret ? (
                <div className="mt-4">
                  <CredentialField
                    providerName={profile.display_name}
                    credentialKind={
                      profile.id ===
                      "coinbase.exchange-direct-market-data"
                        ? "coinbase-exchange"
                        : "api-key"
                    }
                    sessionId={session.session_id}
                    transport={transport}
                    onAccepted={(result) =>
                      continueAfterCredential(profile, result)
                    }
                  />
                </div>
              ) : null}
            </article>
          )
        })}
      </div>
      {error ? (
        <p role="alert" className="text-sm text-red-400">
          {error}
        </p>
      ) : null}
    </div>
  )
}

function plainAction(action: string) {
  return action.replaceAll("_", " ")
}

function ProviderFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 line-clamp-2 text-foreground/80">{value}</dd>
    </div>
  )
}
