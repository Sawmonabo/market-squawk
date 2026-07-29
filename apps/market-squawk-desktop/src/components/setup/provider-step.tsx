import * as React from "react"
import { ArrowUpRight, CircleCheck, LoaderCircle } from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import { CredentialField } from "@/components/setup/credential-field"
import type { ProviderProfile, ProviderSession } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

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

  const start = async (profile: ProviderProfile) => {
    setPending(profile.id)
    setError(null)
    try {
      await transport.onboard({
        action: "start",
        surfaceId: profile.id,
      })
      onChanged()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  const openOfficialPage = async (profile: ProviderProfile) => {
    setPending(profile.id)
    setError(null)
    try {
      await transport.openOfficialProviderPage(profile.id)
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  return (
    <div className="space-y-3">
      <div>
        <h2 className="text-base font-semibold">Connect free data sources</h2>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
          Choose a source and Market Squawk will explain any account or key it
          needs. Provider sign-in pages always open in your normal browser.
        </p>
      </div>
      <div className="grid gap-3 lg:grid-cols-2">
        {profiles.map((profile) => {
          const session = sessions.find(
            (candidate) => candidate.surface_id === profile.id,
          )
          const active = session?.next_action === "active"
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
                {active ? (
                  <span className="flex items-center gap-1 text-[10px] font-medium text-emerald-400">
                    <CircleCheck className="size-3.5" aria-hidden="true" />
                    Active
                  </span>
                ) : null}
              </div>
              <div className="mt-4 flex flex-wrap gap-2">
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
                {!session ? (
                  <Button
                    type="button"
                    size="sm"
                    onClick={() => start(profile)}
                    disabled={pending === profile.id}
                  >
                    {pending === profile.id ? (
                      <LoaderCircle className="animate-spin" aria-hidden="true" />
                    ) : null}
                    Start setup
                  </Button>
                ) : (
                  <span className="self-center text-[10px] text-muted-foreground">
                    Next: {plainAction(session.next_action)}
                  </span>
                )}
              </div>
              {session?.next_action === "import_secret" ? (
                <div className="mt-4">
                  <CredentialField
                    providerName={profile.display_name}
                    sessionId={session.session_id}
                    transport={transport}
                    onAccepted={onChanged}
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
