import * as React from "react"
import { KeyRound, LoaderCircle } from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import type { ProductTransport } from "@/lib/transport"

export function CredentialField({
  providerName,
  sessionId,
  transport,
  onAccepted,
}: {
  providerName: string
  sessionId: string
  transport: ProductTransport
  onAccepted: (result: unknown) => void
}) {
  const [secret, setSecret] = React.useState("")
  const [error, setError] = React.useState<string | null>(null)
  const [submitting, setSubmitting] = React.useState(false)

  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const submitted = secret
    setSecret("")
    setError(null)
    if (!submitted) {
      setError("Enter the provider API key before continuing.")
      return
    }
    setSubmitting(true)
    try {
      const result = await transport.onboard({
        action: "submitSecret",
        sessionId,
        secret: submitted,
      })
      onAccepted(result)
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form
      onSubmit={submit}
      className="space-y-3 rounded-lg border border-border bg-background/40 p-4"
    >
      <div className="space-y-1">
        <Label htmlFor={`provider-secret-${sessionId}`}>Provider API key</Label>
        <p className="text-xs text-muted-foreground">
          Paste the key created by {providerName}. Market Squawk stores it through
          the operating-system credential service and immediately clears this field.
        </p>
      </div>
      <div className="flex flex-col gap-2 sm:flex-row">
        <div className="relative flex-1">
          <KeyRound
            className="pointer-events-none absolute top-2.5 left-3 size-4 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            id={`provider-secret-${sessionId}`}
            type="password"
            value={secret}
            onChange={(event) => setSecret(event.currentTarget.value)}
            autoComplete="new-password"
            spellCheck={false}
            className="pl-9 font-mono"
            disabled={submitting}
          />
        </div>
        <Button type="submit" disabled={submitting}>
          {submitting ? (
            <LoaderCircle className="animate-spin" aria-hidden="true" />
          ) : null}
          Save and verify
        </Button>
      </div>
      {error ? (
        <p role="alert" className="text-sm text-red-400">
          {error}
        </p>
      ) : null}
    </form>
  )
}

