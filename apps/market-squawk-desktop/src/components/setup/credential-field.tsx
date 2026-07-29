import * as React from "react"
import { KeyRound, LoaderCircle } from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import type { ProductTransport } from "@/lib/transport"

export function CredentialField({
  providerName,
  credentialKind = "api-key",
  sessionId,
  transport,
  onAccepted,
}: {
  providerName: string
  credentialKind?: "api-key" | "coinbase-exchange"
  sessionId: string
  transport: ProductTransport
  onAccepted: (result: unknown) => void
}) {
  const fields =
    credentialKind === "coinbase-exchange"
      ? [
          { id: "api-key", label: "Coinbase API key" },
          { id: "passphrase", label: "Coinbase passphrase" },
          { id: "signing-secret", label: "Coinbase API secret" },
        ]
      : [{ id: "api-key", label: "Provider API key" }]
  const [values, setValues] = React.useState<Record<string, string>>({})
  const [error, setError] = React.useState<string | null>(null)
  const [submitting, setSubmitting] = React.useState(false)

  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const submitted = fields.map((field) => values[field.id] ?? "")
    setValues({})
    setError(null)
    if (submitted.some((value) => !value)) {
      setError("Complete every provider credential field before continuing.")
      return
    }
    const [apiKey = "", passphrase = "", signingSecret = ""] = submitted
    const secret =
      credentialKind === "coinbase-exchange"
        ? JSON.stringify({
            version: 1,
            api_key: apiKey,
            passphrase,
            signing_secret: signingSecret,
          })
        : apiKey
    setSubmitting(true)
    try {
      const result = await transport.onboard({
        action: "submitSecret",
        sessionId,
        secret,
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
      <div className="grid gap-2">
        {fields.map((field) => (
          <div className="space-y-1.5" key={field.id}>
            <Label htmlFor={`provider-secret-${sessionId}-${field.id}`}>
              {field.label}
            </Label>
            <div className="relative">
              <KeyRound
                className="pointer-events-none absolute top-2.5 left-3 size-4 text-muted-foreground"
                aria-hidden="true"
              />
              <Input
                id={`provider-secret-${sessionId}-${field.id}`}
                type="password"
                value={values[field.id] ?? ""}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setValues((current) => ({
                    ...current,
                    [field.id]: value,
                  }))
                }}
                autoComplete="new-password"
                spellCheck={false}
                className="pl-9 font-mono"
                disabled={submitting}
              />
            </div>
          </div>
        ))}
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
