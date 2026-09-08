import * as React from "react"
import { AlertCircle, CheckCircle2, FileKey2, LoaderCircle } from "lucide-react"
import { z } from "zod"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { humanize } from "@/lib/formatters"
import type { SystemTransport } from "@/lib/transport"

const providerOrder = [
  "schwab",
  "alpaca",
  "yahoo_finance_experimental",
  "nasdaq_trader_reference",
  "occ_options_reference",
  "cboe_options_reference",
  "iex_hist",
  "bls",
  "bea",
  "census",
  "eia",
  "fred_alfred",
  "tiingo",
  "sec",
  "treasury_fiscal_data",
  "treasury_daily_rates",
  "federal_reserve_board_direct",
] as const

const providerCredentialDispositionSchema = z.strictObject({
  provider: z.enum(providerOrder),
  enabled: z.boolean(),
  disposition: z.enum([
    "credential_stored_unverified",
    "probe_required",
    "disabled",
    "profile_unavailable",
  ]),
  onboardingSessionId: z.string().uuid().nullable(),
})

const providerCredentialImportSchema = z
  .strictObject({
    schema: z.literal("market-squawk-provider-credentials/v1"),
    providers: z.array(providerCredentialDispositionSchema).length(providerOrder.length),
  })
  .superRefine((result, context) => {
    result.providers.forEach((provider, index) => {
      if (
        provider.provider !== providerOrder[index] ||
        provider.enabled === (provider.disposition === "disabled")
      ) {
        context.addIssue({
          code: "custom",
          message: "Provider credential dispositions are internally inconsistent.",
        })
      }
    })
  })

type ProviderCredentialImportResult = z.infer<
  typeof providerCredentialImportSchema
>

export function ProviderCredentialImport({
  available,
  transport,
  onAttempted,
}: {
  available: boolean
  transport: SystemTransport
  onAttempted: () => void
}) {
  const [pending, setPending] = React.useState(false)
  const [result, setResult] = React.useState<ProviderCredentialImportResult | null>(null)
  const [notice, setNotice] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)

  const importBundle = async () => {
    let cancelled = false
    setPending(true)
    setResult(null)
    setNotice(null)
    setError(null)
    try {
      const value = await transport.importProviderCredentialBundle()
      if (value === null) {
        cancelled = true
        setNotice("No credential bundle was selected. Provider setup is unchanged.")
        return
      }
      const parsed = providerCredentialImportSchema.safeParse(value)
      if (!parsed.success) {
        throw new Error("unsupported_provider_credential_receipt")
      }
      setResult(parsed.data)
    } catch {
      setError(
        "Market Squawk could not complete this credential bundle. One or more earlier entries may already have been stored. Source evidence is refreshing; review it before correcting the file or service issue and trying again.",
      )
    } finally {
      if (!cancelled) onAttempted()
      setPending(false)
    }
  }

  const selectedProviders = result?.providers.filter((provider) => provider.enabled) ?? []
  const disabledCount = result
    ? result.providers.length - selectedProviders.length
    : 0

  return (
    <section className="mb-5 rounded-xl border border-border bg-card/45 p-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <FileKey2 className="size-4 text-primary" aria-hidden="true" />
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
              Protected credential import
            </p>
          </div>
          <h2 className="mt-2 text-lg font-semibold">Import provider credentials</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Select one filled Market Squawk provider-credentials .env file. The native app stages
            it through a bounded one-time ticket and returns only provider dispositions. Importing
            stores selected credentials or setup intent; it does not verify, activate, or start a
            provider.
          </p>
        </div>
        <Button
          onClick={() => void importBundle()}
          disabled={!available || pending}
        >
          {pending ? (
            <LoaderCircle className="animate-spin" aria-hidden="true" />
          ) : (
            <FileKey2 aria-hidden="true" />
          )}
          {pending
            ? "Importing safely…"
            : result
              ? "Select another bundle"
              : "Choose credential bundle"}
        </Button>
      </div>

      {!available ? (
        <Alert className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Credential import is unavailable</AlertTitle>
          <AlertDescription>
            This installed service does not advertise the protected credential-bundle operation.
            No file can be selected through an incomplete authority chain.
          </AlertDescription>
        </Alert>
      ) : null}
      {notice ? (
        <p className="mt-4 text-xs text-muted-foreground" role="status">
          {notice}
        </p>
      ) : null}
      {error ? (
        <Alert className="mt-4" variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Credential import needs attention</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
      {result ? (
        <div className="mt-4 rounded-lg border border-border bg-background/45 p-4" role="status">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="size-4 text-emerald-300" aria-hidden="true" />
            <p className="text-sm font-medium">Credential bundle processed</p>
          </div>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            {selectedProviders.length} selected provider
            {selectedProviders.length === 1 ? "" : "s"} returned a protected setup result;{" "}
            {disabledCount} provider{disabledCount === 1 ? " was" : "s were"} not selected.
          </p>
          {selectedProviders.length > 0 ? (
            <ul className="mt-3 grid gap-2 sm:grid-cols-2">
              {selectedProviders.map((provider) => (
                <li
                  key={provider.provider}
                  className="rounded-md border border-border bg-card/50 p-3"
                >
                  <p className="text-xs font-medium">{humanize(provider.provider)}</p>
                  <p className="mt-1 text-[10px] text-muted-foreground">
                    {dispositionLabel(provider.disposition)}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="mt-3 text-xs text-muted-foreground">
              The bundle selected no providers, so no provider setup can continue from this result.
            </p>
          )}
        </div>
      ) : null}
    </section>
  )
}

function dispositionLabel(
  disposition: ProviderCredentialImportResult["providers"][number]["disposition"],
) {
  switch (disposition) {
    case "credential_stored_unverified":
      return "Credential stored; verification and activation are still required."
    case "probe_required":
      return "Selected; provider verification is still required."
    case "profile_unavailable":
      return "Selected, but no compatible provider profile is currently available."
    case "disabled":
      return "Not selected."
  }
}
