import * as React from "react"
import { Link } from "react-router-dom"
import { useMutation, useQuery } from "@tanstack/react-query"

import { messageFrom } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { formatMoney } from "@/lib/formatters"
import type { ProductTransport, RecommendationSetupRequest } from "@/lib/transport"
import {
  formatPreferencePercent, normalizeAmount, parsePreferencePercent,
  parseRecommendationCommit, parseRecommendationPreview, parseRecommendationSetup, preferenceTime,
  type RecommendationAllocation, type RecommendationSetupPreview, type RecommendationSetupStatus,
} from "./recommendation-setup-contracts"

type Draft = {
  accountId: string
  lower: string
  upper: string
  cash: string
  downside: string
  days: string
}
const emptyDraft: Draft = { accountId: "", lower: "", upper: "", cash: "", downside: "", days: "" }

export function RecommendationSetup({ transport, scope }: { transport: ProductTransport; scope: ProductScope }) {
  const [draft, setDraft] = React.useState<Draft>(emptyDraft)
  const [preview, setPreview] = React.useState<RecommendationSetupPreview | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [notice, setNotice] = React.useState<string | null>(null)
  const status = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "recommendationSetup", {}),
    queryFn: async () => parseRecommendationSetup(await transport.recommendationSetup({ action: "status" })),
  })
  const prepare = useMutation({
    mutationFn: async ({ snapshot, request }: {
      snapshot: RecommendationSetupStatus
      request: Extract<RecommendationSetupRequest, { action: "preview" }>
    }) => parseRecommendationPreview(await transport.recommendationSetup(request), snapshot, request),
    onSuccess: setPreview,
    onError: (failure) => setError(messageFrom(failure)),
  })
  const commit = useMutation({
    mutationFn: async (exact: RecommendationSetupPreview) => {
      if (BigInt(exact.expiresAtUnixNanos) <= BigInt(Date.now()) * 1_000_000n) {
        throw new Error("This review has expired. Review your preferences again.")
      }
      return parseRecommendationCommit(await transport.recommendationSetup({
        action: "commit", previewId: exact.previewId, previewDigest: exact.previewDigest,
      }, true), exact)
    },
    onSuccess: async () => {
      setPreview(null)
      setNotice("Your account and allocation preferences were saved. Current readiness is shown below.")
      setDraft(emptyDraft)
      await status.refetch()
    },
    onError: async (failure) => {
      setPreview(null)
      setError(messageFrom(failure))
      await status.refetch()
    },
  })
  React.useEffect(() => {
    if (!preview) return
    const remaining = Number(BigInt(preview.expiresAtUnixNanos) / 1_000_000n) - Date.now()
    const timer = window.setTimeout(() => {
      setPreview(null)
      setError("This review has expired. Review your preferences again.")
    }, Math.max(0, Math.min(remaining, 2_147_483_647)))
    return () => window.clearTimeout(timer)
  }, [preview])

  const snapshot = status.data
  const selected = snapshot?.portfolioCatalog.accounts.find((account) => account.accountId === draft.accountId)
  const busy = status.isFetching || prepare.isPending || commit.isPending
  const previewCurrent = preview && snapshot
    && preview.workspaceId === snapshot.workspaceId
    && preview.currentRevision === snapshot.authority.revision
    && preview.currentAuthorityDigest === snapshot.authority.digest
    && preview.catalogDigest === snapshot.portfolioCatalog.digest
  const update = (key: keyof Draft, value: string) => {
    setPreview(null)
    setNotice(null)
    setError(null)
    setDraft((prior) => ({ ...prior, [key]: value, ...(key === "accountId" ? { cash: "" } : {}) }))
  }

  return <section className="mt-5 rounded-xl border border-border bg-card/35 p-5" aria-labelledby="recommendation-preferences">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h2 id="recommendation-preferences" className="text-lg font-semibold">Recommendation preferences</h2>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Choose the account and allocation limits to use when analyzing investments. Review the saved choices before confirming them.
        </p>
      </div>
      <Button variant="outline" disabled={busy} onClick={() => {
        setPreview(null)
        setError(null)
        void status.refetch()
      }}>Refresh preferences</Button>
    </div>
    {status.isPending ? <p className="mt-4 text-sm" role="status">Loading saved preferences…</p>
      : status.isError ? <p className="mt-4 text-sm text-destructive" role="alert">
          Preferences are unavailable. Refresh to check the current account and saved choices.
        </p>
      : snapshot ? <>
        <p className="mt-4 text-sm" role="status">{setupMessage(snapshot)}</p>
        {snapshot.accountSelection && snapshot.allocationProfile ? <details className="mt-3 rounded-lg border border-border p-3">
          <summary className="cursor-pointer text-sm font-medium">Saved account and preferences</summary>
          <PreferenceSummary accountName={accountName(snapshot, snapshot.accountSelection.accountId)} allocation={snapshot.allocationProfile} />
          <p className="mt-3 text-xs text-muted-foreground">Review due {preferenceTime(snapshot.allocationProfile.reviewDueAtUnixNanos)}.</p>
        </details> : null}
        {snapshot.portfolioCatalog.accounts.length === 0 ? <div className="mt-4 space-y-3">
          <p className="text-sm text-muted-foreground">
            Import your portfolio below, or start a practice portfolio with virtual money. Then choose the account and investment limits to use for recommendations.
          </p>
          <Button asChild variant="outline"><Link to="/paper-execution">Start a practice portfolio</Link></Button>
        </div> : <form className="mt-5" onSubmit={(event) => {
          event.preventDefault()
          setError(null)
          setNotice(null)
          setPreview(null)
          try {
            if (!selected) throw new Error("Choose an account for these preferences.")
            const lower = parsePreferencePercent(draft.lower)
            const upper = parsePreferencePercent(draft.upper)
            if (lower > upper) throw new Error("The lower position percentage must not exceed the upper percentage.")
            if (!/^[1-9]\d{0,3}$/.test(draft.days) || Number(draft.days) > 3650) {
              throw new Error("Enter a whole investment horizon from 1 to 3650 days.")
            }
            prepare.mutate({ snapshot, request: {
              action: "preview",
              expectedRevision: snapshot.authority.revision,
              accountId: selected.accountId,
              allocationProfile: {
                preferredPositionWeightLowerBps: lower,
                preferredPositionWeightUpperBps: upper,
                minimumCashReserve: { amount: normalizeAmount(draft.cash), currency: selected.reportingCurrency },
                maximumDownsideLossBpsOfMarkedEquity: parsePreferencePercent(draft.downside),
                availableInvestmentHorizonDays: Number(draft.days),
              },
            } })
          } catch (failure) {
            setError(messageFrom(failure))
          }
        }}>
          <fieldset disabled={busy} className="space-y-4">
            <legend className="mb-3 text-sm font-medium">Choose preferences to review</legend>
            <div>
              <Label htmlFor="recommendation-account">Account for investment analysis</Label>
              <select id="recommendation-account" required value={draft.accountId} onChange={(event) => update("accountId", event.target.value)}
                className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm">
                <option value="">Choose an account</option>
                {snapshot.portfolioCatalog.accounts.map((account) => <option key={account.accountId} value={account.accountId}>
                  {account.displayName} · {account.reportingCurrency}
                </option>)}
              </select>
              {selected?.availableAtUnixNanos === null ? <p className="mt-2 text-xs text-muted-foreground">
                This account's portfolio evidence is unavailable. Saving preferences will not make investment analysis ready until that information is available.
              </p> : null}
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <PreferenceInput label="Preferred position size — lower (%)" name="lower" value={draft.lower} update={update} />
              <PreferenceInput label="Preferred position size — upper (%)" name="upper" value={draft.upper} update={update} />
              <PreferenceInput label={`Minimum cash reserve${selected ? ` (${selected.reportingCurrency})` : ""}`} name="cash" value={draft.cash} update={update} />
              <PreferenceInput label="Maximum downside loss (% of portfolio equity)" name="downside" value={draft.downside} update={update} />
              <PreferenceInput label="Available investment horizon (days)" name="days" value={draft.days} update={update} />
            </div>
            <p className="text-xs leading-5 text-muted-foreground">
              Position size is a percentage of portfolio equity. Enter percentages from 0.01 to 100, a nonnegative cash reserve, and a whole horizon from 1 to 3650 days. These preferences do not authorize orders.
            </p>
            <Button type="submit" disabled={!selected || busy}>{prepare.isPending ? "Preparing review…" : "Review preferences"}</Button>
          </fieldset>
        </form>}
      </> : null}
    {notice ? <p className="mt-4 text-sm" role="status">{notice}</p> : null}
    {error ? <p className="mt-4 text-sm text-destructive" role="alert">{error}</p> : null}
    <Dialog open={preview !== null} onOpenChange={(open) => { if (!open && !commit.isPending) setPreview(null) }}>
      <DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>Save these recommendation preferences?</DialogTitle>
          <DialogDescription>These are the account and allocation preferences returned for your review. Confirming replaces the previous recommendation setup.</DialogDescription>
        </DialogHeader>
        {preview ? <>
          <PreferenceSummary accountName={snapshot ? accountName(snapshot, preview.accountSelection.accountId) : "Unavailable account"} allocation={preview.allocationProfile} />
          <p className="text-xs text-muted-foreground">This review expires {preferenceTime(preview.expiresAtUnixNanos)}.</p>
          {!previewCurrent ? <p role="alert" className="text-sm text-destructive">The saved account information changed. Close this review, refresh, and review again.</p> : null}
        </> : null}
        <DialogFooter>
          <Button variant="outline" disabled={commit.isPending} onClick={() => setPreview(null)}>Keep current preferences</Button>
          <Button disabled={!previewCurrent || busy} onClick={() => { if (preview && previewCurrent) commit.mutate(preview) }}>
            {commit.isPending ? "Saving…" : "Confirm preferences"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </section>
}

function PreferenceInput({ label, name, value, update }: {
  label: string; name: Exclude<keyof Draft, "accountId">; value: string
  update: (key: keyof Draft, value: string) => void
}) {
  const id = `recommendation-${name}`
  return <div>
    <Label htmlFor={id}>{label}</Label>
    <Input id={id} className="mt-2" required inputMode={name === "days" ? "numeric" : "decimal"}
      autoComplete="off" maxLength={name === "cash" ? 128 : 6} value={value}
      onChange={(event) => update(name, event.target.value)} />
  </div>
}

function accountName(status: RecommendationSetupStatus, accountId: string): string {
  return status.portfolioCatalog.accounts.find((account) => account.accountId === accountId)?.displayName ?? "Unavailable account"
}

function PreferenceSummary({ accountName, allocation }: { accountName: string; allocation: RecommendationAllocation }) {
  const horizon = BigInt(allocation.availableInvestmentHorizonNanos)
  const day = 86_400_000_000_000n
  return <dl className="mt-3 space-y-3 text-sm">
    <div><dt className="text-muted-foreground">Account</dt><dd>{accountName}</dd></div>
    <div><dt className="text-muted-foreground">Preferred position size</dt><dd>{formatPreferencePercent(allocation.preferredPositionWeightLowerBps)}–{formatPreferencePercent(allocation.preferredPositionWeightUpperBps)} of portfolio equity</dd></div>
    <div><dt className="text-muted-foreground">Minimum cash reserve</dt><dd>{formatMoney(allocation.minimumCashReserve)}</dd></div>
    <div><dt className="text-muted-foreground">Maximum downside loss</dt><dd>{formatPreferencePercent(allocation.maximumDownsideLossBpsOfMarkedEquity)} of portfolio equity</dd></div>
    <div><dt className="text-muted-foreground">Investment horizon</dt><dd>{horizon % day === 0n ? `${horizon / day} days` : `${Number(horizon) / Number(day)} days`}</dd></div>
  </dl>
}

function setupMessage(status: RecommendationSetupStatus): string {
  if (status.state === "ready") return "Your recommendation preferences are ready."
  switch (status.setupRequiredReason) {
    case "no_default_account": return "Choose an account and review your allocation preferences before analyzing investments."
    case "ambiguous_accounts": return "Choose which account investment recommendations should use."
    case "portfolio_evidence_unavailable": return "The selected account needs available portfolio information before investment analysis can continue."
    case "profile_review_required": return "Review and confirm your allocation preferences before continuing investment analysis."
    default: return "Review your recommendation account and preferences before continuing."
  }
}
