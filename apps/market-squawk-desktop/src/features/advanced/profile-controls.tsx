import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { CircleAlert, Copy, History, RefreshCw } from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { formatUnixNanos } from "@/features/opportunities/format"
import type { ProductTransport } from "@/lib/transport"

import type {
  AnalyticalControllerRequest,
  AnalyticalControllerResponse,
  AnalyticalProfile,
  ProfileOptions,
} from "./analytical-profile-contracts"
import { useAnalyticalControllerStatus } from "./use-analytical-profile"
import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"
import { FinancialPreferenceControls, RequiredAnalysisSettings, preferenceInput } from "./financial-preference-controls"

type ControllerTransport = Pick<ProductTransport, "analyticalController">

export function ProfileControls({ transport, scope }: {
  transport: ControllerTransport
  scope: ProductScope
}) {
  const queryClient = useQueryClient()
  const [name, setName] = useState("")
  const [confirmation, setConfirmation] = useState<AnalyticalControllerRequest | null>(null)
  const [historyVisible, setHistoryVisible] = useState(false)
  const key = productKeys.operation(scope, "analysis", "Desktop.AnalyticalProfiles", {})
  const profiles = useAnalyticalControllerStatus(transport, scope)
  const optionsNavigation = useCursorNavigation()
  const options = useQuery({
    queryKey: [...key, "options", { cursor: optionsNavigation.after, limit: 25 }],
    gcTime: 0,
    queryFn: async ({ signal }) => {
      const response = await transport.analyticalController({ action: "profileOptions", cursor: optionsNavigation.after, limit: 25 }, false, { signal })
      if (response.kind !== "profile_options") throw new Error("Analysis choices could not be opened.")
      return response.options
    },
  })
  const change = useMutation({
    mutationFn: (request: AnalyticalControllerRequest) =>
      transport.analyticalController(request, true),
    onSuccess: async (_response, request) => {
      setConfirmation(null)
      if (request.action === "copyRecommended") setName("")
      if (request.action === "compareWithRecommended") return
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: key, exact: true }),
        queryClient.invalidateQueries({ queryKey: [...key, "history"] }),
        queryClient.invalidateQueries({
          queryKey: productKeys.operation(scope, "analysis", "Analysis.GetSettingsSummary", {}),
          exact: true,
        }),
      ])
    },
  })

  return (
    <section className="mt-6 border-t border-border pt-6" aria-labelledby="analysis-profiles-heading">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 id="analysis-profiles-heading" className="text-lg font-semibold">Analysis profiles</h2>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            Keep the recommended settings or save your own analysis preferences. Each investment
            brief keeps the profile used to create it.
          </p>
        </div>
        <Button variant="outline" size="sm" disabled={profiles.isFetching}
          onClick={() => void profiles.refetch()}>
          <RefreshCw aria-hidden="true" /> Refresh settings
        </Button>
      </div>
      {profiles.isPending ? <Skeleton className="mt-4 h-36 w-full" />
        : profiles.isError ? <ProfileError />
          : <>
            {profiles.data.profileRecoveryNotice ? <Alert className="mt-4">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>Earlier profiles are unavailable</AlertTitle>
              <AlertDescription>{profiles.data.profileRecoveryNotice}</AlertDescription>
            </Alert> : null}
            <form className="mt-5 flex flex-wrap items-end gap-3" onSubmit={(event) => {
              event.preventDefault()
              if (name.trim()) change.mutate({ action: "copyRecommended", displayName: name.trim() })
            }}>
              <label className="grid min-w-56 gap-2 text-xs font-medium">
                New profile name
                <Input value={name} maxLength={64} placeholder="My analysis preferences"
                  onChange={(event) => setName(event.target.value)}
                  disabled={change.isPending || !profiles.data.canCreateCustomProfile} />
              </label>
              <Button type="submit" variant="outline"
                disabled={change.isPending || !name.trim() || !profiles.data.canCreateCustomProfile}>
                <Copy aria-hidden="true" /> Copy recommended settings
              </Button>
            </form>
            <div className="mt-5 space-y-4">
              {profiles.data.profiles.map((profile) => (
                <ProfileEditor key={`${profile.profileToken}:${profile.profileStateToken}:${profile.active}:${profile.activatedAt ?? ""}`}
                  profile={profile} busy={change.isPending}
                  options={options.data}
                  activationToken={profiles.data.activeProfile.activationToken}
                  onChange={(request) => change.mutate(request)}
                  onConfirm={setConfirmation} />
              ))}
            </div>
            {options.isError ? <p className="mt-4 text-xs text-muted-foreground">
              Available models and required settings could not be refreshed.
              <Button variant="link" size="sm" onClick={() => void options.refetch()}>Retry available choices</Button>
            </p> : null}
            <p className="mt-4 text-xs text-muted-foreground">Browse available forecast models. Each profile keeps its selected model when the choice page changes.</p>
            <CursorNavigation navigation={optionsNavigation} next={options.data?.nextCursor} busy={options.isFetching || change.isPending} error={options.isError}
              onRestart={() => { if (optionsNavigation.after === undefined) void options.refetch() }} />
            <RequiredAnalysisSettings options={options.data} />
          </>}
      {change.isError ? <ProfileError /> : null}
      {change.data?.kind === "comparison" ? <ProfileComparison comparison={change.data} options={options.data} /> : null}
      {confirmation ? (
        <section className="mt-5 rounded-lg border border-primary/35 bg-primary/5 p-4"
          role="region" aria-label="Confirm analysis settings">
          <h3 className="text-sm font-semibold">
            {confirmation.action === "restoreRecommended" ? "Restore recommended settings?" : "Use this profile for new analyses?"}
          </h3>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            New analyses will use these settings. Saved and running analyses keep their original profile.
          </p>
          <div className="mt-3 flex gap-2">
            <Button disabled={change.isPending} onClick={() => change.mutate(confirmation)}>Confirm settings</Button>
            <Button variant="outline" disabled={change.isPending} onClick={() => setConfirmation(null)}>Keep current settings</Button>
          </div>
        </section>
      ) : null}
      <Button className="mt-5" variant="ghost" onClick={() => setHistoryVisible(!historyVisible)}
        aria-expanded={historyVisible}>
        <History aria-hidden="true" /> {historyVisible ? "Hide profile history" : "View profile history"}
      </Button>
      {historyVisible ? <ProfileHistoryRead transport={transport} scope={scope} /> : null}
    </section>
  )
}

function ProfileHistoryRead({ transport, scope }: {
  transport: ControllerTransport
  scope: ProductScope
}) {
  const navigation = useCursorNavigation()
  const history = useQuery({
    queryKey: [...productKeys.operation(scope, "analysis", "Desktop.AnalyticalProfiles", {}), "history", navigation.after],
    gcTime: 0,
    queryFn: async ({ signal }) => {
      const response = await transport.analyticalController({ action: "history", afterToken: navigation.after, limit: 20 }, false, { signal })
      if (response.kind !== "history") throw new Error("Profile history could not be opened.")
      return response
    },
  })
  return <div className="mt-3 rounded-lg border border-border p-4">
    {history.isPending ? <Skeleton className="h-24 w-full" /> : history.isError ? <ProfileError /> : <ol className="space-y-3">
      {history.data.entries.map((entry) => <li key={entry.historyToken} className="flex flex-wrap justify-between gap-2 text-xs">
        <span><strong>{entry.profileName}</strong> · {historyLabel(entry.action)}</span>
        <span className="text-muted-foreground">{formatUnixNanos(entry.recordedAt)}</span>
      </li>)}
    </ol>}
    <CursorNavigation navigation={navigation} next={history.data?.nextAfterToken} busy={history.isFetching}
      onRestart={() => { if (navigation.after === undefined) void history.refetch() }} />
  </div>
}

function ProfileEditor({ profile, busy, options, activationToken, onChange, onConfirm }: {
  profile: AnalyticalProfile
  busy: boolean
  options: ProfileOptions | undefined
  activationToken: string | null
  onChange: (request: AnalyticalControllerRequest) => void
  onConfirm: (request: AnalyticalControllerRequest) => void
}) {
  const [name, setName] = useState(profile.displayName)
  const [breadth, setBreadth] = useState(profile.analysisScope)
  const [preferences, setPreferences] = useState(() => preferenceInput(profile.financialPreferences))
  const changed = name.trim() !== profile.displayName || breadth !== profile.analysisScope
    || JSON.stringify(preferences) !== JSON.stringify(preferenceInput(profile.financialPreferences))
  return (
    <article className="rounded-xl border border-border bg-card/40 p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">{profile.displayName}</h3>
        <span className="text-xs text-muted-foreground">Version {profile.version} · {profile.active ? "Active" : profile.validation.label}</span>
      </div>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">{profile.validation.explanation}</p>
      {profile.canEdit ? <div className="mt-4 grid gap-3 sm:grid-cols-2">
        <label className="grid gap-2 text-xs font-medium">Profile name
          <Input value={name} maxLength={64} disabled={busy} onChange={(event) => setName(event.target.value)} />
        </label>
        <label className="grid gap-2 text-xs font-medium">Discovery breadth
          <select className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            value={breadth} disabled={busy} onChange={(event) => {
              const value = event.target.value
              if (value === "focused" || value === "balanced" || value === "broad") setBreadth(value)
            }}>
            <option value="focused">Focused — up to 8 full analyses</option>
            <option value="balanced">Balanced — up to 16 full analyses</option>
            <option value="broad">Broad — up to 32 full analyses</option>
          </select>
        </label>
        <p className="text-xs leading-5 text-muted-foreground sm:col-span-2">
          Every selected investment receives the full analysis. Discovery reports investments outside
          this limit in its coverage summary.
        </p>
      </div> : null}
      <FinancialPreferenceControls preferences={profile.financialPreferences}
        values={preferences} options={options} disabled={busy || !profile.canEdit} onChange={setPreferences} />
      <div className="mt-4 flex flex-wrap gap-2">
        {profile.canEdit ? <Button size="sm" variant="outline" disabled={busy || !changed || !name.trim()}
          onClick={() => onChange({ action: "updateProfile", profileToken: profile.profileToken,
            profileStateToken: profile.profileStateToken, displayName: name.trim(), analysisScope: breadth,
            financialPreferences: preferences })}>Save preferences</Button> : null}
        {profile.mode === "custom" ? <Button size="sm" variant="outline" disabled={busy || changed}
          onClick={() => onChange({ action: "compareWithRecommended", profileToken: profile.profileToken })}>Compare with recommended</Button> : null}
        {profile.canValidate ? <Button size="sm" variant="outline" disabled={busy || changed}
          onClick={() => onChange({ action: "validateProfile", profileToken: profile.profileToken,
            profileStateToken: profile.profileStateToken })}>Validate settings</Button> : null}
        {profile.canActivate && profile.validationToken && activationToken ? <Button size="sm" disabled={busy || changed}
          onClick={() => onConfirm({ action: "activateProfile", profileToken: profile.profileToken,
            profileStateToken: profile.profileStateToken, validationToken: profile.validationToken!,
            activationToken })}>Use this profile</Button> : null}
        {profile.canRestoreRecommended && profile.activationToken ? <Button size="sm" variant="outline" disabled={busy}
          onClick={() => onConfirm({ action: "restoreRecommended", activationToken: profile.activationToken! })}>Restore recommended</Button> : null}
      </div>
    </article>
  )
}

function ProfileComparison({ comparison, options }: {
  comparison: Extract<AnalyticalControllerResponse, { kind: "comparison" }>
  options: ProfileOptions | undefined
}) {
  const selected = comparison.selectedProfile
  const recommended = comparison.recommendedProfile
  const rows: { label: string; selected: string; recommended: string }[] = []
  if (selected.analysisScope !== recommended.analysisScope) rows.push({
    label: "Discovery breadth", selected: scopeLabel(selected.analysisScope),
    recommended: scopeLabel(recommended.analysisScope),
  })
  if (selected.financialPreferences.coverage !== recommended.financialPreferences.coverage) rows.push({
    label: "Investment coverage", selected: coverageLabel(selected.financialPreferences.coverage),
    recommended: coverageLabel(recommended.financialPreferences.coverage),
  })
  if (selected.financialPreferences.modelChoice !== recommended.financialPreferences.modelChoice) rows.push({
    label: "Forecast model", selected: modelLabel(selected.financialPreferences.modelChoice, options),
    recommended: modelLabel(recommended.financialPreferences.modelChoice, options),
  })
  if (selected.financialPreferences.allowRetrospectiveStudies !== recommended.financialPreferences.allowRetrospectiveStudies) rows.push({
    label: "Simulations using today’s historical data",
    selected: selected.financialPreferences.allowRetrospectiveStudies ? "Allowed" : "Require data known at the time",
    recommended: recommended.financialPreferences.allowRetrospectiveStudies ? "Allowed" : "Require data known at the time",
  })
  for (const field of selected.financialPreferences.fields) {
    const original = recommended.financialPreferences.fields.find((item) => item.key === field.key)
    if (original && field.value !== original.value) rows.push({
      label: field.label, selected: preferenceLabel(field), recommended: preferenceLabel(original),
    })
  }
  return <section className="mt-5 rounded-lg border border-border p-4" aria-live="polite">
    <h3 className="text-sm font-semibold">{comparison.selectedProfile.displayName} compared with recommended</h3>
    {comparison.equivalent ? <p className="mt-2 text-xs text-muted-foreground">These profiles use the same analysis settings.</p>
      : <>
        <ul className="mt-3 space-y-2 text-xs">{comparison.differences.map((difference) => (
          <li key={difference.label}><strong>{difference.label}:</strong> {difference.explanation}</li>
        ))}</ul>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-left text-xs">
            <caption className="sr-only">Saved preference differences</caption>
            <thead><tr className="border-b border-border">
              <th className="py-2 pr-4 font-medium">Preference</th>
              <th className="py-2 pr-4 font-medium">Recommended</th>
              <th className="py-2 font-medium">{selected.displayName}</th>
            </tr></thead>
            <tbody>{rows.map((row) => <tr key={row.label} className="border-b border-border/50 last:border-0">
              <th className="py-2 pr-4 font-normal">{row.label}</th>
              <td className="py-2 pr-4 text-muted-foreground">{row.recommended}</td>
              <td className="py-2">{row.selected}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </>}
  </section>
}

function scopeLabel(scope: AnalyticalProfile["analysisScope"]) {
  return { focused: "Up to 8 full analyses", balanced: "Up to 16 full analyses", broad: "Up to 32 full analyses" }[scope]
}

function coverageLabel(coverage: AnalyticalProfile["financialPreferences"]["coverage"]) {
  return coverage === "stocks" ? "Listed stocks" : "Listed stocks and ETFs"
}

function modelLabel(token: string, options: ProfileOptions | undefined) {
  return token === "recommended" ? "Recommended calibrated forecast"
    : options?.modelChoices.find((model) => model.token === token)?.label ?? "Selected model"
}

function preferenceLabel(field: AnalyticalProfile["financialPreferences"]["fields"][number]) {
  const choice = field.choices.find((choice) => choice.value === field.value)
  return choice?.label ?? `${field.value}${field.unit === "%" ? "%" : field.unit ? ` ${field.unit}` : ""}`
}

function ProfileError() {
  return <Alert className="mt-4" variant="destructive"><CircleAlert aria-hidden="true" />
    <AlertTitle>Analysis settings could not be updated</AlertTitle>
    <AlertDescription>Refresh the settings and try again.</AlertDescription>
  </Alert>
}

function historyLabel(action: string) {
  const labels: Record<string, string> = {
    recommended_initialized: "Recommended settings added", custom_created: "Custom profile created",
    custom_updated: "Preferences changed", validation_unavailable: "Validation unavailable",
    custom_validated: "Settings validated", custom_activated: "Profile activated",
    recommended_restored: "Recommended settings restored",
  }
  return labels[action] ?? "Profile updated"
}
