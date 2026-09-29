import { useId } from "react"

import { Input } from "@/components/ui/input"

import type { FinancialPreferences, FinancialPreferencesInput, ProfileOptions } from "./analytical-profile-contracts"

export function FinancialPreferenceControls({ preferences, values, options, disabled, onChange }: {
  preferences: FinancialPreferences
  values: FinancialPreferencesInput
  options: ProfileOptions | undefined
  disabled: boolean
  onChange: (values: FinancialPreferencesInput) => void
}) {
  const historicalDataExplanationId = useId()
  const groups = [...new Set(preferences.fields.map((field) => field.group))]
  const modelChoices = options?.modelChoices ?? [
    { token: "recommended", label: "Recommended calibrated forecast" },
  ]
  const modelListed = modelChoices.some((model) => model.token === values.modelChoice)
  return <div className="mt-4 space-y-4">
    <div className="grid gap-3 sm:grid-cols-2">
      <label className="grid gap-2 text-xs font-medium">Investments to consider
        <select className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={values.coverage} disabled={disabled} onChange={(event) => {
            const coverage = event.target.value
            if (coverage === "stocks" || coverage === "stocks_and_etfs") onChange({ ...values, coverage })
          }}>
          <option value="stocks_and_etfs">Stocks and exchange-traded funds</option>
          <option value="stocks">Stocks</option>
        </select>
      </label>
      <label className="grid gap-2 text-xs font-medium">Forecast model
        <select className="h-9 min-w-0 rounded-md border border-input bg-background px-3 text-sm"
          value={values.modelChoice} disabled={disabled || !options}
          onChange={(event) => onChange({ ...values, modelChoice: event.target.value })}>
          {modelChoices.map((model) => <option key={model.token} value={model.token}>{model.label}</option>)}
          {!modelListed ? <option value={values.modelChoice}>{options ? "Previously selected model — unavailable" : "Selected model"}</option> : null}
        </select>
      </label>
    </div>
    <div className="rounded-lg border border-border/80 bg-background/25 p-3">
      <label className="flex items-start gap-2 text-xs font-medium">
        <input type="checkbox" className="mt-0.5 accent-primary"
          checked={values.allowRetrospectiveStudies} disabled={disabled}
          aria-describedby={historicalDataExplanationId}
          onChange={(event) => onChange({ ...values, allowRetrospectiveStudies: event.target.checked })} />
        <span>Allow simulations using today’s historical data</span>
      </label>
      <p id={historicalDataExplanationId} className="mt-2 text-xs leading-5 text-muted-foreground">
        Historical simulations can use data available today, including later revisions.
        Results may differ from what was knowable at the time and do not establish live performance.
        Turn this off to require data known at each historical decision.
      </p>
    </div>
    <details className="rounded-lg border border-border/80 bg-background/25 p-3">
      <summary className="cursor-pointer text-xs font-medium">Confidence, risk, and calculation preferences</summary>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        These thresholds govern whether evidence qualifies for an investment recommendation.
        Evidence weights must total 100%, and forecast and valuation contributions must total 100%.
        Price-range weights control interpolation between the saved lower and upper estimates.
      </p>
      {groups.map((group) => <fieldset key={group} className="mt-5 border-t border-border/60 pt-4">
        <legend className="px-1 text-xs font-semibold">{group}</legend>
        <div className="grid gap-3 pt-2 sm:grid-cols-2 xl:grid-cols-3">
          {preferences.fields.filter((field) => field.group === group).map((field) => {
            const value = values.fields.find((value) => value.key === field.key)?.value ?? ""
            const update = (value: string) => onChange({ ...values,
              fields: values.fields.map((entry) => entry.key === field.key ? { ...entry, value } : entry),
            })
            return <label key={field.key} className="grid gap-2 text-xs text-muted-foreground">
              <span>{field.label}{field.unit ? ` (${field.unit})` : ""}</span>
              {field.choices.length ? <select
                className="h-9 rounded-md border border-input bg-background px-3 text-sm text-foreground"
                value={value} disabled={disabled} onChange={(event) => update(event.target.value)}>
                {field.choices.map((choice) => <option key={choice.value} value={choice.value}>{choice.label}</option>)}
              </select> : <Input value={value} maxLength={32}
                inputMode={field.unit === "daily returns" ? "numeric" : "decimal"} disabled={disabled}
                className="text-foreground" onChange={(event) => update(event.target.value)} />}
            </label>
          })}
        </div>
      </fieldset>)}
    </details>
  </div>
}

export function RequiredAnalysisSettings({ options }: { options: ProfileOptions | undefined }) {
  if (!options) return null
  return <details className="mt-5 rounded-lg border border-border bg-background/20 p-4">
    <summary className="cursor-pointer text-xs font-medium">Required analysis settings</summary>
    <dl className="mt-4 grid gap-4 sm:grid-cols-2">
      {options.fixedSettings.map((setting) => <div key={setting.label}>
        <dt className="text-xs font-semibold">{setting.label}</dt>
        <dd className="mt-1 text-xs leading-5 text-muted-foreground">
          <span className="block text-foreground">{setting.value}</span>{setting.explanation}
        </dd>
      </div>)}
    </dl>
  </details>
}

export function preferenceInput(preferences: FinancialPreferences): FinancialPreferencesInput {
  return { coverage: preferences.coverage, modelChoice: preferences.modelChoice,
    allowRetrospectiveStudies: preferences.allowRetrospectiveStudies,
    fields: preferences.fields.map(({ key, value }) => ({ key, value })),
  }
}
