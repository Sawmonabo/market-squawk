import * as React from "react"

import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

const states = [
  ["01", "Alabama"], ["02", "Alaska"], ["04", "Arizona"], ["05", "Arkansas"],
  ["06", "California"], ["08", "Colorado"], ["09", "Connecticut"], ["10", "Delaware"],
  ["11", "District of Columbia"], ["12", "Florida"], ["13", "Georgia"], ["15", "Hawaii"],
  ["16", "Idaho"], ["17", "Illinois"], ["18", "Indiana"], ["19", "Iowa"],
  ["20", "Kansas"], ["21", "Kentucky"], ["22", "Louisiana"], ["23", "Maine"],
  ["24", "Maryland"], ["25", "Massachusetts"], ["26", "Michigan"], ["27", "Minnesota"],
  ["28", "Mississippi"], ["29", "Missouri"], ["30", "Montana"], ["31", "Nebraska"],
  ["32", "Nevada"], ["33", "New Hampshire"], ["34", "New Jersey"], ["35", "New Mexico"],
  ["36", "New York"], ["37", "North Carolina"], ["38", "North Dakota"], ["39", "Ohio"],
  ["40", "Oklahoma"], ["41", "Oregon"], ["42", "Pennsylvania"], ["44", "Rhode Island"],
  ["45", "South Carolina"], ["46", "South Dakota"], ["47", "Tennessee"], ["48", "Texas"],
  ["49", "Utah"], ["50", "Vermont"], ["51", "Virginia"], ["53", "Washington"],
  ["54", "West Virginia"], ["55", "Wisconsin"], ["56", "Wyoming"],
] as const
const selectStyle = "mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm"

export function CensusSelectionFields() {
  const [advanced, setAdvanced] = React.useState(false)
  return <fieldset className="space-y-4 rounded-lg border border-border p-4">
    <legend className="px-2 text-sm">Census data selection</legend>
    <input type="hidden" name="census-mode" value={advanced ? "advanced" : "quarterly_workforce"} />
    <label className="flex items-center gap-2 text-sm">
      <input type="checkbox" checked={advanced} onChange={(event) => setAdvanced(event.target.checked)} />
      Use advanced dataset settings
    </label>
    {advanced ? <>
      <p className="text-sm leading-6 text-muted-foreground">
        Enter a key-free Census configuration for a published dataset. It can specify a year or time series,
        variables or a group, typed filters, standard or uniform geography, time ranges, variable mappings,
        and reported or fixed time coordinates. Published metadata and the connection service validate the selection.
      </p>
      <Label htmlFor="census-configuration">Dataset configuration (JSON)</Label>
      <textarea id="census-configuration" name="census-configuration" required rows={14}
        maxLength={128 * 1024} autoComplete="off" spellCheck={false}
        className="w-full rounded-md border border-input bg-background p-3 font-mono text-xs"
        aria-describedby="census-configuration-help" />
      <p id="census-configuration-help" className="text-xs leading-5 text-muted-foreground">
        Supply dataset, selection, predicates, geography, mappings, and effectiveTime in one configuration object; time may be omitted or null.
        Do not include an API key, URL, or the outer activation request. Your saved credential is used separately.
      </p>
    </> : <>
      <p className="text-sm leading-6 text-muted-foreground">
        Import beginning-of-quarter employment from Quarterly Workforce Indicators for one state and quarter,
        covering all ages and sexes. Counts retain the published quarter and are measured in persons.
      </p>
      <div>
        <Label htmlFor="census-state">State</Label>
        <select id="census-state" name="census-state" required defaultValue="" className={selectStyle}>
          <option value="">Choose a state</option>
          {states.map(([code, name]) => <option key={code} value={code}>{name}</option>)}
        </select>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <div>
          <Label htmlFor="census-year">Year</Label>
          <Input id="census-year" name="census-year" className="mt-2" required type="number"
            min={1900} max={new Date().getUTCFullYear()} step={1} autoComplete="off" />
        </div>
        <div>
          <Label htmlFor="census-quarter">Quarter</Label>
          <select id="census-quarter" name="census-quarter" required defaultValue="" className={selectStyle}>
            <option value="">Choose a quarter</option>
            <option value="1">First quarter (January–March)</option>
            <option value="2">Second quarter (April–June)</option>
            <option value="3">Third quarter (July–September)</option>
            <option value="4">Fourth quarter (October–December)</option>
          </select>
        </div>
      </div>
      <p className="text-xs leading-5 text-muted-foreground">
        Availability depends on the published data. The economic overview currently displays the California series;
        other selected states are retained as separate datasets.
      </p>
    </>}
  </fieldset>
}

export function censusConfiguration(data: FormData): Record<string, unknown> {
  if (data.get("census-mode") === "advanced") {
    const text = String(data.get("census-configuration") ?? "")
    if (text.length > 128 * 1024) throw new Error("The Census configuration is too large.")
    let value: unknown
    try { value = JSON.parse(text) }
    catch { throw new Error("Enter a valid JSON configuration in advanced dataset settings.") }
    const fields = ["dataset", "selection", "predicates", "geography", "time", "mappings", "effectiveTime"]
    if (typeof value !== "object" || value === null || Array.isArray(value)
      || Object.keys(value).some((key) => !fields.includes(key))
      || fields.some((key) => key !== "time" && !Object.hasOwn(value, key))) {
      throw new Error("Supply the dataset configuration fields shown below, without an outer request or credentials.")
    }
    // Preserve the complete native configuration; backend adapter constructors remain its authority.
    return value as Record<string, unknown>
  }
  if (data.get("census-mode") !== "quarterly_workforce") throw new Error("Choose a Census data selection.")
  const state = String(data.get("census-state") ?? "")
  const yearText = String(data.get("census-year") ?? "")
  const quarterText = String(data.get("census-quarter") ?? "")
  if (!states.some(([code]) => code === state)) throw new Error("Choose a state.")
  if (!/^\d{4}$/.test(yearText) || Number(yearText) < 1900 || Number(yearText) > new Date().getUTCFullYear()) {
    throw new Error("Choose a year from 1900 through the current year.")
  }
  if (!/^[1-4]$/.test(quarterText)) throw new Error("Choose a quarter.")
  return {
    dataset: { vintage: { kind: "time_series" }, path: "qwi/sa" },
    selection: { kind: "variables", primary: ["Emp"], wire: ["Emp"] },
    predicates: [
      { variable: "agegrp", predicateType: "string", values: ["A00"] },
      { variable: "sex", predicateType: "string", values: ["0"] },
    ],
    geography: { kind: "standard", forClause: { level: "state", codes: [state] }, inClauses: [] },
    time: { kind: "at", point: { kind: "quarter", year: Number(yearText), quarter: Number(quarterText) } },
    mappings: [{ variable: "Emp", seriesNamespace: "macro.employment.beginning-quarter", unit: "persons" }],
    effectiveTime: { kind: "require_reported_time" },
  }
}
