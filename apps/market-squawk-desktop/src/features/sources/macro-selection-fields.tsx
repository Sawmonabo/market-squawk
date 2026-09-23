import * as React from "react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

type Provider = "bea" | "census"

export function MacroSelectionFields({ provider }: { provider: Provider }) {
  const [rows, setRows] = React.useState([0])
  const sequence = React.useRef(1)
  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Choose the exact datasets and periods to retain. Their published metadata determines units,
        missing values and time definitions. Saved selections are reused when you reconnect.
      </p>
      {rows.map((id) => (
        <fieldset key={id} className="space-y-3 rounded-lg border border-border p-4">
          <legend className="px-2 text-sm">Dataset {rows.indexOf(id) + 1}</legend>
          <input type="hidden" name="macro-row" value={id} />
          {provider === "bea" ? <BeaFields id={id} /> : <CensusFields id={id} />}
          {rows.length > 1 ? (
            <Button type="button" variant="outline" size="sm" onClick={() => setRows(rows.filter((row) => row !== id))}>
              Remove this selection
            </Button>
          ) : null}
        </fieldset>
      ))}
      <Button type="button" variant="outline" disabled={rows.length >= 64} onClick={() => {
        const next = sequence.current++
        setRows((rows) => [...rows, next])
      }}>Add dataset</Button>
    </div>
  )
}

function BeaFields({ id }: { id: number }) {
  const [parameters, setParameters] = React.useState(1)
  return (
    <>
      <TextField name={`${id}-dataset`} label="BEA dataset name" maxLength={128} />
      <p className="text-xs text-muted-foreground">Enter the parameter names and explicit values published for this dataset. Broad ALL, X and * selections are not admitted.</p>
      <input type="hidden" name={`${id}-parameter-count`} value={parameters} />
      {Array.from({ length: parameters }, (_, index) => (
        <div className="grid gap-3 sm:grid-cols-2" key={index}>
          <TextField name={`${id}-parameter-${index}`} label={`Parameter ${index + 1}`} maxLength={128} />
          <TextField name={`${id}-value-${index}`} label="Value or comma-separated values" maxLength={4096} />
        </div>
      ))}
      <div className="flex gap-2">
        <Button type="button" size="sm" variant="outline" disabled={parameters >= 26} onClick={() => setParameters((count) => count + 1)}>Add parameter</Button>
        {parameters > 1 ? <Button type="button" size="sm" variant="outline" onClick={() => setParameters((count) => count - 1)}>Remove last parameter</Button> : null}
      </div>
    </>
  )
}

function CensusFields({ id }: { id: number }) {
  const [kind, setKind] = React.useState("acs_population_households")
  const [survey, setSurvey] = React.useState("five_year")
  const [geography, setGeography] = React.useState("nation")
  const geographic = kind === "acs_population_households" || kind === "county_business_patterns"
  return (
    <>
      <Choice name={`${id}-kind`} label="Measurement family" value={kind} onChange={(value) => {
        setKind(value)
        if (value === "county_business_patterns" && geography === "tract") setGeography("county")
      }} options={[
        ["acs_population_households", "Population and households (ACS)"],
        ["county_business_patterns", "Business employment and establishments"],
        ["quarterly_workforce", "Quarterly workforce employment"],
        ["international_trade", "Monthly international trade value"],
      ]} />
      {kind === "acs_population_households" ? <Choice name={`${id}-survey`} label="Survey period" value={survey} onChange={(value) => {
        setSurvey(value)
        if (value === "one_year" && geography === "tract") setGeography("county")
      }} options={[["five_year", "Five-year survey"], ["one_year", "One-year survey"]]} /> : null}
      <TextField name={`${id}-year`} label={geographic ? "Published vintage year" : "Year"} type="number" min={kind === "county_business_patterns" ? 2017 : kind === "acs_population_households" ? survey === "one_year" ? 2005 : 2009 : 1900} max={new Date().getUTCFullYear()} />
      {geographic ? <Choice name={`${id}-geography`} label="Geographic area" value={geography} onChange={setGeography} options={[
        ["nation", "United States"], ["state", "One state"], ["county", "One county"],
        ...(kind === "acs_population_households" && survey === "five_year" ? [["tract", "One census tract"]] : []),
      ]} /> : null}
      {(geographic && geography !== "nation") || kind === "quarterly_workforce" ? <TextField name={`${id}-state`} label="State FIPS code" pattern="[0-9]{2}" maxLength={2} /> : null}
      {geographic && (geography === "county" || geography === "tract") ? <TextField name={`${id}-county`} label="County FIPS code" pattern="[0-9]{3}" maxLength={3} /> : null}
      {geographic && geography === "tract" ? <TextField name={`${id}-tract`} label="Census tract code" pattern="[0-9]{6}" maxLength={6} /> : null}
      {kind === "county_business_patterns" ? <TextField name={`${id}-naics`} label="NAICS 2017 industry code" pattern="[0-9]{2,6}" maxLength={6} /> : null}
      {kind === "quarterly_workforce" ? <TextField name={`${id}-quarter`} label="Quarter" type="number" min={1} max={4} /> : null}
      {kind === "international_trade" ? <>
        <Choice name={`${id}-direction`} label="Trade direction" options={[["imports", "Imports"], ["exports", "Exports"]]} />
        <TextField name={`${id}-month`} label="Month" type="number" min={1} max={12} />
        <TextField name={`${id}-country`} label="Trading partner code" maxLength={32} />
        <TextField name={`${id}-commodity`} label="Commodity code" maxLength={32} />
      </> : null}
    </>
  )
}

export function macroSelection(provider: Provider, data: FormData): { datasets: Record<string, unknown>[] } {
  const value = (id: string, key: string) => String(data.get(`${id}-${key}`) ?? "")
  return { datasets: data.getAll("macro-row").map((row) => {
    const id = String(row)
    if (provider === "bea") {
      const parameters: Record<string, string> = Object.create(null) as Record<string, string>
      for (let index = 0; index < Number(value(id, "parameter-count")); index++) {
        const name = value(id, `parameter-${index}`)
        if (Object.hasOwn(parameters, name)) throw new Error("Each BEA parameter name must be unique within its dataset.")
        parameters[name] = value(id, `value-${index}`)
      }
      return { dataset: value(id, "dataset"), parameters }
    }
    const kind = value(id, "kind")
    const year = Number(value(id, "year"))
    if (kind === "quarterly_workforce") return { kind, state_fips: value(id, "state"), year, quarter: Number(value(id, "quarter")) }
    if (kind === "international_trade") return { kind, direction: value(id, "direction"), year, month: Number(value(id, "month")), country: value(id, "country"), commodity: value(id, "commodity") }
    const geographicKind = value(id, "geography")
    const geography = {
      kind: geographicKind,
      ...(geographicKind !== "nation" ? { state_fips: value(id, "state") } : {}),
      ...(geographicKind === "county" || geographicKind === "tract" ? { county_fips: value(id, "county") } : {}),
      ...(geographicKind === "tract" ? { tract_code: value(id, "tract") } : {}),
    }
    return kind === "county_business_patterns"
      ? { kind, vintage: year, geography, naics: value(id, "naics") }
      : { kind, vintage: year, geography, survey: value(id, "survey") }
  }) }
}

function TextField({ name, label, ...props }: React.ComponentProps<typeof Input> & { name: string; label: string }) {
  const id = `macro-${name}`
  return <div><Label htmlFor={id}>{label}</Label><Input className="mt-2" id={id} name={name} required autoComplete="off" maxLength={128} {...props} /></div>
}

function Choice({ name, label, options, value, onChange }: { name: string; label: string; options: string[][]; value?: string; onChange?: (value: string) => void }) {
  const id = `macro-${name}`
  return <div><Label htmlFor={id}>{label}</Label><select className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm" id={id} name={name} value={value} onChange={onChange ? (event) => onChange(event.target.value) : undefined}>{options.map(([value, title]) => <option key={value} value={value}>{title}</option>)}</select></div>
}
