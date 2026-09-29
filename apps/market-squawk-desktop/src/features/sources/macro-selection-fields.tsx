import * as React from "react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export function BeaSelectionFields() {
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
          <BeaFields id={id} />
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

export function beaSelection(data: FormData): { datasets: Record<string, unknown>[] } {
  const value = (id: string, key: string) => String(data.get(`${id}-${key}`) ?? "")
  return { datasets: data.getAll("macro-row").map((row) => {
    const id = String(row)
    const parameters: Record<string, string> = Object.create(null) as Record<string, string>
    for (let index = 0; index < Number(value(id, "parameter-count")); index++) {
      const name = value(id, `parameter-${index}`)
      if (Object.hasOwn(parameters, name)) throw new Error("Each BEA parameter name must be unique within its dataset.")
      parameters[name] = value(id, `value-${index}`)
    }
    return { dataset: value(id, "dataset"), parameters }
  }) }
}

function TextField({ name, label, ...props }: React.ComponentProps<typeof Input> & { name: string; label: string }) {
  const id = `macro-${name}`
  return <div><Label htmlFor={id}>{label}</Label><Input className="mt-2" id={id} name={name} required autoComplete="off" maxLength={128} {...props} /></div>
}
