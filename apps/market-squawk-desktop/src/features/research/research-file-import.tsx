import * as React from "react"
import { invoke, isTauri } from "@tauri-apps/api/core"
import {
  AlertCircle,
  CheckCircle2,
  FileSpreadsheet,
  FileUp,
  LoaderCircle,
  ShieldCheck,
  Trash2,
} from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import type { DesktopBootstrap } from "@/lib/schemas"

import {
  parseResearchFileCommit,
  parseResearchFileDiscard,
  parseResearchFilePreview,
  type ResearchFilePreview,
} from "./research-contracts"

type ResearchFileFormat = ResearchFilePreview["format"]
type ImportActivity = "preview" | "commit" | "discard" | null

type ValueMapping = {
  source: string
  field: string
  decimalScale: string
  unit: string
}

type RowMappings = {
  effectiveField: string
  publishedField: string
  availableField: string
  revisionField: string
  revisionNumberField: string
  supersededField: string
}

type GuidedMapping = {
  dataset: string
  identityField: string
  fields: Array<{
    source: string
    field: string
    decimalScale: number
    unit?: string
  }>
  effectiveAt: string
  publishedAt?: string
  effectiveField?: string
  publishedField?: string
  availableField?: string
  revisionField?: string
  revisionNumberField?: string
  supersededField?: string
  instrumentId?: string
  universe?: string
}

const formats: Array<{
  value: ResearchFileFormat
  label: string
  detail: string
}> = [
  { value: "csv", label: "CSV", detail: "A table with a header row" },
  { value: "json", label: "JSON", detail: "One row or an array of rows" },
  { value: "ndjson", label: "NDJSON", detail: "One JSON row per line" },
  { value: "parquet", label: "Parquet", detail: "A columnar data file" },
]

const emptyRowMappings: RowMappings = {
  effectiveField: "",
  publishedField: "",
  availableField: "",
  revisionField: "",
  revisionNumberField: "",
  supersededField: "",
}

const identifierPattern = /^[A-Za-z0-9_.:/-]+$/
const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const nilUuid = "00000000-0000-0000-0000-000000000000"
const visibleColumnLimit = 24

export function ResearchFileImport({
  bootstrap,
  onStarted,
}: {
  bootstrap: DesktopBootstrap
  onStarted: () => void | Promise<unknown>
}) {
  const operations = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const requiredOperations = [
    "Research.PreviewStagedFile",
    "Research.CommitStagedFile",
    "Research.DiscardStagedFile",
  ]
  const missingOperations = requiredOperations.filter(
    (operation) => !operations.has(operation),
  )
  const [format, setFormat] = React.useState<ResearchFileFormat | "">("")
  const [preview, setPreview] = React.useState<ResearchFilePreview | null>(null)
  const [dataset, setDataset] = React.useState("")
  const [identityField, setIdentityField] = React.useState("")
  const [valueMappings, setValueMappings] = React.useState<ValueMapping[]>([])
  const [effectiveAt, setEffectiveAt] = React.useState("")
  const [publishedAt, setPublishedAt] = React.useState("")
  const [rowMappings, setRowMappings] =
    React.useState<RowMappings>(emptyRowMappings)
  const [instrumentId, setInstrumentId] = React.useState("")
  const [universe, setUniverse] = React.useState("")
  const [activity, setActivity] = React.useState<ImportActivity>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [importStarted, setImportStarted] = React.useState(false)
  const [confirmationOpen, setConfirmationOpen] = React.useState(false)

  const clearMapping = React.useCallback(() => {
    setDataset("")
    setIdentityField("")
    setValueMappings([])
    setEffectiveAt("")
    setPublishedAt("")
    setRowMappings(emptyRowMappings)
    setInstrumentId("")
    setUniverse("")
  }, [])

  const beginPreview = async () => {
    if (!format) return
    setError(null)
    setImportStarted(false)
    setActivity("preview")
    try {
      const value = await invoke<unknown>("preview_research_file_import", {
        format,
        confirmed: true,
      })
      if (value === null) return
      const next = parseResearchFilePreview(value)
      if (next.format !== format) {
        throw new Error(
          "The installed service previewed the file with a different format than the one selected.",
        )
      }
      setPreview(next)
      clearMapping()
    } catch {
      setError("The file could not be checked. Try again or choose another file.")
    } finally {
      setActivity(null)
    }
  }

  const discard = async () => {
    if (!preview) return
    setError(null)
    setActivity("discard")
    try {
      const value = await invoke<unknown>("discard_research_file_import", {
        previewId: preview.previewId,
        confirmed: true,
      })
      parseResearchFileDiscard(value, preview.previewId)
      setPreview(null)
      clearMapping()
      setConfirmationOpen(false)
    } catch {
      setError("The file preview could not be closed. Try again.")
    } finally {
      setActivity(null)
    }
  }

  const mappingResult = preview
    ? buildMapping({
        preview,
        dataset,
        identityField,
        valueMappings,
        effectiveAt,
        publishedAt,
        rowMappings,
        instrumentId,
        universe,
      })
    : { mapping: null, error: "Choose a file first." }

  const commit = async () => {
    if (!preview || !mappingResult.mapping) return
    setError(null)
    setActivity("commit")
    try {
      const value = await invoke<unknown>("commit_research_file_import", {
        previewId: preview.previewId,
        mapping: mappingResult.mapping,
        confirmed: true,
      })
      parseResearchFileCommit(value)
      setImportStarted(true)
      setPreview(null)
      clearMapping()
      setConfirmationOpen(false)
      await onStarted()
    } catch {
      setError("The file could not be imported. Review your choices and try again.")
      setConfirmationOpen(false)
    } finally {
      setActivity(null)
    }
  }

  const toggleValueField = (source: string, selected: boolean) => {
    setValueMappings((current) => {
      if (!selected) return current.filter((mapping) => mapping.source !== source)
      if (current.some((mapping) => mapping.source === source) || current.length >= 64) {
        return current
      }
      return [...current, { source, field: "", decimalScale: "", unit: "" }]
    })
  }

  const updateValueField = (
    source: string,
    update: (current: ValueMapping) => ValueMapping,
  ) => {
    setValueMappings((current) =>
      current.map((mapping) =>
        mapping.source === source ? update(mapping) : mapping,
      ),
    )
  }

  const numericColumns =
    preview?.columns.filter(
      (column) => column.kind === "exact_decimal" && !column.nullable,
    ) ?? []
  const identityColumns =
    preview?.columns.filter(
      (column) =>
        !column.nullable &&
        !["unsupported", "null"].includes(column.kind),
    ) ?? []
  const rowFieldColumns =
    preview?.columns.filter(
      (column) =>
        !column.nullable &&
        !["unsupported", "null"].includes(column.kind),
    ) ?? []
  const advancedRowFields: Array<{
    key: keyof RowMappings
    label: string
    columns: ResearchFilePreview["columns"]
  }> = [
    {
      key: "publishedField",
      label: "First-public time column",
      columns: rowFieldColumns,
    },
    {
      key: "availableField",
      label: "Available-to-you time column",
      columns: rowFieldColumns,
    },
    {
      key: "revisionField",
      label: "Revision label column",
      columns: rowFieldColumns,
    },
    {
      key: "revisionNumberField",
      label: "Revision number column",
      columns: numericColumns,
    },
    {
      key: "supersededField",
      label: "Superseded time column",
      columns: rowFieldColumns,
    },
  ]

  return (
    <section className="mt-5 overflow-hidden rounded-xl border border-primary/25 bg-card/45">
      <div className="border-b border-border bg-gradient-to-br from-primary/10 via-card/30 to-transparent p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <FileSpreadsheet className="size-4 text-primary" aria-hidden="true" />
              <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
                Your own research data
              </p>
            </div>
            <h2 className="mt-2 text-lg font-semibold">Import a research file</h2>
            <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
              Choose a file, check a small preview, and tell Market Squawk what each useful
              column means. The selected file remains on this device, and only the columns you
              choose become research information.
            </p>
          </div>
          <ol className="flex shrink-0 items-center gap-2 text-[10px] text-muted-foreground">
            <Step active={!preview} number="1" label="Choose" />
            <span aria-hidden="true">→</span>
            <Step active={preview !== null && !confirmationOpen} number="2" label="Describe" />
            <span aria-hidden="true">→</span>
            <Step active={confirmationOpen} number="3" label="Confirm" />
          </ol>
        </div>
      </div>

      <div className="p-5">
        {missingOperations.length > 0 ? (
          <Alert>
            <AlertCircle aria-hidden="true" />
            <AlertTitle>File import is unavailable</AlertTitle>
            <AlertDescription>
              This installation cannot safely import files yet. No file has been selected.
            </AlertDescription>
          </Alert>
        ) : !isTauri() ? (
          <Alert>
            <AlertCircle aria-hidden="true" />
            <AlertTitle>Open the installed desktop application</AlertTitle>
            <AlertDescription>
              Choose files from the installed Market Squawk desktop application. File selection
              is not available in a standalone browser tab.
            </AlertDescription>
          </Alert>
        ) : null}

        {!preview ? (
          <div className="mt-1">
            <fieldset disabled={activity !== null || missingOperations.length > 0 || !isTauri()}>
              <legend className="text-xs font-semibold">What kind of file are you adding?</legend>
              <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                {formats.map((option) => (
                  <label
                    key={option.value}
                    className={`cursor-pointer rounded-lg border p-3 transition-colors focus-within:ring-2 focus-within:ring-ring ${
                      format === option.value
                        ? "border-primary/55 bg-primary/10"
                        : "border-border bg-background/25 hover:border-primary/35"
                    }`}
                  >
                    <span className="flex items-center gap-2 text-sm font-semibold">
                      <input
                        type="radio"
                        name="research-file-format"
                        value={option.value}
                        checked={format === option.value}
                        onChange={() => setFormat(option.value)}
                        className="accent-primary"
                      />
                      {option.label}
                    </span>
                    <span className="mt-1 block pl-5 text-[11px] leading-5 text-muted-foreground">
                      {option.detail}
                    </span>
                  </label>
                ))}
              </div>
            </fieldset>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <Button
                onClick={() => void beginPreview()}
                disabled={
                  !format ||
                  activity !== null ||
                  missingOperations.length > 0 ||
                  !isTauri()
                }
              >
                {activity === "preview" ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <FileUp aria-hidden="true" />
                )}
                {activity === "preview" ? "Checking your file…" : "Choose file and preview"}
              </Button>
              <p className="text-[11px] leading-5 text-muted-foreground">
                Nothing is imported until you review and confirm the mapping.
              </p>
            </div>
          </div>
        ) : (
          <div className="space-y-5">
            <PreviewTable preview={preview} />

            {identityColumns.length === 0 ? (
              <CorrectionAlert>
                Every row needs one value that identifies it. Add a unique column without blank
                values, then choose the corrected file again.
              </CorrectionAlert>
            ) : null}
            {numericColumns.length === 0 ? (
              <CorrectionAlert>
                No consistently numeric value column was found. Make at least one research value
                numeric in every populated row, then choose the corrected file again.
              </CorrectionAlert>
            ) : null}

            <div className="grid gap-5 border-t border-border pt-5 xl:grid-cols-2">
              <div className="space-y-4">
                <div>
                  <h3 className="text-sm font-semibold">Name and identify the rows</h3>
                  <p className="mt-1 text-xs leading-5 text-muted-foreground">
                    These names make the imported history searchable and identify every row.
                  </p>
                </div>
                <Field label="Collection name" help="Use letters, numbers, dots, dashes, underscores, colons, or slashes.">
                  <Input
                    value={dataset}
                    onChange={(event) => setDataset(event.target.value)}
                    maxLength={256}
                    placeholder="example_price_history"
                    autoComplete="off"
                    spellCheck={false}
                  />
                </Field>
                <Field
                  label="Column that identifies each row"
                  help="Choose one always-present column that is different for every row, such as a record number or an existing combined date-and-symbol value."
                >
                  <ColumnSelect
                    value={identityField}
                    columns={identityColumns.map((column) => column.name)}
                    emptyLabel="Choose the unique column"
                    onChange={setIdentityField}
                  />
                </Field>
              </div>

              <div>
                <h3 className="text-sm font-semibold">Choose the values to analyze</h3>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  Select one or more always-present numeric columns. Market Squawk verifies the
                  number format across the full file before importing it.
                </p>
                <div className="mt-3 max-h-72 space-y-2 overflow-y-auto pr-1">
                  {numericColumns.map((column) => {
                    const mapping = valueMappings.find(
                      (candidate) => candidate.source === column.name,
                    )
                    return (
                      <div key={column.name} className="rounded-lg border border-border bg-background/25 p-3">
                        <label className="flex items-start gap-2 text-xs font-semibold">
                          <input
                            type="checkbox"
                            checked={mapping !== undefined}
                            onChange={(event) =>
                              toggleValueField(column.name, event.target.checked)
                            }
                            className="mt-0.5 accent-primary"
                          />
                          <span className="min-w-0 break-all">{column.name}</span>
                        </label>
                        {mapping ? (
                          <div className="mt-3 grid gap-3 sm:grid-cols-[minmax(0,1fr)_7rem_8rem]">
                            <Field label="Analysis name" help="A short searchable field name.">
                              <Input
                                value={mapping.field}
                                onChange={(event) =>
                                  updateValueField(column.name, (current) => ({
                                    ...current,
                                    field: event.target.value,
                                  }))
                                }
                                maxLength={256}
                                placeholder="close_price"
                                autoComplete="off"
                                spellCheck={false}
                              />
                            </Field>
                            <Field label="Decimal places" help="0 through 28. For example, 123.45 uses 2.">
                              <Input
                                type="number"
                                min={0}
                                max={28}
                                step={1}
                                inputMode="numeric"
                                value={mapping.decimalScale}
                                onChange={(event) =>
                                  updateValueField(column.name, (current) => ({
                                    ...current,
                                    decimalScale: event.target.value,
                                  }))
                                }
                                placeholder="2"
                              />
                            </Field>
                            <Field label="Unit / currency" help="Optional, such as USD or percent.">
                              <Input
                                value={mapping.unit}
                                onChange={(event) =>
                                  updateValueField(column.name, (current) => ({
                                    ...current,
                                    unit: event.target.value,
                                  }))
                                }
                                maxLength={64}
                                placeholder="USD"
                                autoComplete="off"
                                spellCheck={false}
                              />
                            </Field>
                          </div>
                        ) : null}
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>

            <div className="grid gap-5 border-t border-border pt-5 xl:grid-cols-2">
              <div className="space-y-4">
                <div>
                  <h3 className="text-sm font-semibold">Set the data time</h3>
                  <p className="mt-1 text-xs leading-5 text-muted-foreground">
                    The fallback time is required and is interpreted as UTC. A row column can
                    replace it when every row carries its own RFC 3339 timestamp.
                  </p>
                </div>
                <Field label="Fallback data time (UTC)" help="Used when no per-row data-time column is selected.">
                  <Input
                    type="datetime-local"
                    value={effectiveAt}
                    onChange={(event) => setEffectiveAt(event.target.value)}
                  />
                </Field>
                <Field label="Per-row data time (optional)" help="Each entry must include a date, time, and time zone, such as 2026-08-29T14:30:00Z.">
                  <ColumnSelect
                    value={rowMappings.effectiveField}
                    columns={rowFieldColumns.map((column) => column.name)}
                    emptyLabel="Use the fallback time"
                    onChange={(value) =>
                      setRowMappings((current) => ({ ...current, effectiveField: value }))
                    }
                  />
                </Field>
                <Field label="Fallback first-public time (optional, UTC)" help="When the information first became public.">
                  <Input
                    type="datetime-local"
                    value={publishedAt}
                    onChange={(event) => setPublishedAt(event.target.value)}
                  />
                </Field>
              </div>

              <details className="rounded-lg border border-border bg-background/20 p-4">
                <summary className="cursor-pointer text-sm font-semibold">
                  Advanced time, revision, and investment links
                </summary>
                <p className="mt-2 text-xs leading-5 text-muted-foreground">
                  Use these only when the file explicitly contains the corresponding information.
                  Leave them blank when it does not.
                </p>
                <div className="mt-4 grid gap-4 sm:grid-cols-2">
                  {advancedRowFields.map((field) => (
                    <OptionalRowField
                      key={field.key}
                      label={field.label}
                      value={rowMappings[field.key]}
                      columns={field.columns}
                      onChange={(value) =>
                        setRowMappings((current) => ({
                          ...current,
                          [field.key]: value,
                        }))
                      }
                    />
                  ))}
                  <Field label="Investment link (optional)" help="Use the reference copied from an investment page.">
                    <Input value={instrumentId} onChange={(event) => setInstrumentId(event.target.value)} maxLength={36} autoComplete="off" spellCheck={false} />
                  </Field>
                  <Field label="Investment group on that date (optional)" help="Available after linking an investment.">
                    <Input value={universe} onChange={(event) => setUniverse(event.target.value)} maxLength={256} placeholder="my_watchlist" autoComplete="off" spellCheck={false} />
                  </Field>
                </div>
              </details>
            </div>

            {mappingResult.error ? (
              <p className="text-xs leading-5 text-destructive" role="status">
                {mappingResult.error}
              </p>
            ) : (
              <Alert>
                <ShieldCheck aria-hidden="true" />
                <AlertTitle>Ready for final review</AlertTitle>
                <AlertDescription>
                  The file preview, {valueMappings.length.toLocaleString()} selected value
                  {valueMappings.length === 1 ? "" : "s"}, dates, and optional investment link are
                  ready to import.
                </AlertDescription>
              </Alert>
            )}

            <div className="flex flex-wrap justify-end gap-2 border-t border-border pt-4">
              <Button variant="outline" onClick={() => void discard()} disabled={activity !== null}>
                <Trash2 aria-hidden="true" />
                {activity === "discard" ? "Closing…" : "Choose a different file"}
              </Button>
              <Button
                onClick={() => setConfirmationOpen(true)}
                disabled={!mappingResult.mapping || activity !== null}
              >
                <ShieldCheck aria-hidden="true" />
                Review import
              </Button>
            </div>
          </div>
        )}

        {error ? (
          <Alert variant="destructive" className="mt-4">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>Research file import did not complete</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        {importStarted ? (
          <Alert className="mt-4">
            <CheckCircle2 aria-hidden="true" />
            <AlertTitle>Your research import is running</AlertTitle>
            <AlertDescription>
              You may leave this page. Follow progress and recovery actions in Background activity
              or Operations &amp; Jobs.
            </AlertDescription>
          </Alert>
        ) : null}
      </div>

      <Dialog
        open={confirmationOpen}
        onOpenChange={(open) => {
          if (activity !== "commit") setConfirmationOpen(open)
        }}
      >
        <DialogContent showCloseButton={activity !== "commit"}>
          <DialogHeader>
            <DialogTitle>Import this research file?</DialogTitle>
            <DialogDescription>
              Market Squawk will use the choices shown below and add the file to your research
              library. Review them before continuing.
            </DialogDescription>
          </DialogHeader>
          {preview && mappingResult.mapping ? (
            <div className="space-y-2 rounded-lg border border-border bg-card/40 p-3 text-xs leading-5">
              <Summary label="Collection" value={mappingResult.mapping.dataset} />
              <Summary label="Rows" value={preview.rowCount.toLocaleString()} />
              <Summary label="Row identifier column" value={mappingResult.mapping.identityField} />
              <Summary
                label="Values"
                value={mappingResult.mapping.fields
                  .map((field) => `${field.field} (${field.source})`)
                  .join(", ")}
              />
              <Summary label="Fallback data time" value={mappingResult.mapping.effectiveAt} />
              <Summary
                label="Investment link"
                value={mappingResult.mapping.instrumentId ? "Linked" : "Not linked"}
              />
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" disabled={activity === "commit"} onClick={() => setConfirmationOpen(false)}>
              Keep reviewing
            </Button>
            <Button disabled={!mappingResult.mapping || activity === "commit"} onClick={() => void commit()}>
              {activity === "commit" ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <ShieldCheck aria-hidden="true" />
              )}
              {activity === "commit" ? "Importing…" : "Confirm and import"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function PreviewTable({ preview }: { preview: ResearchFilePreview }) {
  const visibleColumns = preview.columns.slice(0, visibleColumnLimit)
  return (
    <div>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold">Check the detected data</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {preview.rowCount.toLocaleString()} rows · {preview.columns.length.toLocaleString()} columns · {preview.format.toUpperCase()}. This table shows at most {preview.sampleRows.length.toLocaleString()} sample rows and {visibleColumns.length.toLocaleString()} columns.
          </p>
        </div>
        <span className="rounded-md border border-border px-2 py-1 font-mono text-[10px] text-muted-foreground">
          Ready to review
        </span>
      </div>
      <div className="mt-3 max-h-80 overflow-auto rounded-lg border border-border bg-background/30">
        <table className="min-w-full border-collapse text-left text-[11px]">
          <thead className="sticky top-0 z-10 bg-card">
            <tr>
              <th className="border-b border-r border-border px-3 py-2 font-mono text-muted-foreground">Row</th>
              {visibleColumns.map((column) => (
                <th key={column.name} className="min-w-32 border-b border-r border-border px-3 py-2 align-bottom">
                  <span className="block max-w-48 break-all font-semibold">{column.name}</span>
                  <span className="mt-1 block font-mono text-[9px] font-normal uppercase tracking-wide text-muted-foreground">
                    {columnKind(column.kind)}{column.nullable ? " · blanks" : ""}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {preview.sampleRows.map((row, rowIndex) => (
              <tr key={rowIndex} className="odd:bg-card/20">
                <th className="border-b border-r border-border px-3 py-2 font-mono text-muted-foreground">{rowIndex + 1}</th>
                {visibleColumns.map((column, columnIndex) => (
                  <td key={column.name} className="max-w-64 border-b border-r border-border px-3 py-2 align-top">
                    <PreviewCell cell={row[columnIndex]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {preview.columns.length > visibleColumnLimit ? (
        <p className="mt-2 text-[10px] text-muted-foreground">
          {preview.columns.length - visibleColumnLimit} additional columns are available in the mapping controls without expanding this preview table.
        </p>
      ) : null}
    </div>
  )
}

function PreviewCell({ cell }: { cell: ResearchFilePreview["sampleRows"][number][number] | undefined }) {
  if (!cell || cell.kind === "missing") return <span className="italic text-muted-foreground">Missing</span>
  if (cell.kind === "null") return <span className="italic text-muted-foreground">Null</span>
  if (cell.kind === "unsupported") return <span className="text-destructive">Unsupported value</span>
  return <span className="break-all">{cell.value}{cell.truncated ? "…" : ""}</span>
}

function Step({ active, number, label }: { active: boolean; number: string; label: string }) {
  return (
    <li className={`flex items-center gap-1.5 rounded-full border px-2.5 py-1 ${active ? "border-primary/45 bg-primary/10 text-foreground" : "border-border"}`}>
      <span className="font-mono text-primary">{number}</span>
      <span>{label}</span>
    </li>
  )
}

function CorrectionAlert({ children }: { children: React.ReactNode }) {
  return (
    <Alert variant="destructive">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>This file needs one correction</AlertTitle>
      <AlertDescription>{children}</AlertDescription>
    </Alert>
  )
}

function Field({ label, help, children }: { label: string; help: string; children: React.ReactNode }) {
  return (
    <label className="grid gap-1.5 text-xs">
      <span className="font-semibold">{label}</span>
      {children}
      <span className="text-[10px] leading-4 text-muted-foreground">{help}</span>
    </label>
  )
}

function ColumnSelect({ value, columns, emptyLabel, onChange }: { value: string; columns: string[]; emptyLabel: string; onChange: (value: string) => void }) {
  return (
    <select
      value={value}
      onChange={(event) => onChange(event.target.value)}
      className="h-9 w-full rounded-md border border-input bg-background px-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      <option value="">{emptyLabel}</option>
      {columns.map((column) => <option key={column} value={column}>{column}</option>)}
    </select>
  )
}

function OptionalRowField({ label, value, columns, onChange }: { label: string; value: string; columns: ResearchFilePreview["columns"]; onChange: (value: string) => void }) {
  return (
    <Field label={label} help="Leave blank when this evidence is not in the file.">
      <ColumnSelect value={value} columns={columns.map((column) => column.name)} emptyLabel="Not included" onChange={onChange} />
    </Field>
  )
}

function Summary({ label, value }: { label: string; value: string }) {
  return <p><span className="font-semibold">{label}:</span> <span className="break-all">{value}</span></p>
}

function columnKind(kind: ResearchFilePreview["columns"][number]["kind"]) {
  if (kind === "exact_decimal") return "Number"
  if (kind === "text") return "Text"
  if (kind === "mixed") return "Mixed values"
  if (kind === "null") return "Empty"
  return "Unsupported"
}

function buildMapping(input: {
  preview: ResearchFilePreview
  dataset: string
  identityField: string
  valueMappings: ValueMapping[]
  effectiveAt: string
  publishedAt: string
  rowMappings: RowMappings
  instrumentId: string
  universe: string
}): { mapping: GuidedMapping | null; error: string | null } {
  const dataset = input.dataset.trim()
  if (!validIdentifier(dataset, 256)) return invalid("Enter a valid searchable collection name.")
  const columns = new Map(input.preview.columns.map((column) => [column.name, column]))
  const identity = columns.get(input.identityField)
  if (!identity || identity.nullable || ["unsupported", "null"].includes(identity.kind)) {
    return invalid("Choose an always-present column that identifies each row.")
  }
  if (input.valueMappings.length === 0) return invalid("Choose at least one numeric value column.")
  if (input.valueMappings.length > 64) return invalid("Choose no more than 64 numeric value columns.")
  const outputNames = new Set<string>()
  const fields: GuidedMapping["fields"] = []
  for (const field of input.valueMappings) {
    const source = columns.get(field.source)
    if (source?.kind !== "exact_decimal" || source.nullable) {
      return invalid("A selected value column is not always present and numeric.")
    }
    const output = field.field.trim()
    if (!validIdentifier(output, 256) || outputNames.has(output)) return invalid("Give every selected value a different valid analysis name.")
    outputNames.add(output)
    if (field.decimalScale.trim() === "") return invalid(`Choose decimal places for ${field.source}.`)
    const decimalScale = Number(field.decimalScale)
    if (!Number.isInteger(decimalScale) || decimalScale < 0 || decimalScale > 28) return invalid(`Choose decimal places from 0 through 28 for ${field.source}.`)
    const unit = field.unit.trim()
    if (unit && !validIdentifier(unit, 64)) return invalid(`Use a short valid unit or currency for ${field.source}.`)
    fields.push({ source: field.source, field: output, decimalScale, ...(unit ? { unit } : {}) })
  }
  const effective = utcTimestamp(input.effectiveAt)
  if (!effective) return invalid("Choose the fallback data time in UTC.")
  const published = input.publishedAt ? utcTimestamp(input.publishedAt) : null
  if (input.publishedAt && !published) return invalid("Choose a valid fallback first-public time in UTC.")
  for (const field of Object.values(input.rowMappings)) {
    if (field && !columns.has(field)) return invalid("A selected date or revision column is no longer in this file preview.")
  }
  const instrument = input.instrumentId.trim().toLowerCase()
  if (instrument && (!uuidPattern.test(instrument) || instrument === nilUuid)) return invalid("Enter a valid investment reference or leave it blank.")
  const universe = input.universe.trim()
  if (universe && !instrument) return invalid("Link an investment before naming its dated group.")
  if (universe && !validIdentifier(universe, 256)) return invalid("Enter a valid dated investment-group name.")
  const optionalRows = Object.fromEntries(
    Object.entries(input.rowMappings).filter(([, value]) => value !== ""),
  ) as Partial<RowMappings>
  return {
    mapping: {
      dataset,
      identityField: input.identityField,
      fields,
      effectiveAt: effective,
      ...(published ? { publishedAt: published } : {}),
      ...optionalRows,
      ...(instrument ? { instrumentId: instrument } : {}),
      ...(universe ? { universe } : {}),
    },
    error: null,
  }
}

function validIdentifier(value: string, maximum: number) {
  return value.length > 0 && value.length <= maximum && identifierPattern.test(value)
}

function utcTimestamp(value: string) {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?$/.test(value)) return null
  const expected = value.length === 16 ? `${value}:00` : value
  const timestamp = new Date(`${value}Z`)
  if (Number.isNaN(timestamp.getTime())) return null
  const exact = timestamp.toISOString()
  return exact.slice(0, 19) === expected ? exact : null
}

function invalid(error: string) {
  return { mapping: null, error }
}
