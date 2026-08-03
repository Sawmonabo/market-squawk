import * as React from "react"
import { invoke, isTauri } from "@tauri-apps/api/core"
import {
  AlertCircle,
  CheckCircle2,
  FileUp,
  ShieldCheck,
  Trash2,
} from "lucide-react"

import { messageFrom } from "@/app/product-context"
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
import type { DesktopBootstrap } from "@/lib/schemas"

import {
  parsePortfolioImportCommit,
  parsePortfolioImportPreview,
  type PortfolioImportCommit,
  type PortfolioImportPreview,
} from "./portfolio-contracts"
import { shortIdentity } from "./portfolio-format"

const accountUuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const nilUuid = "00000000-0000-0000-0000-000000000000"

type PortfolioImportInterpretation = {
  recordId: string
  interpretation: string
  rationale: string
  selectedLotIndexes: number[]
}

type PortfolioImportActivity = "preview" | "commit" | "discard" | null

export function PortfolioImportWorkflow({
  bootstrap,
  selectedAccountId,
  onCommitted,
}: {
  bootstrap: DesktopBootstrap
  selectedAccountId: string | null
  onCommitted: () => void | Promise<unknown>
}) {
  const operationNames = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const requiredOperations = [
    "Portfolio.PreviewStagedImport",
    "Portfolio.ApproveStagedImport",
    "Portfolio.CommitStagedImport",
    "Portfolio.DiscardStagedImport",
  ]
  const missingOperations = requiredOperations.filter(
    (operation) => !operationNames.has(operation),
  )
  const [accountId, setAccountId] = React.useState(selectedAccountId ?? "")
  const [preview, setPreview] = React.useState<PortfolioImportPreview | null>(null)
  const [interpretations, setInterpretations] = React.useState<
    PortfolioImportInterpretation[]
  >([])
  const [activity, setActivity] = React.useState<PortfolioImportActivity>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [confirmationOpen, setConfirmationOpen] = React.useState(false)
  const [receipt, setReceipt] = React.useState<PortfolioImportCommit | null>(null)

  React.useEffect(() => {
    if (selectedAccountId) {
      setAccountId((current) => (current.trim() === "" ? selectedAccountId : current))
    }
  }, [selectedAccountId])

  const normalizedAccountId = accountId.trim()
  const accountIdValid = isAccountUuid(normalizedAccountId)

  const beginPreview = async () => {
    const requestedAccount = accountId.trim().toLowerCase()
    if (!isAccountUuid(requestedAccount)) {
      setError(
        "Enter a non-nil account UUID that exactly matches the account UUID in the selected extraction batch.",
      )
      return
    }
    setError(null)
    setReceipt(null)
    setActivity("preview")
    try {
      const value = await invoke<unknown>("preview_portfolio_import", {
        accountId: requestedAccount,
        confirmed: true,
      })
      if (value === null) return
      const next = parsePortfolioImportPreview(value)
      if (next.preview.accountId !== requestedAccount) {
        throw new Error(
          "The installed service returned a preview for a different portfolio account.",
        )
      }
      setPreview(next)
      setInterpretations(
        next.preview.transactions
          .filter((transaction) =>
            ["trade", "income"].includes(transaction.classification),
          )
          .map((transaction) => {
            const [onlyInterpretation] = transaction.allowedInterpretations
            const exactlyOneInterpretation =
              transaction.allowedInterpretations.length === 1 &&
              onlyInterpretation !== undefined
            return {
              recordId: transaction.recordId,
              interpretation: exactlyOneInterpretation ? onlyInterpretation : "",
              rationale: exactlyOneInterpretation
                ? "Confirmed the only service-enumerated interpretation after reviewing this source record."
                : "",
              selectedLotIndexes: [],
            }
          }),
      )
    } catch (cause) {
      setError(messageFrom(cause))
    } finally {
      setActivity(null)
    }
  }

  const commit = async () => {
    if (!preview || !interpretationsReady(preview, interpretations)) return
    setError(null)
    setActivity("commit")
    try {
      const value = await invoke<unknown>("commit_portfolio_import", {
        previewId: preview.previewId,
        previewDigest: preview.digest,
        interpretations,
        confirmed: true,
      })
      const committed = parsePortfolioImportCommit(value)
      if (committed.previewId !== preview.previewId) {
        throw new Error(
          "The installed service committed a receipt for a different portfolio preview.",
        )
      }
      setReceipt(committed)
      setPreview(null)
      setInterpretations([])
      setConfirmationOpen(false)
      await onCommitted()
    } catch (cause) {
      setError(messageFrom(cause))
      setConfirmationOpen(false)
    } finally {
      setActivity(null)
    }
  }

  const discard = async () => {
    if (!preview) return
    setError(null)
    setActivity("discard")
    try {
      await invoke<unknown>("discard_portfolio_import", {
        previewId: preview.previewId,
        confirmed: true,
      })
      setPreview(null)
      setInterpretations([])
      setConfirmationOpen(false)
    } catch (cause) {
      setError(messageFrom(cause))
    } finally {
      setActivity(null)
    }
  }

  const updateInterpretation = (
    recordId: string,
    update: (current: PortfolioImportInterpretation) => PortfolioImportInterpretation,
  ) => {
    setInterpretations((current) =>
      current.map((entry) => (entry.recordId === recordId ? update(entry) : entry)),
    )
  }

  const ready = preview ? interpretationsReady(preview, interpretations) : false
  const corporateActionBlocked =
    preview?.preview.resolutionRequirements.requiresServerHeldCorporateActionPlan ?? false

  return (
    <section className="mt-5 rounded-xl border border-border bg-card/45 p-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-primary" aria-hidden="true" />
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
              Protected local import
            </p>
          </div>
          <h2 className="mt-2 text-lg font-semibold">Import portfolio evidence</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Market Squawk opens the file natively, stages a bounded no-follow copy, and sends only
            its opaque ticket to the installed service. Review the service-owned preview and exact
            mappings before anything is approved or committed.
          </p>
        </div>
        {!preview ? (
          <Button
            onClick={() => void beginPreview()}
            disabled={
              activity !== null ||
              missingOperations.length > 0 ||
              !isTauri() ||
              !accountIdValid
            }
          >
            <FileUp aria-hidden="true" />
            {activity === "preview" ? "Staging and previewing…" : "Select portfolio file"}
          </Button>
        ) : null}
      </div>

      {missingOperations.length > 0 ? (
        <Alert className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Protected import is unavailable</AlertTitle>
          <AlertDescription>
            The installed service is missing {missingOperations.join(", ")}. No local file can be
            selected through an incomplete authority chain.
          </AlertDescription>
        </Alert>
      ) : !isTauri() ? (
        <Alert className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Native application required</AlertTitle>
          <AlertDescription>
            Portfolio files can be selected only in the installed desktop application.
          </AlertDescription>
        </Alert>
      ) : (
        <label className="mt-4 grid max-w-xl gap-1.5 text-xs">
          <span className="font-semibold">Destination account UUID</span>
          <input
            value={accountId}
            onChange={(event) => setAccountId(event.target.value)}
            disabled={preview !== null || activity !== null}
            autoComplete="off"
            spellCheck={false}
            aria-invalid={accountId !== "" && !accountIdValid}
            aria-describedby="portfolio-import-account-help"
            placeholder="00000000-0000-0000-0000-000000000001"
            className="h-9 rounded-md border border-input bg-background px-3 font-mono outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-60"
          />
          <span
            id="portfolio-import-account-help"
            className={accountId !== "" && !accountIdValid ? "text-destructive" : "text-muted-foreground"}
          >
            Enter a non-nil UUID. It must exactly match the account UUID retained in the selected
            portfolio extraction batch.
          </span>
        </label>
      )}

      {error ? (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Portfolio import did not complete</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {receipt ? (
        <Alert className="mt-4">
          <CheckCircle2 aria-hidden="true" />
          <AlertTitle>Portfolio revision committed</AlertTitle>
          <AlertDescription>
            The installed authority committed preview {shortIdentity(receipt.previewId, "Preview")} under approval {shortIdentity(receipt.approvalId, "Approval")}. Account evidence is refreshing.
          </AlertDescription>
        </Alert>
      ) : null}

      {preview ? (
        <div className="mt-5 space-y-4 border-t border-border pt-5">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <ImportFact label="Account" value={preview.preview.accountId} />
            <ImportFact
              label="Source records"
              value={preview.preview.rawRecords.length.toLocaleString()}
            />
            <ImportFact
              label="Transactions"
              value={preview.preview.transactions.length.toLocaleString()}
            />
            <ImportFact
              label="Reconciliation breaks"
              value={preview.preview.reconciliationDiscrepancies.length.toLocaleString()}
            />
          </div>
          <div className="rounded-lg border border-border bg-background/25 p-3">
            <p className="text-[11px] font-semibold">Exact preview digest</p>
            <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">
              {preview.digest}
            </p>
          </div>

          {preview.preview.reconciliationDiscrepancies.length > 0 ? (
            <Alert>
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Reconciliation breaks remain visible</AlertTitle>
              <AlertDescription>
                The preview contains {preview.preview.reconciliationDiscrepancies.length.toLocaleString()} supplied-total discrepancies. A committed revision will not be presented as reconciled while these breaks remain.
              </AlertDescription>
            </Alert>
          ) : null}

          {corporateActionBlocked ? (
            <Alert variant="destructive">
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Corporate-action resolution is required</AlertTitle>
              <AlertDescription>
                This preview requires a server-held corporate-action plan. The installed service
                has not made that plan available, so this preview cannot be approved or committed.
              </AlertDescription>
            </Alert>
          ) : null}

          <div>
            <h3 className="text-sm font-semibold">Normalized source preview and mappings</h3>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Trade and income records require an explicit service-enumerated interpretation.
              Specific-lot choices are indexes into the server-held eligible-lot list; no lot ID is
              accepted from the browser.
            </p>
          </div>
          <div className="max-h-[32rem] space-y-3 overflow-y-auto pr-1">
            {preview.preview.transactions.map((transaction) => {
              const interpretation = interpretations.find(
                (entry) => entry.recordId === transaction.recordId,
              )
              return (
                <div
                  key={transaction.recordId}
                  className="rounded-lg border border-border bg-background/25 p-4"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold">
                        {transaction.classification.replaceAll("_", " ")} · {transaction.amount.value} {transaction.amount.currency}
                      </p>
                      <p className="mt-1 break-all font-mono text-[10px] text-muted-foreground">
                        {transaction.recordId}
                      </p>
                    </div>
                    <span className="rounded-md border border-border px-2 py-1 font-mono text-[10px] text-muted-foreground">
                      {transaction.sourceRevision}
                    </span>
                  </div>
                  {interpretation ? (
                    <div className="mt-4 grid gap-3">
                      <label className="grid gap-1.5 text-xs">
                        <span className="font-semibold">Interpretation</span>
                        <select
                          value={interpretation.interpretation}
                          onChange={(event) =>
                            updateInterpretation(transaction.recordId, (current) => ({
                              ...current,
                              interpretation: event.target.value,
                              rationale: "",
                              selectedLotIndexes: [],
                            }))
                          }
                          className="h-9 rounded-md border border-input bg-background px-3 outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        >
                          <option value="" disabled>Select an interpretation</option>
                          {transaction.allowedInterpretations.map((option) => (
                            <option key={option} value={option}>
                              {option.replaceAll("_", " ")}
                            </option>
                          ))}
                        </select>
                      </label>
                      {requiresSelectedLots(interpretation.interpretation) ? (
                        <fieldset className="rounded-md border border-border p-3">
                          <legend className="px-1 text-xs font-semibold">
                            Eligible opening lots
                          </legend>
                          {transaction.eligibleOpeningLotIds.length ? (
                            <div className="grid gap-2">
                              {transaction.eligibleOpeningLotIds.map((lotId, index) => (
                                <label key={lotId} className="flex items-center gap-2 text-xs">
                                  <input
                                    type="checkbox"
                                    checked={interpretation.selectedLotIndexes.includes(index)}
                                    onChange={(event) =>
                                      updateInterpretation(transaction.recordId, (current) => ({
                                        ...current,
                                        selectedLotIndexes: event.target.checked
                                          ? [...current.selectedLotIndexes, index].sort(
                                              (left, right) => left - right,
                                            )
                                          : current.selectedLotIndexes.filter(
                                              (selected) => selected !== index,
                                            ),
                                      }))
                                    }
                                  />
                                  <span className="break-all font-mono">{lotId}</span>
                                </label>
                              ))}
                            </div>
                          ) : (
                            <p className="text-xs text-destructive">
                              The service did not enumerate an eligible opening lot for this
                              interpretation.
                            </p>
                          )}
                        </fieldset>
                      ) : null}
                      <label className="grid gap-1.5 text-xs">
                        <span className="font-semibold">Approval rationale</span>
                        <textarea
                          value={interpretation.rationale}
                          onChange={(event) =>
                            updateInterpretation(transaction.recordId, (current) => ({
                              ...current,
                              rationale: event.target.value,
                            }))
                          }
                          maxLength={4096}
                          rows={2}
                          placeholder="Why does this interpretation match the retained source record?"
                          className="resize-y rounded-md border border-input bg-background px-3 py-2 outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        />
                      </label>
                    </div>
                  ) : (
                    <p className="mt-3 text-xs text-muted-foreground">
                      This record does not require a user-authored interpretation.
                    </p>
                  )}
                </div>
              )
            })}
          </div>

          <div className="flex flex-wrap justify-end gap-2">
            <Button
              variant="outline"
              onClick={() => void discard()}
              disabled={activity !== null}
            >
              <Trash2 aria-hidden="true" />
              {activity === "discard" ? "Discarding…" : "Discard preview"}
            </Button>
            <Button
              onClick={() => setConfirmationOpen(true)}
              disabled={!ready || corporateActionBlocked || activity !== null}
            >
              <ShieldCheck aria-hidden="true" />
              Review and commit
            </Button>
          </div>
        </div>
      ) : null}

      <Dialog
        open={confirmationOpen}
        onOpenChange={(open) => {
          if (activity !== "commit") setConfirmationOpen(open)
        }}
      >
        <DialogContent showCloseButton={activity !== "commit"}>
          <DialogHeader>
            <DialogTitle>Approve and commit this exact portfolio preview?</DialogTitle>
            <DialogDescription>
              The installed service will first bind these interpretations to preview {preview ? shortIdentity(preview.previewId, "Preview") : ""}, then commit only the resulting durable approval. It cannot substitute another file, account, mapping, or lot.
            </DialogDescription>
          </DialogHeader>
          {preview ? (
            <div className="rounded-lg border border-border bg-card/40 p-3 text-xs leading-5">
              <p><span className="font-semibold">Account:</span> {preview.preview.accountId}</p>
              <p><span className="font-semibold">Transactions:</span> {preview.preview.transactions.length.toLocaleString()}</p>
              <p><span className="font-semibold">Governed mappings:</span> {interpretations.length.toLocaleString()}</p>
              <p><span className="font-semibold">Reconciliation breaks:</span> {preview.preview.reconciliationDiscrepancies.length.toLocaleString()}</p>
            </div>
          ) : null}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={activity === "commit"}
              onClick={() => setConfirmationOpen(false)}
            >
              Keep reviewing
            </Button>
            <Button
              disabled={!ready || corporateActionBlocked || activity === "commit"}
              onClick={() => void commit()}
            >
              {activity === "commit" ? "Approving and committing…" : "Approve exact preview and commit"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function interpretationsReady(
  preview: PortfolioImportPreview,
  interpretations: PortfolioImportInterpretation[],
) {
  if (preview.preview.resolutionRequirements.requiresServerHeldCorporateActionPlan) {
    return false
  }
  const resolvable = preview.preview.transactions.filter((transaction) =>
    ["trade", "income"].includes(transaction.classification),
  )
  if (resolvable.length !== interpretations.length) return false
  return resolvable.every((transaction) => {
    const selected = interpretations.find(
      (interpretation) => interpretation.recordId === transaction.recordId,
    )
    return Boolean(
      selected &&
        transaction.allowedInterpretations.includes(selected.interpretation) &&
        selected.rationale.trim() !== "" &&
        selected.rationale.length <= 4096 &&
        (!requiresSelectedLots(selected.interpretation) ||
          (selected.selectedLotIndexes.length > 0 &&
            selected.selectedLotIndexes.every(
              (index) =>
                Number.isInteger(index) &&
                index >= 0 &&
                index < transaction.eligibleOpeningLotIds.length,
            )))
    )
  })
}

function requiresSelectedLots(interpretation: string) {
  return [
    "sell_specific_identification",
    "buy_to_cover_specific_identification",
  ].includes(interpretation)
}

function isAccountUuid(value: string) {
  return accountUuidPattern.test(value) && value.toLowerCase() !== nilUuid
}

function ImportFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-background/25 p-3">
      <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 break-all text-sm font-semibold">{value}</p>
    </div>
  )
}
