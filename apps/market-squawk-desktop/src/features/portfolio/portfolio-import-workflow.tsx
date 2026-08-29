import * as React from "react"
import { invoke, isTauri } from "@tauri-apps/api/core"
import {
  AlertCircle,
  CheckCircle2,
  FileUp,
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
import type { DesktopBootstrap } from "@/lib/schemas"

import {
  parsePortfolioImportCommit,
  parsePortfolioImportPreview,
  type PortfolioImportCommit,
  type PortfolioImportPreview,
} from "./portfolio-contracts"
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
        "Enter a valid account ID that matches the account selected for this import.",
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
          "The preview does not match the selected portfolio account.",
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
                ? "Confirmed the only available interpretation after reviewing this transaction."
                : "",
              selectedLotIndexes: [],
            }
          }),
      )
    } catch {
      setError("The file could not be prepared for review. Check the account ID and try again.")
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
          "The import confirmation did not match the portfolio file being reviewed.",
        )
      }
      setReceipt(committed)
      setPreview(null)
      setInterpretations([])
      setConfirmationOpen(false)
      await onCommitted()
    } catch {
      setError("Your portfolio details could not be saved right now. Try again.")
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
    } catch {
      setError("The import preview could not be discarded right now. Try again.")
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
          <h2 className="mt-2 text-lg font-semibold">Import portfolio details</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Choose a portfolio file, review the imported details, and confirm any transaction
            interpretations before saving it to this account.
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
            Import is unavailable right now. No changes have been made to this account.
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
          <span className="font-semibold">Account ID</span>
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
            Choose an account above whenever possible. If needed, enter the account ID for this
            import.
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
          <AlertTitle>Portfolio details saved</AlertTitle>
          <AlertDescription>
            Your portfolio details were saved. The account is refreshing now.
          </AlertDescription>
        </Alert>
      ) : null}

      {preview ? (
        <div className="mt-5 space-y-4 border-t border-border pt-5">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <ImportFact label="Account" value={preview.preview.accountId} />
            <ImportFact
              label="Imported records"
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
          {preview.preview.reconciliationDiscrepancies.length > 0 ? (
            <Alert>
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Reconciliation breaks remain visible</AlertTitle>
              <AlertDescription>
                The preview contains {preview.preview.reconciliationDiscrepancies.length.toLocaleString()} differences in reported totals. It will not be shown as reconciled until they are resolved.
              </AlertDescription>
            </Alert>
          ) : null}

          {corporateActionBlocked ? (
            <Alert variant="destructive">
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Corporate-action resolution is required</AlertTitle>
              <AlertDescription>
                This import needs a corporate-action plan before it can be saved.
              </AlertDescription>
            </Alert>
          ) : null}

          <div>
            <h3 className="text-sm font-semibold">Review transactions</h3>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Some trade and income records need your interpretation before they can be saved.
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
                    </div>
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
                                  <span>Lot {index + 1}</span>
                                </label>
                              ))}
                            </div>
                          ) : (
                            <p className="text-xs text-destructive">
                              No eligible opening lot is available for this interpretation.
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
                          placeholder="Why does this interpretation match this transaction?"
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
            <DialogTitle>Save these portfolio details?</DialogTitle>
            <DialogDescription>
              Confirm that the selected account, file, and transaction interpretations are correct.
            </DialogDescription>
          </DialogHeader>
          {preview ? (
            <div className="rounded-lg border border-border bg-card/40 p-3 text-xs leading-5">
              <p><span className="font-semibold">Transactions:</span> {preview.preview.transactions.length.toLocaleString()}</p>
              <p><span className="font-semibold">Interpretations:</span> {interpretations.length.toLocaleString()}</p>
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
              {activity === "commit" ? "Saving…" : "Save portfolio details"}
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
