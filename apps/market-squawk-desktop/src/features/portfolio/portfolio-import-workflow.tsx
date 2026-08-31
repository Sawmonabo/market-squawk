import { AlertCircle, FileUp } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

export function PortfolioImportWorkflow({
  selectedPortfolioName,
}: {
  selectedPortfolioName: string | null
}) {
  return (
    <section className="mt-5 rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start gap-3">
        <FileUp className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Review before saving
          </p>
          <h2 className="mt-2 text-lg font-semibold">Import portfolio details</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            A portfolio file must be reviewed with named investments, exact amounts, dates, and
            explicit transaction choices before it can update a portfolio.
          </p>
        </div>
      </div>
      <Alert className="mt-4">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>
          {selectedPortfolioName ? "Import is unavailable" : "Choose a portfolio first"}
        </AlertTitle>
        <AlertDescription>
          {selectedPortfolioName
            ? `No complete import choices are available for ${selectedPortfolioName}. Nothing has been changed.`
            : "Select the portfolio you want to update. Market Squawk will not choose one for you."}
        </AlertDescription>
      </Alert>
    </section>
  )
}
