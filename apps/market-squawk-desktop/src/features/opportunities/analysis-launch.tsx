import { useId, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { Search } from "lucide-react"
import { Link, useNavigate } from "react-router-dom"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import type { ProfileOptions } from "@/features/advanced/analytical-profile-contracts"
import { useAnalyticalProductProjection } from "@/features/advanced/use-analytical-profile"
import type { ProductTransport } from "@/lib/transport"

/** Starts the native workflow; the WebView never chooses data, models, quantities or targets. */
export function AnalysisLaunch({ transport, scope, selectionToken }: {
  transport: ProductTransport
  scope: ProductScope
  selectionToken?: string
}) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const profile = useAnalyticalProductProjection(transport, scope)
  const descriptionId = useId()
  const identityId = useId()
  const [selectedBenchmark, setSelectedBenchmark] = useState<ProfileOptions["benchmarkChoices"][number] | null>(null)
  const options = useQuery({
    queryKey: [...productKeys.operation(scope, "analysis", "Desktop.AnalyticalProfiles", {}), "options"],
    queryFn: async () => {
      const response = await transport.analyticalController({ action: "profileOptions" })
      if (response.kind !== "profile_options") throw new Error("Comparison choices could not be opened.")
      return response.options
    },
  })
  const choices = options.data?.benchmarkChoices ?? []
  const defaultBenchmark = choices.find((choice) => choice.isDefault)
  const currentSelection = choices.find((choice) => choice.instrumentId === selectedBenchmark?.instrumentId)
  const retainedSelection = selectedBenchmark !== null && currentSelection === undefined
  const comparison = selectedBenchmark ? currentSelection ?? selectedBenchmark : defaultBenchmark
  const comparisonIdentity = comparison ? `${comparison.displayName} (${comparison.symbol})` : null
  const benchmarkInput = selectedBenchmark ? { benchmarkInstrumentId: selectedBenchmark.instrumentId } : {}
  const launch = useMutation({
    mutationFn: () => transport.analyticalController(selectionToken
      ? { action: "analyzeInvestment", selectionToken, ...benchmarkInput }
      : { action: "findOpportunities", ...benchmarkInput }, true),
    onSuccess: async (response) => {
      if (response.kind !== "workflow") throw new Error("Analysis could not be started.")
      await queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "analysis") })
      navigate(`/opportunities?workflow=${encodeURIComponent(response.workflow.workflowToken)}`)
    },
    retry: false,
  })
  const available = profile.data?.workflowAvailability === "available"
  const setupMessage = launch.isError ? setupRequiredMessage(launch.error) : null
  return <div className="min-w-0">
    <div className="mb-4 min-w-0 max-w-xl">
      <label className="grid min-w-0 gap-2 text-xs font-medium">
        Compare performance with
        <select className="h-9 w-full min-w-0 max-w-full truncate rounded-md border border-input bg-background px-3 text-sm focus-visible:outline-2 focus-visible:outline-ring"
          value={selectedBenchmark?.instrumentId ?? ""} disabled={launch.isPending}
          aria-describedby={`${identityId} ${descriptionId}`}
          title={comparisonIdentity ?? "Default comparison"}
          onChange={(event) => {
            if (event.target.value === "") setSelectedBenchmark(null)
            else {
              const choice = choices.find((item) => item.instrumentId === event.target.value)
              if (choice) setSelectedBenchmark(choice)
            }
          }}>
          <option value="">{defaultBenchmark
            ? `Default — ${defaultBenchmark.displayName} (${defaultBenchmark.symbol})`
            : "Default comparison"}</option>
          {retainedSelection ? <option value={selectedBenchmark.instrumentId}>
            {selectedBenchmark.displayName} ({selectedBenchmark.symbol}) — currently unavailable
          </option> : null}
          {choices.map((choice) => <option key={choice.instrumentId} value={choice.instrumentId}>
            {choice.displayName} ({choice.symbol})
          </option>)}
        </select>
      </label>
      <p id={identityId} className="mt-2 text-xs leading-5 [overflow-wrap:anywhere]">
        {comparisonIdentity ? <>Selected comparison: {comparisonIdentity}</> : "Default comparison"}
      </p>
      <p id={descriptionId} className="mt-2 text-xs leading-5 text-muted-foreground [overflow-wrap:anywhere]" aria-live="polite">
        {retainedSelection
          ? "Your selected comparison is currently unavailable. Analysis will keep this selection and report any missing comparison evidence."
          : options.isPending ? "Checking available comparisons…"
            : options.isError ? "Comparison choices could not be refreshed. Analysis can still start with the selected or default comparison."
              : comparison?.comparisonDescription
                ?? "Comparison choices are unavailable. Analysis can still start; missing comparison evidence will be reported."}
      </p>
      {options.isError ? <Button type="button" variant="link" size="sm" disabled={options.isFetching || launch.isPending}
        onClick={() => void options.refetch()}>Retry comparisons</Button> : null}
    </div>
    <Button type="button" disabled={!available || launch.isPending}
      onClick={() => launch.mutate()}>
      <Search aria-hidden="true" />
      {launch.isPending ? "Starting analysis…" : selectionToken ? "Analyze this investment" : "Find opportunities"}
    </Button>
    <p className="mt-2 max-w-xl text-xs leading-5 text-muted-foreground" role={launch.isError ? "alert" : undefined}>
      {launch.isError
        ? setupMessage ?? "Analysis could not start. Check current analysis activity and try again."
        : profile.data?.nextAction ?? "Checking analysis availability…"}
    </p>
    {setupMessage ? <Link className="mt-2 inline-block text-sm text-primary underline underline-offset-4" to="/portfolio">
      Open Portfolio
    </Link> : null}
  </div>
}

function setupRequiredMessage(error: unknown): string | null {
  if (typeof error !== "object" || error === null
    || !("code" in error) || error.code !== "analysis_setup_required"
    || !("message" in error) || typeof error.message !== "string"
    || error.message.trim().length === 0) return null
  return error.message
}
