import { useId, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { Search } from "lucide-react"
import { useNavigate } from "react-router-dom"

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
  return <div>
    <div className="mb-4 max-w-xl">
      <label className="grid gap-2 text-xs font-medium">
        Compare performance with
        <select className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={selectedBenchmark?.instrumentId ?? ""} disabled={launch.isPending}
          aria-describedby={descriptionId}
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
      <p id={descriptionId} className="mt-2 text-xs leading-5 text-muted-foreground" aria-live="polite">
        {retainedSelection
          ? "Your selected comparison is currently unavailable. Analysis will keep this selection and report any missing comparison evidence."
          : options.isPending ? "Checking available comparisons…"
            : options.isError ? "Comparison choices could not be refreshed. Analysis can still start with the selected or default comparison."
              : comparison?.comparisonDescription
                ?? "Comparison choices are unavailable. Analysis can still start; missing comparison evidence will be reported."}
      </p>
      <Button type="button" variant="link" size="sm" disabled={options.isFetching || launch.isPending}
        onClick={() => void options.refetch()}>{options.isError ? "Retry comparisons" : "Refresh comparisons"}</Button>
    </div>
    <Button type="button" disabled={!available || launch.isPending}
      onClick={() => launch.mutate()}>
      <Search aria-hidden="true" />
      {launch.isPending ? "Starting analysis…" : selectionToken ? "Analyze this investment" : "Find opportunities"}
    </Button>
    <p className="mt-2 max-w-xl text-xs leading-5 text-muted-foreground" role={launch.isError ? "alert" : undefined}>
      {launch.isError
        ? "Analysis could not start. Check current analysis activity and try again."
        : profile.data?.nextAction ?? "Checking analysis availability…"}
    </p>
  </div>
}
