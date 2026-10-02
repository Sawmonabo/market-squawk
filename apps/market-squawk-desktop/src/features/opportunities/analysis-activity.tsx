import { RefreshButton } from "@/components/ui/refresh-button"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { CircleAlert, Play, Square } from "lucide-react"
import { useEffect, useRef } from "react"
import type { AnalyticalControllerResponse, MissingInvestmentEvidence, WorkflowCoverageCursor } from "@/features/advanced/analytical-profile-contracts"
import { Link, useSearchParams } from "react-router-dom"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { useAnalyticalControllerStatus } from "@/features/advanced/use-analytical-profile"
import type { ProductTransport } from "@/lib/transport"

import { formatUnixNanos } from "./format"

export function AnalysisActivity({ transport, scope }: {
  transport: Pick<ProductTransport, "analyticalController">
  scope: ProductScope
}) {
  const queryClient = useQueryClient()
  const [searchParams] = useSearchParams()
  const requestedWorkflow = searchParams.get("workflow")
  const activity = useAnalyticalControllerStatus(transport, scope)
  const published = activity.data?.workflows.flatMap((workflow) => workflow.resultActionTokens).join(":") ?? ""
  const previouslyPublished = useRef("")
  useEffect(() => {
    if (!published || published === previouslyPublished.current) return
    previouslyPublished.current = published
    void queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "decision") })
  }, [published, queryClient, scope])
  const resume = useMutation({
    mutationFn: (workflowToken: string) => transport.analyticalController({ action: "resumeWorkflow", workflowToken }, true),
    onSuccess: async () => { await queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "analysis") }) },
  })
  const cancel = useMutation({
    mutationFn: (workflowToken: string) => transport.analyticalController({
      action: "cancelWorkflow", workflowToken,
    }, true),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "analysis") })
    },
  })
  if (!activity.data) return null
  const workflows = activity.data.workflows
  const selectedWorkflow = workflows.find((workflow) => workflow.workflowToken === requestedWorkflow)
  const visibleWorkflows = selectedWorkflow
    ? [selectedWorkflow, ...workflows.filter((workflow) => workflow.workflowToken !== requestedWorkflow)
      .slice(-11).reverse()]
    : workflows.slice(-12).reverse()
  if (!visibleWorkflows.length && requestedWorkflow === null) return null

  return <section className="mt-6" aria-labelledby="analysis-activity-heading">
    <div className="flex items-center justify-between gap-3">
      <h2 id="analysis-activity-heading" className="text-lg font-semibold">Analysis activity</h2>
      <RefreshButton label="Refresh activity" refreshing={activity.isFetching} disabled={activity.isFetching}
        onClick={() => void activity.refetch()} />
    </div>
    {cancel.isError || resume.isError || activity.isError ? <Alert className="mt-4" variant="destructive">
      <CircleAlert aria-hidden="true" /><AlertTitle>Analysis progress could not be updated</AlertTitle>
      <AlertDescription>Refresh the activity before trying again.</AlertDescription>
    </Alert> : null}
    {requestedWorkflow !== null && selectedWorkflow === undefined ? <Alert className="mt-4">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>This analysis is not in current activity</AlertTitle>
      <AlertDescription>Refresh activity, or open a completed brief from saved history below.</AlertDescription>
    </Alert> : null}
    <div className="mt-4 space-y-3">
      {visibleWorkflows.map((workflow) => (
        <article key={workflow.workflowToken} aria-current={workflow.workflowToken === requestedWorkflow ? "true" : undefined}
          className={`rounded-xl border bg-card/40 p-4 ${workflow.workflowToken === requestedWorkflow ? "border-primary/50" : "border-border"}`}>
          {workflow.workflowToken === requestedWorkflow ? <p className="mb-2 font-mono text-[10px] uppercase tracking-[0.16em] text-primary">Selected analysis</p> : null}
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold">{workflow.kind === "opportunity_discovery"
                ? "Opportunity search" : workflow.kind === "investment_analysis"
                  ? "Investment analysis" : "Track record update"}</h3>
              <p className="mt-1 text-xs text-muted-foreground">{stateLabel(workflow.state)} · {formatUnixNanos(workflow.startedAt)}</p>
              {workflow.explanation ? <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">{workflow.explanation}</p> : null}
            </div>
            {workflow.canResume ? <Button size="sm" disabled={resume.isPending}
              onClick={() => resume.mutate(workflow.workflowToken)}><Play aria-hidden="true" /> Resume analysis</Button> : null}
            {workflow.canResume ? <Button asChild size="sm" variant="outline"><Link to="/system/settings/onboarding">Review setup</Link></Button> : null}
            {workflow.canCancel ? <Button size="sm" variant="outline" disabled={cancel.isPending}
              onClick={() => cancel.mutate(workflow.workflowToken)}><Square aria-hidden="true" />
              {workflow.state === "cancelling" ? "Continue stopping" : "Stop analysis"}
            </Button> : null}
          </div>
          {workflow.resultOrdering ? <p className="mt-3 text-xs text-muted-foreground">{workflow.resultOrdering === "estimated_gain_descending"
            ? "Higher estimated gains first. Open each brief for its evidence and risks."
            : "These estimates cover different periods. Briefs retain their original search order."}</p> : null}
          {workflow.resultActionTokens.length ? <div className="mt-3 flex flex-wrap gap-3">
            {workflow.resultActionTokens.map((actionToken, index) => <Link key={actionToken} className="text-sm text-primary underline underline-offset-4" to={`/opportunities?analysis=${encodeURIComponent(actionToken)}`}>Open investment brief {index + 1}</Link>)}
          </div> : null}
          {workflow.unavailableMembers.length ? <div className="mt-4 space-y-2" aria-label="Unavailable investment evidence">
            {workflow.unavailableMembers.map((member, index) => <div key={member.candidateId} className="rounded-lg border border-border/70 p-3 text-xs">
              <p className="font-medium">Investment {index + 1}: analysis unavailable</p>
              <p className="mt-1 text-muted-foreground">{member.reason === "identity_unavailable"
                ? "The investment could not be identified reliably for this search."
                : "Required information was unavailable when this investment was checked."}</p>
              {member.missingEvidence.length ? <ul className="mt-2 list-disc space-y-1 pl-4 text-muted-foreground">
                {member.missingEvidence.map((category, position) => <li key={`${category}-${position}`}>{missingEvidenceLabel(category)}</li>)}
              </ul> : null}
            </div>)}
          </div> : null}
          {workflow.coverage ? <>
            <p className="mt-4 text-xs font-medium">{workflow.coverage.completeness === "complete"
              ? "Search coverage recorded" : "Partial search coverage"}</p>
            <dl className="mt-3 grid grid-cols-2 gap-3 border-t border-border/70 pt-3 sm:grid-cols-4">
              <Count label="Investments searched" value={workflow.coverage.searched} />
              <Count label="Analyzed" value={workflow.coverage.deeplyAnalyzed} />
              <Count label="Actions" value={workflow.coverage.generated} />
              <Count label="No action" value={workflow.coverage.noAction} />
              <Count label="In search scope" value={workflow.coverage.population} />
              <Count label="Inputs unavailable" value={workflow.coverage.inputUnavailable} />
              <Count label="Outside search scope" value={workflow.coverage.excluded} />
              <Count label="Analysis unavailable" value={workflow.coverage.unavailable} />
            </dl>
            {workflow.kind === "opportunity_discovery" && workflow.state === "complete"
              ? <CoverageDetails transport={transport} workflowToken={workflow.workflowToken} /> : null}
          </> : <p className="mt-3 text-xs text-muted-foreground">{stageLabel(workflow.progress.stage)}</p>}
        </article>
      ))}
    </div>
    {workflows.length > 12 ? <p className="mt-3 text-xs text-muted-foreground">
      {selectedWorkflow ? "Showing the selected analysis and up to 11 recent analyses." : "Showing the 12 most recent analyses."} Saved investment briefs remain in your history.
    </p> : null}
  </section>
}

function Count({ label, value }: { label: string; value: number }) {
  return <div><dt className="text-[10px] text-muted-foreground">{label}</dt>
    <dd className="mt-1 text-sm font-medium tabular-nums">{value.toLocaleString("en-US")}</dd></div>
}

function stateLabel(state: string) {
  const labels: Record<string, string> = {
    waiting: "Waiting", in_progress: "In progress", paused: "Ready to resume",
    cancelling: "Stopping", complete: "Complete", cancelled: "Stopped", unavailable: "Unavailable",
  }
  return labels[state] ?? "Unavailable"
}

function stageLabel(stage: string) {
  const labels: Record<string, string> = {
    preparing: "Preparing the investment search", gathering_evidence: "Checking investment information",
    building_results: "Building investment briefs", finalizing: "Completing coverage and results",
    complete: "Analysis complete", unavailable: "More information is needed to complete this analysis",
  }
  return labels[stage] ?? "Checking progress"
}

function CoverageDetails({ transport, workflowToken }: {
  transport: Pick<ProductTransport, "analyticalController">
  workflowToken: string
}) {
  const page = useMutation({
    mutationFn: async (after?: WorkflowCoverageCursor) => {
      const result = await transport.analyticalController({ action: "workflowCoverage", workflowToken, ...(after ? { after } : {}) }, false)
      if (result.kind !== "workflow_coverage" || result.workflowToken !== workflowToken) throw new Error("Saved coverage did not match this search.")
      return result
    },
  })
  return <details className="mt-4 text-xs">
    <summary className="cursor-pointer font-medium">Why investments were excluded or unavailable</summary>
    <p className="mt-2 text-muted-foreground">These are the original reasons saved with this search. Each investment brief explains its own decision and evidence.</p>
    {page.isError ? <p className="mt-2 text-destructive">Saved coverage could not be opened. Try again.</p> : null}
    {page.data ? <>
      <ul className="mt-3 space-y-3">{page.data.rows.map((row) => <li key={row.instrumentId}>
        <span>{coverageReasonLabel(row.reason)}</span>
      </li>)}</ul>
      {page.data.rows.length === 0 ? <p className="mt-3 text-muted-foreground">No reasons in this part of the saved coverage.</p> : null}
      {page.data.nextAfter ? <Button className="mt-3" size="sm" variant="outline" disabled={page.isPending}
        onClick={() => { const next = page.data?.nextAfter; if (next) page.mutate(next) }}>Next reasons</Button>
        : <p className="mt-3 text-muted-foreground">End of saved coverage.</p>}
    </> : null}
    <Button className="mt-3" size="sm" variant="outline" disabled={page.isPending}
      onClick={() => page.mutate(undefined)}>{page.isPending ? "Opening coverage…" : page.data ? "Start again" : "Open saved reasons"}</Button>
  </details>
}

function coverageReasonLabel(reason: Extract<AnalyticalControllerResponse, { kind: "workflow_coverage" }>["rows"][number]["reason"]) {
  const labels = {
    no_effective_canonical_definition: "Investment identity was unavailable at the search cutoff.",
    outside_profile_asset_scope: "Outside your selected investment scope.",
    missing_official_listing: "Official listing information was unavailable.",
    ambiguous_official_listing: "Official listing information was ambiguous.",
    source_history_unavailable: "Required price history was unavailable.",
    required_feature_unavailable: "Required analytical inputs were unavailable.",
    source_rights_unavailable: "The selected source did not admit this use.",
    freshness_unavailable: "The available information was not current enough.",
    calendar_unavailable: "Completed-session evidence was unavailable.",
  }
  return labels[reason]
}

function missingEvidenceLabel(category: MissingInvestmentEvidence): string {
  const labels: Record<MissingInvestmentEvidence, string> = {
    current_market: "Current market information was unavailable.",
    interest_rate_history: "Required interest-rate history was unavailable.",
    benchmark_history: "Comparison investment history was unavailable.",
    investment_history: "The investment’s price history was unavailable.",
    corporate_actions: "Information about stock splits and other corporate actions was unavailable.",
    equity_premium: "The required equity risk premium estimate was unavailable.",
  }
  return labels[category]
}
