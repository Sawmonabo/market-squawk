import * as React from "react"
import {
  useInfiniteQuery,
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query"
import {
  BadgeCheck,
  CircleAlert,
  FileSearch,
  Landmark,
  Layers3,
  RefreshCw,
  ScrollText,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
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
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type FairValueApproval,
  type FairValueAuditEvent,
  type FairValueClassification,
  type FairValueGovernanceProposal,
  type FairValueInput,
  type FairValueMeasurement,
  type GovernanceAuthorization,
  parseFairValueApprovals,
  parseFairValueAuditPage,
  parseFairValueClassification,
  parseFairValueClassificationControl,
  parseFairValueEvidence,
  parseFairValueGovernanceCommit,
  parseFairValueGovernancePreview,
  parseFairValueExplanation,
  parseFairValueMarketAccess,
  parseGovernanceAuthorization,
  parseGovernancePrincipals,
  parseFairValueWorkspace,
} from "./fair-value-contracts"
import { FairValueDetail } from "./fair-value-detail"
import { FairValueGovernanceWorkflow } from "./fair-value-governance"

export function FairValuePage() {
  const product = useProduct()

  if (product.status === "loading") return <FairValueLoading />
  if (product.status === "error") {
    return (
      <FairValueFrame>
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Fair-value service unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </FairValueFrame>
    )
  }

  const available = product.bootstrap.operations.some(
    (operation) => operation.name === "FairValue.ListMeasurements",
  )
  if (!available) {
    return (
      <FairValueFrame>
        <EmptyState
          title="Fair-value workspace is not available"
          detail="The installed service did not advertise the bounded measurement index. Restore or update the Fair Value component before relying on classifications."
        />
      </FairValueFrame>
    )
  }

  return (
    <FairValueWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function FairValueWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [selectedId, setSelectedId] = React.useState<string | null>(null)
  const measurements = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "FairValue",
      "FairValue.ListMeasurements",
      {},
    ),
    queryFn: async () =>
      parseFairValueWorkspace(
        await transport.query({ query: "fairValueMeasurements" }),
      ),
  })

  const rows = measurements.data?.measurements ?? []
  const selected =
    rows.find((measurement) => measurement.measurementId === selectedId) ??
    rows[0] ??
    null
  const accountCount = new Set(rows.map((measurement) => measurement.accountId)).size
  const instrumentCount = new Set(
    rows.map((measurement) => measurement.instrumentId),
  ).size
  const latestMeasurement = rows.reduce<string | null>((latest, measurement) => {
    if (!latest || measurement.measurementAt > latest) return measurement.measurementAt
    return latest
  }, null)

  return (
    <FairValueFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => void measurements.refetch()}
          disabled={measurements.isFetching}
        >
          <RefreshCw
            className={measurements.isFetching ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh measurements
        </Button>
      }
    >
      {measurements.isPending ? (
        <FairValueContentLoading />
      ) : measurements.isError ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Measurements could not be loaded</AlertTitle>
          <AlertDescription>{messageFrom(measurements.error)}</AlertDescription>
        </Alert>
      ) : rows.length === 0 ? (
        <EmptyState
          title="No fair-value measurements yet"
          detail="Create a measurement through a typed Fair Value workflow. Measurements appear here only after the service has retained their exact method, amount, inputs, and evidence identity."
        />
      ) : (
        <>
          <section
            aria-label="Fair-value workspace summary"
            className="grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-4"
          >
            <SummaryFact
              icon={Landmark}
              label="Measurements"
              value={String(rows.length)}
              detail="Immutable retained records"
            />
            <SummaryFact
              icon={Layers3}
              label="Accounts"
              value={String(accountCount)}
              detail="Reporting accounts in view"
            />
            <SummaryFact
              icon={BadgeCheck}
              label="Instruments"
              value={String(instrumentCount)}
              detail="Measured instruments in view"
            />
            <SummaryFact
              icon={ScrollText}
              label="Latest measurement"
              value={latestMeasurement ? shortDate(latestMeasurement) : "Not available"}
              detail="Newest timestamp in this result"
            />
          </section>

          {measurements.data.availableItems > measurements.data.returnedItems ? (
            <Alert className="mt-4">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>Bounded measurement result</AlertTitle>
              <AlertDescription>
                Showing {measurements.data.returnedItems.toLocaleString()} of{" "}
                {measurements.data.availableItems.toLocaleString()} retained measurements. Do not
                treat this dashboard window as the complete population.
              </AlertDescription>
            </Alert>
          ) : null}

          <div className="mt-4 grid min-h-[650px] gap-4 xl:grid-cols-[minmax(260px,0.68fr)_minmax(0,1.62fr)]">
            <MeasurementIndex
              measurements={rows}
              selectedId={selected?.measurementId ?? null}
              onSelect={setSelectedId}
            />
            {selected ? (
              <SelectedFairValue
                key={selected.measurementId}
                bootstrap={bootstrap}
                transport={transport}
                measurement={selected}
              />
            ) : null}
          </div>

          <p className="mt-4 text-[10px] leading-5 text-muted-foreground">
            Result boundary: {humanize(measurements.data.completeness)} · returned{" "}
            {measurements.data.returnedItems.toLocaleString()} · available{" "}
            {measurements.data.availableItems.toLocaleString()}. Fair-value classification is
            analytical evidence and never grants execution-quality status.
          </p>
        </>
      )}
    </FairValueFrame>
  )
}

const AUDIT_PAGE_LIMIT = 50
const MAXIMUM_AUDIT_PAGES = 20

function SelectedFairValue({
  bootstrap,
  transport,
  measurement,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  measurement: FairValueMeasurement
}) {
  const queryClient = useQueryClient()
  const [approvalCutoff] = React.useState(() => new Date().toISOString())
  const [confirmClassification, setConfirmClassification] = React.useState(false)
  const [announcement, setAnnouncement] = React.useState("")
  const [governanceAuthorizations, setGovernanceAuthorizations] = React.useState<
    GovernanceAuthorization[]
  >([])
  const operations = React.useMemo(
    () => new Set(bootstrap.operations.map((operation) => operation.name)),
    [bootstrap.operations],
  )
  const requiredDetailOperations = [
    "FairValue.GetClassification",
    "FairValue.Explain",
    "FairValue.GetEvidence",
    "FairValue.GetApprovalStatus",
    "FairValue.ListAuditEvents",
    "FairValue.GetMarketAccess",
  ]
  const unavailableOperations = requiredDetailOperations.filter(
    (operation) => !operations.has(operation),
  )
  const detailKey = (operation: string, input: Record<string, unknown>) =>
    productKeys.operation(bootstrap.runtime, "FairValue", operation, input)

  const classification = useQuery({
    queryKey: detailKey("FairValue.GetClassification", {
      measurementId: measurement.measurementId,
    }),
    queryFn: async () =>
      parseFairValueClassification(
        await transport.query({
          query: "fairValueClassification",
          measurementId: measurement.measurementId,
        }),
        measurement,
      ),
    enabled: operations.has("FairValue.GetClassification"),
  })
  const explanation = useQuery({
    queryKey: detailKey("FairValue.Explain", {
      measurementId: measurement.measurementId,
    }),
    queryFn: async () =>
      parseFairValueExplanation(
        await transport.query({
          query: "fairValueExplanation",
          measurementId: measurement.measurementId,
        }),
        measurement,
      ),
    enabled: operations.has("FairValue.Explain"),
  })
  const evidence = useQuery({
    queryKey: detailKey("FairValue.GetEvidence", {
      measurementId: measurement.measurementId,
    }),
    queryFn: async () =>
      parseFairValueEvidence(
        await transport.query({
          query: "fairValueEvidence",
          measurementId: measurement.measurementId,
        }),
        measurement,
      ),
    enabled: operations.has("FairValue.GetEvidence"),
  })
  const approvals = useQuery({
    queryKey: detailKey("FairValue.GetApprovalStatus", {
      measurementId: measurement.measurementId,
      at: approvalCutoff,
    }),
    queryFn: async () =>
      parseFairValueApprovals(
        await transport.query({
          query: "fairValueApprovalStatus",
          measurementId: measurement.measurementId,
          at: approvalCutoff,
        }),
        measurement.measurementId,
      ),
    enabled: operations.has("FairValue.GetApprovalStatus"),
  })
  const audit = useInfiniteQuery({
    queryKey: detailKey("FairValue.ListAuditEvents", {
      limit: AUDIT_PAGE_LIMIT,
    }),
    initialPageParam: undefined as
      | { sequence: number; eventId: string }
      | undefined,
    queryFn: async ({ pageParam }) =>
      parseFairValueAuditPage(
        await transport.query({
          query: "fairValueAudit",
          limit: AUDIT_PAGE_LIMIT,
          ...(pageParam ? { after: pageParam } : {}),
        }),
      ),
    getNextPageParam: (page, pages) =>
      pages.length < MAXIMUM_AUDIT_PAGES
        ? (page.nextCursor ?? undefined)
        : undefined,
    enabled: operations.has("FairValue.ListAuditEvents"),
  })

  const assessmentIds = React.useMemo(
    () =>
      [...new Set(
        (evidence.data?.inputs ?? []).flatMap((input) =>
          input.marketAccessAssessment
            ? [input.marketAccessAssessment.assessmentId]
            : [],
        ),
      )],
    [evidence.data],
  )
  const marketAccess = useQueries({
    queries: assessmentIds.map((assessmentId) => ({
      queryKey: detailKey("FairValue.GetMarketAccess", { assessmentId }),
      queryFn: async () =>
        parseFairValueMarketAccess(
          await transport.query({
            query: "fairValueMarketAccess",
            assessmentId,
          }),
          assessmentId,
        ),
      enabled: operations.has("FairValue.GetMarketAccess"),
    })),
  })
  const classify = useMutation({
    mutationFn: async () =>
      parseFairValueClassificationControl(
        await transport.fairValueControl(
          { action: "classify", measurementId: measurement.measurementId },
          true,
        ),
        measurement,
      ),
    onSuccess: async (result) => {
      setConfirmClassification(false)
      setAnnouncement(
        result.replay
          ? "The current rules classification was already retained."
          : "The measurement was evaluated with the current rules.",
      )
      await queryClient.invalidateQueries({
        queryKey: productKeys.domain(bootstrap.runtime, "FairValue"),
      })
    },
  })
  const governanceAvailable =
    operations.has("Governance.ListPrincipals") &&
    operations.has("Governance.AuthenticateAction") &&
    operations.has("FairValue.PreviewGovernanceAction") &&
    operations.has("FairValue.CommitGovernanceAction")
  const governancePrincipals = useQuery({
    queryKey: productKeys.operation(bootstrap.runtime, "Governance", "Governance.ListPrincipals", {}),
    queryFn: async () =>
      parseGovernancePrincipals(
        await transport.governanceQuery({ query: "principals" }),
      ),
    enabled: governanceAvailable,
  })
  const governancePreview = useMutation({
    mutationFn: async (proposal: FairValueGovernanceProposal) =>
      parseFairValueGovernancePreview(
        await transport.fairValueControl(
          { action: "previewGovernanceAction", proposal },
          true,
        ),
      ),
    onSuccess: () => {
      setGovernanceAuthorizations([])
      setAnnouncement("The service created a canonical governance preview. Authenticate every required principal before committing it.")
    },
  })
  const governanceAuthenticate = useMutation({
    mutationFn: async ({
      principalId,
      credential,
    }: {
      principalId: string
      credential: string
    }) => {
      const preview = governancePreview.data
      if (!preview) throw new Error("Create a governance preview before authenticating a principal.")
      return parseGovernanceAuthorization(
        await transport.governanceControl(
          {
            action: "authenticateAction",
            previewId: preview.previewId,
            principalId,
            credential,
          },
          true,
        ),
        preview.previewId,
      )
    },
    onSuccess: (authorization) => {
      setGovernanceAuthorizations((current) => [
        ...current.filter(
          (item) =>
            item.principalId !== authorization.principalId,
        ),
        authorization,
      ])
      setAnnouncement("The service accepted this principal's one-use authorization for the exact preview.")
    },
  })
  const governanceCommit = useMutation({
    mutationFn: async () => {
      const preview = governancePreview.data
      if (!preview) throw new Error("Create a governance preview before committing it.")
      return parseFairValueGovernanceCommit(
        await transport.fairValueControl(
          {
            action: "commitGovernanceAction",
            previewId: preview.previewId,
            authorizationHandles: governanceAuthorizations.map(
              (authorization) => authorization.authorizationHandle,
            ),
          },
          true,
        ),
        preview.previewId,
      )
    },
    onSuccess: async (commit) => {
      setGovernanceAuthorizations([])
      governancePreview.reset()
      setAnnouncement(
        `The service committed the governed action and retained receipt ${commit.receiptId}.`,
      )
      await queryClient.invalidateQueries({
        queryKey: productKeys.domain(bootstrap.runtime, "FairValue"),
      })
    },
  })

  const resolvedClassification =
    classification.data ?? explanation.data?.classification
  const auditEvents = React.useMemo(
    () =>
      relatedAuditEvents(
        measurement,
        resolvedClassification,
        evidence.data?.inputs,
        approvals.data?.approvals,
        audit.data?.pages,
      ),
    [measurement, resolvedClassification, evidence.data, approvals.data, audit.data],
  )
  const hydrated: FairValueMeasurement = {
    ...measurement,
    ...(resolvedClassification
      ? { classification: resolvedClassification }
      : {}),
    ...(explanation.data
      ? {
          explanation: {
            truthTable: explanation.data.truthTable,
            reasons: explanation.data.reasons,
          },
        }
      : {}),
    ...(evidence.data ? { evidence: { inputs: evidence.data.inputs } } : {}),
    ...(approvals.data
      ? {
          approvalStatus: {
            at: approvals.data.at,
            approvals: approvals.data.approvals,
          },
        }
      : {}),
    ...(marketAccess.some((query) => query.data)
      ? {
          marketAccess: marketAccess.flatMap((query) =>
            query.data ? [query.data] : [],
          ),
        }
      : {}),
    ...(audit.data ? { auditEvents } : {}),
  }
  const queries = [classification, explanation, evidence, approvals, audit]
  const loading =
    (operations.has("FairValue.GetClassification") && classification.isPending) ||
    (operations.has("FairValue.Explain") && explanation.isPending) ||
    (operations.has("FairValue.GetEvidence") && evidence.isPending) ||
    (operations.has("FairValue.GetApprovalStatus") && approvals.isPending) ||
    (operations.has("FairValue.ListAuditEvents") && audit.isPending) ||
    (operations.has("FairValue.GetMarketAccess") &&
      marketAccess.some((query) => query.isPending))
  const errors = [
    ...queries.flatMap((query) =>
      query.isError ? [messageFrom(query.error)] : [],
    ),
    ...marketAccess.flatMap((query) =>
      query.isError ? [messageFrom(query.error)] : [],
    ),
  ]
  const boundedDetails = [explanation.data, evidence.data, approvals.data].filter(
    (result) =>
      result !== undefined && result.availableItems > result.returnedItems,
  )
  const auditCapped =
    (audit.data?.pages.length ?? 0) >= MAXIMUM_AUDIT_PAGES &&
    audit.data?.pages.at(-1)?.nextCursor !== null

  return (
    <div className="min-w-0 space-y-3">
      <p className="sr-only" aria-live="polite">
        {announcement}
      </p>
      {loading ? (
        <Alert>
          <RefreshCw className="animate-spin" aria-hidden="true" />
          <AlertTitle>Loading retained evidence</AlertTitle>
          <AlertDescription>
            The service is resolving this measurement&apos;s classification, evidence, approvals,
            market access, and bounded audit history.
          </AlertDescription>
        </Alert>
      ) : null}
      {errors.length > 0 ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Some fair-value details could not be loaded</AlertTitle>
          <AlertDescription>{[...new Set(errors)].join(" ")}</AlertDescription>
        </Alert>
      ) : null}
      {unavailableOperations.length > 0 ? (
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Installed Fair Value detail is incomplete</AlertTitle>
          <AlertDescription>
            The service did not advertise {unavailableOperations.map(operationLabel).join(", ")}.
            Restore or update the installed component before relying on this record as a complete
            review.
          </AlertDescription>
        </Alert>
      ) : null}
      {boundedDetails.length > 0 ? (
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Bounded detail result</AlertTitle>
          <AlertDescription>
            One or more evidence, explanation, or approval sections were truncated by the service.
            The displayed classification remains tied to its exact evidence and ruleset identities.
          </AlertDescription>
        </Alert>
      ) : null}
      {operations.has("FairValue.Classify") ? (
        <div className="flex justify-end">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => setConfirmClassification(true)}
            disabled={classify.isPending}
          >
            <BadgeCheck aria-hidden="true" />
            Evaluate with current rules
          </Button>
        </div>
      ) : null}
      {classify.isError ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Classification was not accepted</AlertTitle>
          <AlertDescription>{messageFrom(classify.error)}</AlertDescription>
        </Alert>
      ) : null}
      <FairValueDetail
        measurement={hydrated}
        auditBoundary={{
          loadedEventCount: audit.data?.pages.reduce(
            (count, page) => count + page.events.length,
            0,
          ) ?? 0,
          totalEventCount: audit.data?.pages[0]?.totalEventCount,
          hasMore: audit.hasNextPage,
          loadingMore: audit.isFetchingNextPage,
          capped: auditCapped,
          onLoadMore: () => void audit.fetchNextPage(),
        }}
      />
      {governanceAvailable ? (
        <FairValueGovernanceWorkflow
          measurement={measurement}
          classification={resolvedClassification}
          inputs={evidence.data?.inputs}
          approvals={approvals.data?.approvals}
          principals={governancePrincipals.data}
          state={{
            preview: governancePreview.data ?? null,
            authorizations: governanceAuthorizations,
            busy:
              governancePreview.isPending ||
              governanceAuthenticate.isPending ||
              governanceCommit.isPending,
            error: governanceAuthenticate.isError
              ? "Authentication was not accepted. Reauthenticate an eligible principal for this exact preview."
              : governanceCommit.isError
                ? messageFrom(governanceCommit.error)
                : governancePreview.isError
                  ? messageFrom(governancePreview.error)
                  : governancePrincipals.isError
                    ? messageFrom(governancePrincipals.error)
                    : null,
          }}
          onPreview={(proposal) => governancePreview.mutate(proposal)}
          onAuthenticate={(principalId, credential) =>
            governanceAuthenticate.mutate({ principalId, credential })
          }
          onCommit={() => governanceCommit.mutate()}
        />
      ) : (
        <Alert className="border-dashed">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Guided governance is unavailable</AlertTitle>
          <AlertDescription>
            This installed service does not advertise the principal, preview, authentication, and
            commit operations required to record a governed action. Do not substitute actor names,
            raw requests, or browser confirmation for those service-held controls.
          </AlertDescription>
        </Alert>
      )}
      <Dialog open={confirmClassification} onOpenChange={setConfirmClassification}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Evaluate this measurement?</DialogTitle>
            <DialogDescription>
              Market Squawk will apply the installed ruleset to the retained measurement and its
              exact evidence hash. Existing decisions remain immutable and auditable.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmClassification(false)}>
              Cancel
            </Button>
            <Button onClick={() => classify.mutate()} disabled={classify.isPending}>
              {classify.isPending ? "Evaluating…" : "Evaluate measurement"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function operationLabel(operation: string) {
  return operation.replace("FairValue.", "").replace(/([a-z])([A-Z])/g, "$1 $2")
}

function relatedAuditEvents(
  measurement: FairValueMeasurement,
  classification: FairValueClassification | undefined,
  inputs: FairValueInput[] | undefined,
  approvals: FairValueApproval[] | undefined,
  pages: { events: FairValueAuditEvent[] }[] | undefined,
) {
  const decisionIds = new Set(
    [classification?.decisionId, ...(approvals ?? []).map((item) => item.decisionId)]
      .filter((value): value is string => value !== undefined),
  )
  const approvalIds = new Set((approvals ?? []).map((approval) => approval.approvalId))
  const overrideIds = new Set(
    (approvals ?? []).flatMap((approval) =>
      approval.overrideId ? [approval.overrideId] : [],
    ),
  )
  const assessmentIds = new Set(
    inputs?.flatMap((input) =>
      input.marketAccessAssessment
        ? [input.marketAccessAssessment.assessmentId]
        : [],
    ) ?? [],
  )
  return (pages ?? [])
    .flatMap((page) => page.events)
    .filter((event) => {
      const subject = event.subject
      switch (subject.kind) {
        case "classified":
          return subject.measurementId === measurement.measurementId
        case "override_proposed":
          return decisionIds.has(subject.decisionId) || overrideIds.has(subject.overrideId)
        case "approved":
          return decisionIds.has(subject.decisionId) || approvalIds.has(subject.approvalId)
        case "revoked":
          return approvalIds.has(subject.approvalId)
        case "market_access_approved":
          return assessmentIds.has(subject.assessmentId)
      }
    })
}

function MeasurementIndex({
  measurements,
  selectedId,
  onSelect,
}: {
  measurements: FairValueMeasurement[]
  selectedId: string | null
  onSelect: (id: string) => void
}) {
  return (
    <section className="overflow-hidden rounded-xl border border-border bg-card/35">
      <div className="border-b border-border px-4 py-3">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Measurement register
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Newest retained records from the bounded service view.
        </p>
      </div>
      <ul className="max-h-[720px] space-y-1 overflow-y-auto p-2">
        {measurements.map((measurement) => {
          const active = measurement.measurementId === selectedId
          return (
            <li key={measurement.measurementId}>
              <button
                type="button"
                aria-pressed={active}
                onClick={() => onSelect(measurement.measurementId)}
                className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  active
                    ? "border-primary/45 bg-primary/10"
                    : "border-transparent hover:border-border hover:bg-accent/40"
                }`}
              >
                <span className="flex items-center justify-between gap-2">
                  <span className="truncate text-xs font-semibold">
                    {measurement.instrumentId}
                  </span>
                  <HierarchyPip
                    hierarchy={measurement.classification?.hierarchy ?? null}
                  />
                </span>
                <span className="mt-2 block font-mono text-sm">
                  {formatMoney(measurement.amount)}
                </span>
                <span className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
                  <span>{humanize(measurement.method)}</span>
                  <span>{shortDate(measurement.measurementAt)}</span>
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </section>
  )
}

function SummaryFact({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Landmark
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="border-border p-4 sm:border-r sm:last:border-r-0">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-xl font-semibold">{value}</p>
      <p className="mt-1 text-[10px] text-muted-foreground">{detail}</p>
    </div>
  )
}

function HierarchyPip({ hierarchy }: { hierarchy: string | null }) {
  return (
    <span className="shrink-0 rounded-full border border-border bg-background/55 px-2 py-0.5 font-mono text-[8px] uppercase text-muted-foreground">
      {hierarchy ? humanize(hierarchy) : "Unloaded"}
    </span>
  )
}

function FairValueFrame({
  children,
  action,
}: {
  children: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div className="mx-auto w-full max-w-[1320px] p-5 lg:p-7">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-border pb-6">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            ASC 820 · IFRS 13 evidence workspace
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Fair Value</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Inspect measurement methods, hierarchy decisions, evidence quality, market access, and
            governed review history without confusing analytical classification with live market
            depth or execution eligibility.
          </p>
        </div>
        {action}
      </header>
      <div className="mt-5">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <FileSearch className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function FairValueLoading() {
  return (
    <FairValueFrame>
      <FairValueContentLoading />
    </FairValueFrame>
  )
}

function FairValueContentLoading() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-28 rounded-xl" />
      <div className="grid gap-4 xl:grid-cols-[minmax(260px,0.68fr)_minmax(0,1.62fr)]">
        <Skeleton className="h-[650px] rounded-xl" />
        <Skeleton className="h-[650px] rounded-xl" />
      </div>
    </div>
  )
}

function shortDate(value: string) {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return value
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(timestamp)
}
