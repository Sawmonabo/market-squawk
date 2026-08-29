import {
  BadgeCheck,
  Ban,
  BookOpenCheck,
  CircleAlert,
  Database,
  FileCheck2,
  Landmark,
  Layers3,
  ScrollText,
  ShieldCheck,
} from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { formatMoney, humanize } from "@/lib/formatters"

import type {
  FairValueApproval,
  FairValueAuditEvent,
  FairValueHierarchy,
  FairValueInput,
  FairValueMarketAccess,
  FairValueMeasurement,
} from "./fair-value-contracts"

export function FairValueDetail({
  measurement,
  auditBoundary,
}: {
  measurement: FairValueMeasurement
  auditBoundary: FairValueAuditBoundary
}) {
  const classification = measurement.classification
  const inputs = measurement.evidence?.inputs
  const reasons = measurement.explanation?.reasons
  const approvals = measurement.approvalStatus?.approvals
  const auditEvents = measurement.auditEvents
  const marketAccess = mergeMarketAccess(measurement, inputs)
  const qualities = unique(inputs?.map((input) => input.dataQuality))
  const depths = unique(
    inputs
      ?.map((input) => input.marketDepth)
      .filter((value): value is NonNullable<typeof value> => value !== undefined),
  )

  return (
    <article className="min-w-0 overflow-hidden rounded-xl border border-border bg-card/35">
      <header className="border-b border-border bg-card/45 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
              Fair-value measurement
            </p>
            <h2 className="mt-2 break-words text-xl font-semibold">
              {measurement.instrumentId}
            </h2>
            <p className="mt-1 font-mono text-2xl font-semibold">
              {formatMoney(measurement.amount)}
            </p>
          </div>
          <HierarchyBadge hierarchy={classification?.hierarchy ?? null} />
        </div>
        <p className="mt-4 max-w-3xl text-xs leading-5 text-muted-foreground">
          Prepared by {measurement.preparedBy} using {humanize(measurement.method)}. The amount,
          method, supporting inputs, classification, and approval history are reviewed separately.
        </p>
      </header>

      <div className="space-y-4 p-4 lg:p-5">
        <SemanticSeparation
          hierarchy={classification?.hierarchy ?? null}
          depths={depths}
          qualities={qualities}
        />

        <div className="grid gap-4 lg:grid-cols-2">
          <MeasurementPanel measurement={measurement} />
          <ClassificationPanel measurement={measurement} />
        </div>

        <LevelOneReview reasons={reasons} classificationLoaded={classification !== undefined} />
        <EvidencePanel inputs={inputs} expectedCount={measurement.inputCount} />

        <div className="grid gap-4 lg:grid-cols-2">
          <MarketAccessPanel assessments={marketAccess} loaded={inputs !== undefined} />
          <GovernancePanel
            measurement={measurement}
            approvals={approvals}
            evaluatedAt={measurement.approvalStatus?.at}
          />
        </div>

        <AuditPanel events={auditEvents} boundary={auditBoundary} />
      </div>
    </article>
  )
}

interface FairValueAuditBoundary {
  loadedEventCount: number
  totalEventCount: number | undefined
  hasMore: boolean
  loadingMore: boolean
  capped: boolean
  onLoadMore: () => void
}

function SemanticSeparation({
  hierarchy,
  depths,
  qualities,
}: {
  hierarchy: FairValueHierarchy | null
  depths: string[] | undefined
  qualities: string[] | undefined
}) {
  return (
    <section
      aria-label="Separate fair-value, market-depth, and quality classifications"
      className="grid gap-3 md:grid-cols-3"
    >
      <SemanticCard
        icon={Landmark}
        eyebrow="Fair-value hierarchy"
        value={hierarchy ? humanize(hierarchy) : "Not loaded"}
        detail="ASC 820 / IFRS 13 classification of valuation evidence."
        tone="primary"
      />
      <SemanticCard
        icon={Layers3}
        eyebrow="Market-depth level"
        value={depths?.length ? depths.map(humanize).join(", ") : "Not reported"}
        detail="Order-book granularity. It is not inferred from hierarchy or method."
        tone="neutral"
      />
      <SemanticCard
        icon={ShieldCheck}
        eyebrow="Data-quality class"
        value={qualities?.length ? qualities.map(humanize).join(", ") : "Not loaded"}
        detail="How usable the supporting information is. It does not assign hierarchy."
        tone={qualities?.includes("direct_verified") ? "good" : "warning"}
      />
    </section>
  )
}

function SemanticCard({
  icon: Icon,
  eyebrow,
  value,
  detail,
  tone,
}: {
  icon: typeof Landmark
  eyebrow: string
  value: string
  detail: string
  tone: "primary" | "good" | "warning" | "neutral"
}) {
  const iconTone =
    tone === "good"
      ? "text-emerald-300"
      : tone === "warning"
        ? "text-amber-200"
        : tone === "primary"
          ? "text-primary"
          : "text-muted-foreground"
  return (
    <div className="rounded-lg border border-border bg-background/40 p-4">
      <Icon className={`size-4 ${iconTone}`} aria-hidden="true" />
      <p className="mt-3 text-[9px] uppercase tracking-wider text-muted-foreground">
        {eyebrow}
      </p>
      <p className="mt-1 text-sm font-semibold">{value}</p>
      <p className="mt-2 text-[10px] leading-4 text-muted-foreground">{detail}</p>
    </div>
  )
}

function MeasurementPanel({ measurement }: { measurement: FairValueMeasurement }) {
  return (
    <Panel title="Measurement" icon={FileCheck2}>
      <dl className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
        <Fact label="Amount" value={formatMoney(measurement.amount)} />
        <Fact label="Amount basis" value={humanize(measurement.amount.amountBasis)} />
        <Fact label="Declared scale" value={String(measurement.amount.scale)} />
        <Fact label="Method" value={humanize(measurement.method)} />
        <Fact label="Input count" value={measurement.inputCount.toLocaleString()} />
        <Fact label="Measurement at" value={dateTime(measurement.measurementAt)} />
        <Fact label="Prepared at" value={dateTime(measurement.preparedAt)} />
        <Fact label="Prepared by" value={measurement.preparedBy} />
        <Fact label="Account" value={measurement.accountId} />
      </dl>
    </Panel>
  )
}

function ClassificationPanel({ measurement }: { measurement: FairValueMeasurement }) {
  const classification = measurement.classification
  if (!classification) {
    return (
      <Panel title="Classification" icon={Landmark}>
        <MissingDetail text="The measurement summary is available, but its classification, basis, and explanation are not available in this view." />
      </Panel>
    )
  }

  return (
    <Panel title="Classification" icon={Landmark}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[9px] uppercase tracking-wider text-muted-foreground">Hierarchy</p>
          <p className="mt-1 text-lg font-semibold">{humanize(classification.hierarchy)}</p>
        </div>
        <span className="rounded-full border border-primary/35 bg-primary/10 px-2.5 py-1 font-mono text-[9px] text-primary">
          Ruleset v{classification.rulesetVersion}
        </span>
      </div>
      <dl className="mt-4 grid gap-x-4 gap-y-3 sm:grid-cols-2">
        <Fact label="Basis" value={humanize(classification.basis.kind)} />
        <Fact
          label="Explanation"
          value={`${classification.truthTableItemCount} checks · ${classification.reasonCount} reasons`}
        />
      </dl>
      {classification.basis.kind === "override" ? (
        <Alert className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Override-based classification</AlertTitle>
          <AlertDescription>
            This decision references a separately governed override. An override cannot promote a
            measurement to Level 1.
          </AlertDescription>
        </Alert>
      ) : null}
    </Panel>
  )
}

function LevelOneReview({
  reasons,
  classificationLoaded,
}: {
  reasons: { inputId: string | null; code: string }[] | undefined
  classificationLoaded: boolean
}) {
  if (!classificationLoaded || reasons === undefined) {
    return (
      <Panel title="Level 1 qualification review" icon={BookOpenCheck}>
        <MissingDetail text="Classification reasons are not available. No Level 1 eligibility or disqualification is inferred from the measurement method, amount, or market availability alone." />
      </Panel>
    )
  }

  return (
    <Panel title="Level 1 qualification review" icon={BookOpenCheck}>
      {reasons.length === 0 ? (
        <div className="flex gap-3 rounded-lg border border-emerald-400/20 bg-emerald-400/5 p-4">
          <BadgeCheck className="mt-0.5 size-4 shrink-0 text-emerald-300" aria-hidden="true" />
          <div>
            <p className="text-xs font-semibold">No Level 1 disqualifying condition was found</p>
            <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
              This result applies to the current saved classification and supporting information.
            </p>
          </div>
        </div>
      ) : (
        <ul className="grid gap-2 md:grid-cols-2">
          {reasons.map((reason, index) => (
            <li
              key={`${reason.inputId ?? "measurement"}:${reason.code}:${index}`}
              className="flex gap-2 rounded-lg border border-amber-400/15 bg-amber-400/5 p-3"
            >
              <Ban className="mt-0.5 size-3.5 shrink-0 text-amber-200" aria-hidden="true" />
              <span>
                <span className="block text-xs font-medium">{humanize(reason.code)}</span>
                <span className="mt-1 block font-mono text-[9px] text-muted-foreground">
                  {reason.inputId ? "Valuation input" : "Measurement-wide"}
                </span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  )
}

function EvidencePanel({
  inputs,
  expectedCount,
}: {
  inputs: FairValueInput[] | undefined
  expectedCount: number
}) {
  return (
    <Panel title="Valuation inputs and evidence" icon={Database}>
      {inputs === undefined ? (
        <MissingDetail text={`The measurement uses ${expectedCount.toLocaleString()} input${expectedCount === 1 ? "" : "s"}, but their timing, observability, quality, and verification details are not available in this view.`} />
      ) : inputs.length === 0 ? (
        <MissingDetail text="Supporting input details are not available for this measurement." />
      ) : (
        <div className="space-y-3">
          {inputs.map((input) => (
            <EvidenceInput key={input.inputId} input={input} />
          ))}
        </div>
      )}
    </Panel>
  )
}

function EvidenceInput({ input }: { input: FairValueInput }) {
  const current = input.dataQuality === "direct_verified"
  return (
    <details className="group rounded-lg border border-border bg-background/35 px-4 py-3">
      <summary className="cursor-pointer list-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
        <span className="flex flex-wrap items-start justify-between gap-3">
          <span>
            <span className="block text-xs font-semibold">
              {humanize(input.evidence.origin.kind)} input
            </span>
            <span className="mt-1 block text-[9px] text-muted-foreground">
              {humanize(input.significance)}
            </span>
          </span>
          <span
            className={`rounded-full border px-2 py-1 text-[9px] ${
              current
                ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-300"
                : "border-amber-400/25 bg-amber-400/10 text-amber-200"
            }`}
          >
            Data quality · {humanize(input.dataQuality)}
          </span>
        </span>
      </summary>
      <dl className="mt-4 grid gap-x-4 gap-y-3 border-t border-border pt-4 sm:grid-cols-2 lg:grid-cols-3">
        <Fact label="Relationship" value={humanize(input.relationship)} />
        <Fact label="Observability" value={humanize(input.observability)} />
        <Fact label="Adjustment" value={humanize(input.adjustment)} />
        <Fact label="Market activity" value={humanize(input.marketActivity)} />
        <Fact label="Market access" value={humanize(input.marketAccess)} />
        <Fact
          label="Market depth"
          value={input.marketDepth ? humanize(input.marketDepth) : "Not reported"}
        />
        <Fact label="Input amount" value={formatMoney(input.amount)} />
        <Fact label="Verification status" value={humanize(input.evidence.verification)} />
        <Fact
          label="Available since"
          value={input.evidence.availableAt ? dateTime(input.evidence.availableAt) : "Not reported"}
        />
        <Fact
          label="Observation time"
          value={input.evidence.sourceTimestamp ? dateTime(input.evidence.sourceTimestamp) : "Not reported"}
        />
        <Fact
          label="Qualification valid until"
          value={
            input.evidence.qualificationValidUntil
              ? dateTime(input.evidence.qualificationValidUntil)
              : "Not reported"
          }
        />
        <Fact label="Recorded at" value={dateTime(input.evidence.ingestedAt)} />
      </dl>
    </details>
  )
}

function MarketAccessPanel({
  assessments,
  loaded,
}: {
  assessments: FairValueMarketAccess[]
  loaded: boolean
}) {
  return (
    <Panel title="Market access" icon={ShieldCheck}>
      {!loaded ? (
        <MissingDetail text="Market-access details are not available. Accessibility requires a reporting-entity assessment and is not inferred from data availability alone." />
      ) : assessments.length === 0 ? (
        <MissingDetail text="No approved market-access assessment is attached to the returned valuation inputs." />
      ) : (
        <div className="space-y-3">
          {assessments.map((assessment) => (
            <div key={assessment.assessmentId} className="rounded-lg border border-border p-3">
              <div className="flex items-center justify-between gap-3">
                <p className="text-xs font-semibold">{assessment.venueId}</p>
                <span className="rounded-full border border-border px-2 py-0.5 text-[9px]">
                  {humanize(assessment.conclusion)}
                </span>
              </div>
              <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
                {assessment.rationale || "No rationale text returned."}
              </p>
              <p className="mt-2 font-mono text-[9px] text-muted-foreground">
                {dateTime(assessment.effectiveFrom)} → {dateTime(assessment.effectiveUntil)}
              </p>
            </div>
          ))}
        </div>
      )}
    </Panel>
  )
}

function GovernancePanel({
  measurement,
  approvals,
  evaluatedAt,
}: {
  measurement: FairValueMeasurement
  approvals: FairValueApproval[] | undefined
  evaluatedAt: string | undefined
}) {
  const override = measurement.classification?.basis.kind === "override"
  return (
    <Panel title="Approval, override, and revocation" icon={BadgeCheck}>
      {override ? (
        <div className="mb-3 rounded-lg border border-primary/25 bg-primary/5 p-3">
          <p className="text-xs font-semibold">Governed override applied</p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
            The classification includes a separately reviewed override.
          </p>
        </div>
      ) : null}
      {evaluatedAt ? (
        <p className="mb-3 text-[10px] text-muted-foreground">
          Status evaluated at {dateTime(evaluatedAt)}.
        </p>
      ) : null}
      {approvals === undefined ? (
        <MissingDetail text="Approval status, override details, and revocation evidence were not included in this view." />
      ) : approvals.length === 0 ? (
        <MissingDetail text="No approval is active for this measurement at the requested date and time." />
      ) : (
        <div className="space-y-3">
          {approvals.map((approval) => (
            <ApprovalCard key={approval.approvalId} approval={approval} />
          ))}
        </div>
      )}
    </Panel>
  )
}

function ApprovalCard({ approval }: { approval: FairValueApproval }) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold">{approval.approvedBy}</p>
        <span className="rounded-full border border-border px-2 py-0.5 text-[9px]">
          {humanize(approval.status)}
        </span>
      </div>
      <p className="mt-2 font-mono text-[9px] text-muted-foreground">
        Approved {dateTime(approval.approvedAt)} · expires {dateTime(approval.expiresAt)}
      </p>
      {approval.revocation ? (
        <div className="mt-3 border-t border-border pt-3">
          <p className="text-[10px] font-semibold text-amber-200">Revoked</p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
            {approval.revocation.reason} · {approval.revocation.revokedBy} ·{" "}
            {dateTime(approval.revocation.revokedAt)}
          </p>
        </div>
      ) : null}
    </div>
  )
}

function AuditPanel({
  events,
  boundary,
}: {
  events: FairValueAuditEvent[] | undefined
  boundary: FairValueAuditBoundary
}) {
  return (
    <Panel title="Audit trail" icon={ScrollText}>
      {events === undefined ? (
        <MissingDetail text="Review history is not available in this view." />
      ) : events.length === 0 ? (
        <MissingDetail text="No matching fair-value review events were found." />
      ) : (
        <ol className="space-y-2">
          {events.map((event) => (
            <li key={event.auditEventId} className="flex gap-3 rounded-lg border border-border p-3">
              <span className="font-mono text-[9px] text-muted-foreground">
                #{event.sequence}
              </span>
              <span className="min-w-0 flex-1">
                <span className="block text-xs font-semibold">
                  {humanize(event.subject.kind)}
                </span>
                <span className="mt-1 block text-[10px] text-muted-foreground">
                  {event.actor} · {dateTime(event.businessAt)}
                </span>
              </span>
            </li>
          ))}
        </ol>
      )}
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3">
        <p className="text-[10px] leading-4 text-muted-foreground">
          Showing {events?.length.toLocaleString() ?? 0} related review events.
        </p>
        {boundary.hasMore ? (
          <button
            type="button"
            className="rounded-md border border-border px-3 py-1.5 text-[10px] font-medium transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
            onClick={boundary.onLoadMore}
            disabled={boundary.loadingMore}
          >
            {boundary.loadingMore ? "Loading…" : "Load more history"}
          </button>
        ) : null}
      </div>
      {boundary.capped ? (
        <p className="mt-2 text-[10px] leading-4 text-amber-200">
          Additional older review history is available in Logs.
        </p>
      ) : null}
    </Panel>
  )
}

function Panel({
  title,
  icon: Icon,
  children,
}: {
  title: string
  icon: typeof Landmark
  children: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-border bg-background/30 p-4">
      <div className="mb-4 flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">{title}</h3>
      </div>
      {children}
    </section>
  )
}

function MissingDetail({ text }: { text: string }) {
  return (
    <div className="flex gap-2 rounded-lg border border-dashed border-border p-3">
      <CircleAlert className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
      <p className="text-[11px] leading-5 text-muted-foreground">{text}</p>
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs text-foreground/85">{value}</dd>
    </div>
  )
}

function HierarchyBadge({ hierarchy }: { hierarchy: FairValueHierarchy | null }) {
  return (
    <span className="rounded-full border border-primary/35 bg-primary/10 px-3 py-1.5 text-xs font-semibold text-primary">
      {hierarchy ? humanize(hierarchy) : "Classification not loaded"}
    </span>
  )
}

function mergeMarketAccess(
  measurement: FairValueMeasurement,
  inputs: FairValueInput[] | undefined,
) {
  const byId = new Map<string, FairValueMarketAccess>()
  for (const assessment of measurement.marketAccess ?? []) {
    byId.set(assessment.assessmentId, assessment)
  }
  for (const input of inputs ?? []) {
    const assessment = input.marketAccessAssessment
    if (assessment) byId.set(assessment.assessmentId, assessment)
  }
  return [...byId.values()]
}

function unique<T>(values: T[] | undefined) {
  return values ? [...new Set(values)] : undefined
}

function dateTime(value: string) {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return value
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(timestamp)
}
