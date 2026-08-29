import * as React from "react"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { FileCheck2, FlaskConical, Play, ShieldAlert } from "lucide-react"

import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import type { DesktopBootstrap, InputTicket } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  parseEvaluationResult,
  parseModelJobReceipt,
  type EvaluationResult,
  type ModelMetadata,
} from "./models-contracts"

type PendingAction =
  | { kind: "evaluate"; values: number[] }
  | { kind: "train"; configuration: InputTicket; authority: InputTicket }

export function ModelWorkflows({
  bootstrap,
  transport,
  metadata,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  metadata: ModelMetadata | null
}) {
  const queryClient = useQueryClient()
  const operations = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const canEvaluate = operations.has("Model.Evaluate")
  const canTrain = operations.has("Model.StartTraining")
  const [featureValues, setFeatureValues] = React.useState<Record<string, string>>({})
  const [configuration, setConfiguration] = React.useState<InputTicket | null>(null)
  const [authority, setAuthority] = React.useState<InputTicket | null>(null)
  const [stageError, setStageError] = React.useState<string | null>(null)
  const [pending, setPending] = React.useState<PendingAction | null>(null)
  const [evaluation, setEvaluation] = React.useState<EvaluationResult | null>(null)
  const [trainingJob, setTrainingJob] = React.useState<string | null>(null)
  const [staging, setStaging] = React.useState<"configuration" | "model_authority" | null>(null)

  React.useEffect(() => {
    setFeatureValues({})
    setEvaluation(null)
  }, [metadata?.modelId, metadata?.bundleVersion])

  const mutation = useMutation({
    mutationFn: async (action: PendingAction) => {
      if (action.kind === "evaluate") {
        if (!metadata) throw new Error("No admitted model metadata is selected.")
        return {
          kind: "evaluate" as const,
          value: parseEvaluationResult(
            await transport.modelControl(
              {
                action: "evaluate",
                modelId: metadata.modelId,
                input: {
                  bundleId: metadata.bundleId,
                  bundleVersion: metadata.bundleVersion,
                  featureValues: action.values,
                },
              },
              true,
            ),
          ),
        }
      }
      return {
        kind: "train" as const,
        value: parseModelJobReceipt(
          await transport.modelControl(
            {
              action: "startTraining",
              configTicketId: action.configuration.id,
              authorityTicketId: action.authority.id,
            },
            true,
          ),
        ),
      }
    },
    onSuccess: async (result) => {
      setPending(null)
      if (result.kind === "evaluate") {
        setEvaluation(result.value)
      } else {
        setTrainingJob(result.value.jobId)
        setConfiguration(null)
        setAuthority(null)
        await queryClient.invalidateQueries({
          queryKey: productKeys.domain(bootstrap.runtime, "job"),
        })
      }
    },
  })

  const stage = async (kind: "configuration" | "model_authority") => {
    setStageError(null)
    setStaging(kind)
    try {
      const ticket = await transport.stageTrainingInput(kind)
      if (ticket) {
        if (kind === "configuration") setConfiguration(ticket)
        else setAuthority(ticket)
      }
    } catch {
      setStageError("The selected file could not be prepared. Check the file and try again.")
    } finally {
      setStaging(null)
    }
  }

  const evaluationValues = metadata?.features.map((feature) => {
    const value = Number(featureValues[feature.semanticDigest])
    return Number.isFinite(value) && featureValues[feature.semanticDigest]?.trim() !== ""
      ? value
      : null
  })
  const evaluationReady =
    metadata !== null &&
    evaluationValues !== undefined &&
    evaluationValues.every((value) => value !== null)

  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div>
        <p className="font-mono text-[10px] uppercase tracking-wider text-primary">
          Guided research workflows
        </p>
        <h2 className="mt-2 text-xl font-semibold">Evaluate or train</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Test a model with your assumptions or start training after reviewing the inputs. Neither
          action can place a trade.
        </p>
      </div>

      <div className="mt-5 grid gap-4 xl:grid-cols-2">
        <div className="rounded-lg border border-border bg-background/25 p-4">
          <div className="flex items-center gap-2">
            <FlaskConical className="size-4 text-primary" aria-hidden="true" />
            <h3 className="text-sm font-semibold">Test the selected model</h3>
          </div>
          {!canEvaluate ? (
            <WorkflowUnavailable text="Model testing is unavailable." />
          ) : !metadata ? (
            <WorkflowUnavailable text="Select a model with complete metadata before evaluating it." />
          ) : (
            <>
              <div className="mt-4 grid gap-3">
                {metadata.features.map((feature, index) => (
                  <label key={feature.semanticDigest} className="grid gap-1.5 text-xs">
                    <span>
                      {index + 1}. {feature.name} v{feature.version}
                    </span>
                    <input
                      type="number"
                      step="any"
                      inputMode="decimal"
                      value={featureValues[feature.semanticDigest] ?? ""}
                      onChange={(event) =>
                        setFeatureValues((current) => ({
                          ...current,
                          [feature.semanticDigest]: event.target.value,
                        }))
                      }
                      className="h-9 rounded-md border border-input bg-transparent px-3 font-mono outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    />
                    <span className="text-[10px] leading-4 text-muted-foreground">
                      {feature.normalizer.kind === "standard"
                        ? `Raw value; admitted normalizer uses mean ${feature.normalizer.mean} and scale ${feature.normalizer.scale}.`
                        : "Raw value; identity normalization."}
                    </span>
                  </label>
                ))}
              </div>
              <Button
                className="mt-4"
                variant="outline"
                disabled={!evaluationReady || mutation.isPending}
                onClick={() => {
                  if (evaluationReady) {
                    setPending({ kind: "evaluate", values: evaluationValues as number[] })
                    mutation.reset()
                  }
                }}
              >
                <ShieldAlert aria-hidden="true" />
                Review evaluation
              </Button>
            </>
          )}
          {evaluation ? <EvaluationEvidence result={evaluation} /> : null}
        </div>

        <div className="rounded-lg border border-border bg-background/25 p-4">
          <div className="flex items-center gap-2">
            <FileCheck2 className="size-4 text-primary" aria-hidden="true" />
            <h3 className="text-sm font-semibold">Start governed training</h3>
          </div>
          {!canTrain ? (
            <WorkflowUnavailable text="Model training is unavailable." />
          ) : (
            <>
              <p className="mt-2 text-xs leading-5 text-muted-foreground">
                Choose the training settings and approved model definition, then review before
                starting.
              </p>
              <div className="mt-4 grid gap-2 sm:grid-cols-2">
                <StageButton
                  label="Training configuration"
                  ticket={configuration}
                  loading={staging === "configuration"}
                  onClick={() => void stage("configuration")}
                />
                <StageButton
                  label="Model definition"
                  ticket={authority}
                  loading={staging === "model_authority"}
                  onClick={() => void stage("model_authority")}
                />
              </div>
              <Button
                className="mt-4"
                disabled={!configuration || !authority || mutation.isPending}
                onClick={() => {
                  if (configuration && authority) {
                    setPending({ kind: "train", configuration, authority })
                    mutation.reset()
                  }
                }}
              >
                <Play aria-hidden="true" />
                Review training start
              </Button>
            </>
          )}
          {trainingJob ? (
            <p className="mt-3 text-xs text-emerald-300">
              Training queued as job <span className="font-mono">{trainingJob}</span>.
            </p>
          ) : null}
          {stageError ? (
            <WorkflowError text="The selected file could not be prepared. Check the file and try again." />
          ) : null}
        </div>
      </div>

      <Dialog open={pending !== null} onOpenChange={(open) => { if (!open && !mutation.isPending) setPending(null) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {pending?.kind === "evaluate" ? "Retain this evaluation?" : "Start this training job?"}
            </DialogTitle>
            <DialogDescription>
              {pending?.kind === "evaluate"
                ? "The selected model will be tested with these values. The result is research only and cannot place a trade."
                : "Training will start with the selected files. A started job does not mean the resulting model is suitable for investment use."}
            </DialogDescription>
          </DialogHeader>
          {pending?.kind === "evaluate" && metadata ? (
            <dl className="grid gap-2 rounded-lg border border-border p-3 text-xs sm:grid-cols-2">
              {metadata.features.map((feature, index) => (
                <div key={feature.semanticDigest}>
                  <dt className="text-muted-foreground">{feature.name}</dt>
                  <dd className="font-mono">{pending.values[index]}</dd>
                </div>
              ))}
            </dl>
          ) : null}
          {pending?.kind === "train" ? (
            <dl className="grid gap-2 rounded-lg border border-border p-3 text-xs">
              <TicketFact label="Configuration" ticket={pending.configuration} />
              <TicketFact label="Model definition" ticket={pending.authority} />
            </dl>
          ) : null}
          {mutation.isError ? (
            <WorkflowError text="The requested model action could not be completed. Review the inputs and try again." />
          ) : null}
          <DialogFooter>
            <Button variant="outline" disabled={mutation.isPending} onClick={() => setPending(null)}>
              Cancel
            </Button>
            <Button disabled={!pending || mutation.isPending} onClick={() => { if (pending) mutation.mutate(pending) }}>
              {mutation.isPending ? "Submitting…" : "Confirm locally"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function EvaluationEvidence({ result }: { result: EvaluationResult }) {
  return (
    <div className="mt-4 rounded-lg border border-emerald-400/25 bg-emerald-400/5 p-3">
      <p className="text-xs font-medium text-emerald-300">
        {result.decision.replaceAll("_", " ")} · score {result.score.toLocaleString(undefined, { maximumFractionDigits: 6 })} · confidence {result.confidence.toLocaleString(undefined, { maximumFractionDigits: 6 })}
      </p>
      <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
        Confidence reflects this model output, not certainty of profit. If the model cannot produce
        a valid result, no action is suggested.
      </p>
    </div>
  )
}

function StageButton({
  label,
  ticket,
  loading,
  onClick,
}: {
  label: string
  ticket: InputTicket | null
  loading: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={loading}
      className="rounded-lg border border-border p-3 text-left outline-none hover:bg-accent/40 focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-60"
    >
      <span className="block text-xs font-medium">{loading ? "Opening secure picker…" : label}</span>
      <span className="mt-1 block text-[10px] text-muted-foreground">
        {ticket ? `${ticket.byteLength.toLocaleString()} bytes · ${ticket.mediaType}` : "No staged file"}
      </span>
    </button>
  )
}

function TicketFact({ label, ticket }: { label: string; ticket: InputTicket }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-all font-mono text-[10px]">{ticket.id} · {ticket.byteLength.toLocaleString()} bytes</dd>
    </div>
  )
}

function WorkflowUnavailable({ text }: { text: string }) {
  return <p className="mt-3 text-xs leading-5 text-muted-foreground">{text}</p>
}

function WorkflowError({ text }: { text: string }) {
  return <p className="mt-3 text-xs leading-5 text-red-300">{text}</p>
}
