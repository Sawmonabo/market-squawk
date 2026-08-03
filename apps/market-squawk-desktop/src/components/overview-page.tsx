import * as React from "react"
import { KeyRound, LoaderCircle, ShieldCheck } from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { SquawkSignal } from "@/components/squawk-signal"
import { SetupOverview } from "@/components/setup/setup-overview"
import { VerificationPanel } from "@/components/setup/verification-panel"
import { OverviewDashboard } from "@/features/overview/overview-dashboard"

export function OverviewPage() {
  const product = useProduct()
  if (product.status === "loading") {
    return <OverviewLoading />
  }
  if (product.status === "error") {
    if (product.serviceBootstrap) {
      return <ServiceBootstrapRequired bootstrap={product.serviceBootstrap} />
    }
    return (
      <div className="mx-auto max-w-3xl p-8">
        <Alert variant="destructive">
          <AlertTitle>Local application unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </div>
    )
  }
  const bootstrap = product.bootstrap
  const centralRiskAvailable = bootstrap.operations.some(
    (operation) => operation.domain === "bot",
  ) && bootstrap.operations.some(
    (operation) => operation.domain === "execution",
  )

  return (
    <div className="mx-auto w-full max-w-[1120px] space-y-4 p-5 lg:p-7">
      <section className="grid items-start gap-6 lg:grid-cols-[1fr_340px]">
        <div className="pt-1">
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Your decision workspace
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-[-0.04em] sm:text-4xl">
            <span className="text-white">Welcome to Market</span>{" "}
            <span className="text-primary">Squawk</span>
          </h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-muted-foreground">
            See what needs attention, check the evidence behind current data,
            and find anything in your installed workspace. Guided setup remains
            available below until every selected outcome is backed by its owning
            service.
          </p>
        </div>
        <SquawkSignal status={bootstrap.storage.label} />
      </section>

      <section
        aria-label="Application facts"
        className="grid overflow-hidden rounded-xl border border-border bg-card/55 sm:grid-cols-2 lg:grid-cols-4"
      >
        <Fact
          label="Application"
          value={bootstrap.storage.label}
          ready={bootstrap.storage.state === "ready"}
        />
        <Fact
          label="Release"
          value={`v${bootstrap.applicationVersion} · ${bootstrap.installation.label}`}
        />
        <Fact label="Model runtime" value={bootstrap.modelRuntime.label} />
        <Fact
          label="Execution safety"
          value={centralRiskAvailable ? "Central risk required" : "Unavailable"}
          ready={centralRiskAvailable}
        />
      </section>

      <OverviewDashboard
        transport={product.transport}
        scope={bootstrap.runtime}
      />

      <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
        <SetupOverview
          bootstrap={bootstrap}
          transport={product.transport}
          onRefresh={product.refresh}
        />
        <VerificationPanel bootstrap={bootstrap} />
      </div>

      <aside className="flex items-start gap-3 rounded-lg border border-border bg-card/20 px-4 py-3 text-[11px] leading-relaxed text-muted-foreground">
        <ShieldCheck
          className="mt-0.5 size-4 shrink-0 text-foreground/70"
          aria-hidden="true"
        />
        <span>
          Safe to close. Accepted provider work is checkpointed by the Rust
          authorities and resumes without exposing credentials or fabricating
          readiness.
        </span>
      </aside>
    </div>
  )
}

function ServiceBootstrapRequired({
  bootstrap,
}: {
  bootstrap: NonNullable<ReturnType<typeof useProduct>["serviceBootstrap"]>
}) {
  const product = useProduct()
  const [unlock, setUnlock] = React.useState("")
  const [submitting, setSubmitting] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const requiresUnlock =
    bootstrap.requirement === "encrypted_fallback_locked"

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const submittedUnlock = unlock
    setUnlock("")
    setError(null)
    if (requiresUnlock && !submittedUnlock) {
      setError("Enter the encrypted-fallback unlock before continuing.")
      return
    }
    setSubmitting(true)
    try {
      await product.transport.bootstrapService(
        requiresUnlock
          ? {
              action: "unlock_encrypted_fallback",
              unlock: submittedUnlock,
            }
          : { action: "retry_after_foreground_keyring" },
      )
      product.refresh()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="mx-auto w-full max-w-[760px] p-5 lg:p-7">
      <section className="rounded-xl border border-border bg-card/55 p-5">
        <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
          Guided setup
        </p>
        <h1 className="mt-2 text-2xl font-semibold">Unlock the local service</h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
          {requiresUnlock
            ? "The encrypted credential fallback is locked. Enter its unlock once so the native application can finish connecting."
            : "The operating-system credential service needs one foreground retry before the native application can finish connecting."}
        </p>
        <form onSubmit={submit} className="mt-5 space-y-3">
          {requiresUnlock ? (
            <div className="space-y-1.5">
              <Label htmlFor="service-fallback-unlock">Fallback unlock</Label>
              <div className="relative">
                <KeyRound
                  className="pointer-events-none absolute top-2.5 left-3 size-4 text-muted-foreground"
                  aria-hidden="true"
                />
                <Input
                  id="service-fallback-unlock"
                  type="password"
                  value={unlock}
                  onChange={(event) => setUnlock(event.currentTarget.value)}
                  autoComplete="current-password"
                  spellCheck={false}
                  className="pl-9 font-mono"
                  disabled={submitting}
                />
              </div>
            </div>
          ) : null}
          <Button type="submit" disabled={submitting}>
            {submitting ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : null}
            {requiresUnlock ? "Unlock local service" : "Retry local service"}
          </Button>
          {error ? (
            <p role="alert" className="text-sm text-red-400">
              {error}
            </p>
          ) : null}
        </form>
      </section>
    </div>
  )
}

function Fact({
  label,
  value,
  ready = false,
}: {
  label: string
  value: string
  ready?: boolean
}) {
  return (
    <div className="min-h-16 border-b border-border px-4 py-3 last:border-b-0 sm:odd:border-r lg:border-b-0 lg:border-r lg:last:border-r-0">
      <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 flex items-center gap-2 text-xs font-medium">
        {ready ? (
          <span
            className="size-1.5 rounded-full bg-[var(--success)]"
            aria-hidden="true"
          />
        ) : null}
        {value}
      </p>
    </div>
  )
}

function OverviewLoading() {
  return (
    <div className="mx-auto w-full max-w-[1120px] space-y-5 p-7" aria-label="Loading workspace">
      <Skeleton className="h-4 w-32" />
      <Skeleton className="h-11 w-3/5" />
      <Skeleton className="h-5 w-4/5" />
      <Skeleton className="h-16 w-full rounded-xl" />
      <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
        <Skeleton className="h-80 rounded-xl" />
        <Skeleton className="h-80 rounded-xl" />
      </div>
    </div>
  )
}
