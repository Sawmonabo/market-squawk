import * as React from "react"
import {
  Download,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  Trash2,
  Wrench,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import type {
  InstallationControlResult,
  InstallationStatus,
} from "@/lib/schemas"
import type { InstallationControlRequest } from "@/lib/transport"

export function InstallationPage({
  recovery = false,
}: {
  recovery?: boolean
}) {
  const product = useProduct()
  const [status, setStatus] = React.useState<InstallationStatus | null>(null)
  const [result, setResult] =
    React.useState<InstallationControlResult | null>(null)
  const [busy, setBusy] =
    React.useState<InstallationControlRequest["action"] | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [uninstallOpen, setUninstallOpen] = React.useState(false)

  const run = React.useCallback(
    async (action: InstallationControlRequest["action"]) => {
      setBusy(action)
      setError(null)
      try {
        const response = await product.transport.installation({ action })
        setStatus(response.status)
        setResult(response)
        if (action !== "status") {
          product.refresh()
        }
        return true
      } catch (requestError) {
        setError(messageFrom(requestError))
        return false
      } finally {
        setBusy(null)
      }
    },
    [product],
  )

  React.useEffect(() => {
    void run("status")
  }, [run])

  const installed = status?.installed === true
  const healthy = installed && status.healthy
  const canUpdate = healthy && status.channel_manifest_url !== null
  const canRollback = installed && status.previous_version !== null

  return (
    <main className="mx-auto w-full max-w-6xl p-6 lg:p-8">
      <header className="max-w-3xl">
        <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
          Operations
        </p>
        <h1 className="mt-2 text-2xl font-semibold tracking-tight">
          {recovery ? "Backup & Recovery" : "Updates"}
        </h1>
        <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
          {recovery
            ? "Verify or restore the installed program without deleting your configuration, portfolios, datasets, models, logs, or artifacts."
            : "Review the active verified release and install a complete newer version only when you approve it."}
        </p>
      </header>

      {error ? (
        <Alert className="mt-6" variant="destructive">
          <AlertTitle>The operation did not complete</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
      {result?.restartRequired ? (
        <Alert className="mt-6">
          <RefreshCw aria-hidden="true" />
          <AlertTitle>Restart Market Squawk</AlertTitle>
          <AlertDescription>
            Close and reopen the desktop application to use the newly selected
            program version.
          </AlertDescription>
        </Alert>
      ) : null}
      {result?.action === "update" &&
      result.receipt === null &&
      !result.restartRequired ? (
        <Alert className="mt-6">
          <ShieldCheck aria-hidden="true" />
          <AlertTitle>Market Squawk is up to date</AlertTitle>
          <AlertDescription>
            The verified update channel does not contain a newer release.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="mt-6 grid gap-4 lg:grid-cols-[1fr_360px]">
        <section className="rounded-xl border border-border bg-card/50 p-5">
          <div className="flex items-start gap-3">
            <span className="flex size-9 items-center justify-center rounded-lg border border-border bg-background">
              <ShieldCheck
                className={healthy ? "size-4 text-emerald-400" : "size-4 text-amber-400"}
                aria-hidden="true"
              />
            </span>
            <div>
              <h2 className="text-sm font-semibold">Installed release</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                {busy === "status"
                  ? "Verifying every installed component…"
                  : healthy
                    ? "Every retained component matches the active release manifest."
                    : installed
                      ? "The active release needs repair before it can be trusted."
                      : "No complete release is installed for this user."}
              </p>
            </div>
            <Button
              className="ml-auto"
              type="button"
              size="sm"
              variant="ghost"
              disabled={busy !== null}
              onClick={() => void run("status")}
            >
              <RefreshCw aria-hidden="true" />
              Refresh
            </Button>
          </div>

          <dl className="mt-5 grid gap-4 border-t border-border pt-5 sm:grid-cols-2">
            <ReleaseFact
              label="Active version"
              value={status?.active_version ?? "Not installed"}
            />
            <ReleaseFact
              label="Previous version"
              value={status?.previous_version ?? "None retained"}
            />
            <ReleaseFact
              label="Platform"
              value={status?.target ?? "Not available"}
            />
            <ReleaseFact
              label="Update channel"
              value={
                status?.channel_manifest_url
                  ? "Verified GitHub Releases"
                  : "Not configured"
              }
            />
          </dl>
        </section>

        <section className="rounded-xl border border-border bg-card/40 p-5">
          <h2 className="text-sm font-semibold">
            {recovery ? "Recovery controls" : "Release controls"}
          </h2>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            Changes use the same verified Rust lifecycle as terminal and native
            package installation.
          </p>
          <div className="mt-5 grid gap-2">
            {!recovery ? (
              <Button
                type="button"
                disabled={!canUpdate || busy !== null}
                onClick={() => void run("update")}
              >
                <Download aria-hidden="true" />
                {busy === "update" ? "Installing update…" : "Check and update"}
              </Button>
            ) : null}
            <Button
              type="button"
              variant="outline"
              disabled={!installed || busy !== null}
              onClick={() => void run("repair")}
            >
              <Wrench aria-hidden="true" />
              {busy === "repair" ? "Repairing…" : "Verify and repair"}
            </Button>
            <Button
              type="button"
              variant="outline"
              disabled={!canRollback || busy !== null}
              onClick={() => void run("rollback")}
            >
              <RotateCcw aria-hidden="true" />
              Restore previous version
            </Button>
            {recovery ? (
              <Dialog open={uninstallOpen} onOpenChange={setUninstallOpen}>
                <DialogTrigger asChild>
                  <Button
                    type="button"
                    variant="ghost"
                    disabled={!installed || busy !== null}
                    className="text-red-300 hover:text-red-200"
                  >
                    <Trash2 aria-hidden="true" />
                    Uninstall programs
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Uninstall Market Squawk programs?</DialogTitle>
                    <DialogDescription>
                      This removes installed program versions and launchers.
                      Configuration, credentials, portfolios, datasets, models,
                      logs, and artifacts remain untouched.
                    </DialogDescription>
                  </DialogHeader>
                  <DialogFooter>
                    <Button
                      type="button"
                      variant="ghost"
                      onClick={() => setUninstallOpen(false)}
                    >
                      Keep installed
                    </Button>
                    <Button
                      type="button"
                      variant="destructive"
                      onClick={async () => {
                        if (await run("uninstall")) {
                          setUninstallOpen(false)
                        }
                      }}
                    >
                      Uninstall programs only
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            ) : null}
          </div>
        </section>
      </div>
    </main>
  )
}

function ReleaseFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 break-words font-mono text-xs text-foreground">
        {value}
      </dd>
    </div>
  )
}
