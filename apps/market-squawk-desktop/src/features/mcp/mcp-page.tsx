import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { CircleAlert, RefreshCw, ShieldCheck } from "lucide-react"

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
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { ClientCard } from "./client-card"
import {
  actionDescription,
  actionLabel,
  requireMcpTransport,
  type McpClientAction,
  type McpClientControlRequest,
} from "./contracts"
import { ServiceEvidence } from "./service-evidence"

export function McpPage() {
  const product = useProduct()

  if (product.status === "loading") return <McpLoading />
  if (product.status === "error") {
    return (
      <McpFrame>
        <Unavailable title="MCP service is unavailable" detail={product.error} />
      </McpFrame>
    )
  }

  return (
    <McpWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function McpWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const [pending, setPending] = React.useState<McpClientControlRequest | null>(null)
  const [announcement, setAnnouncement] = React.useState("")
  const queryKey = [...productKeys.domain(bootstrap.runtime, "mcp"), "clients"] as const
  const status = useQuery({
    queryKey,
    queryFn: () => requireMcpTransport(transport).mcpClients(),
    refetchInterval: 15_000,
  })
  const control = useMutation({
    mutationFn: (request: McpClientControlRequest) =>
      requireMcpTransport(transport).mcpClientControl(request, true),
    onSuccess: async (next, request) => {
      queryClient.setQueryData(queryKey, next)
      setAnnouncement(`${clientName(request.client)} ${actionPastTense(request.action)}.`)
      setPending(null)
      await queryClient.invalidateQueries({ queryKey })
    },
  })

  const requestAction = (
    client: "claude_code" | "codex",
    action: McpClientAction,
  ) => {
    control.reset()
    setPending({ client, action })
  }

  return (
    <McpFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => void status.refetch()}
          disabled={status.isFetching || control.isPending}
        >
          <RefreshCw className={status.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Refresh discovery
        </Button>
      }
    >
      <p className="sr-only" aria-live="polite">{announcement}</p>

      {status.isPending ? (
        <StatusLoading />
      ) : status.isError ? (
        <Unavailable
          title="MCP status could not be loaded"
          detail={messageFrom(status.error)}
          action={
            <Button variant="outline" size="sm" onClick={() => void status.refetch()}>
              Retry
            </Button>
          }
        />
      ) : (
        <>
          <ServiceEvidence bootstrap={bootstrap} status={status.data} />

          <section className="mt-6" aria-labelledby="mcp-clients-heading">
            <div>
              <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
                Client connections
              </p>
              <h2 id="mcp-clients-heading" className="mt-2 text-xl font-semibold">
                Claude Code and Codex
              </h2>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
                Each client receives an independent registration, credential, session identity,
                and receipt while sharing this workspace's single application service.
              </p>
            </div>

            <div className="mt-4 grid gap-4 xl:grid-cols-2">
              {status.data.clients.map((client) => (
                <ClientCard
                  key={client.client}
                  client={client}
                  disabled={control.isPending || !status.data.serviceReady || !status.data.sharedEndpointReady}
                  onAction={(action) => requestAction(client.client, action)}
                />
              ))}
            </div>
          </section>

          <section className="mt-6 rounded-xl border border-border bg-card/35 p-5">
            <div className="flex items-start gap-3">
              <ShieldCheck className="mt-0.5 size-5 text-primary" aria-hidden="true" />
              <div>
                <h2 className="text-sm font-semibold">Governed access</h2>
                <p className="mt-1 max-w-4xl text-xs leading-relaxed text-muted-foreground">
                  Every MCP request remains bound to typed product operations, an independent
                  client identity, bounded results, central risk evaluation, and durable local
                  audit evidence. Conversation contents stay separate between clients.
                </p>
              </div>
            </div>
          </section>
        </>
      )}

      <Dialog
        open={pending !== null}
        onOpenChange={(open) => {
          if (!open && !control.isPending) setPending(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {pending ? `${actionLabel(pending.action)} ${clientName(pending.client)}?` : "Confirm MCP action"}
            </DialogTitle>
            <DialogDescription>
              {pending ? actionDescription(pending.action, clientName(pending.client)) : null}
            </DialogDescription>
          </DialogHeader>
          {control.isError ? (
            <Alert variant="destructive">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>The action was not completed</AlertTitle>
              <AlertDescription>{messageFrom(control.error)}</AlertDescription>
            </Alert>
          ) : null}
          <DialogFooter>
            <Button variant="outline" disabled={control.isPending} onClick={() => setPending(null)}>
              Cancel
            </Button>
            <Button
              variant={
                pending?.action === "disconnect" || pending?.action === "revokeCredential"
                  ? "destructive"
                  : "default"
              }
              disabled={!pending || control.isPending}
              onClick={() => pending && control.mutate(pending)}
            >
              {control.isPending ? "Working…" : pending ? actionLabel(pending.action) : "Confirm"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </McpFrame>
  )
}

function McpFrame({
  children,
  action,
}: {
  children: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div className="mx-auto w-full max-w-[1320px] p-5 lg:p-7">
      <header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            One shared service · independent client sessions
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">MCP</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Connect supported AI clients, verify the real bounded protocol path, and repair only
            Market Squawk-owned registrations without closing the desktop.
          </p>
        </div>
        {action}
      </header>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function Unavailable({
  title,
  detail,
  action,
}: {
  title: string
  detail: string
  action?: React.ReactNode
}) {
  return (
    <Alert>
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        {detail}
        {action ? <div className="mt-3">{action}</div> : null}
      </AlertDescription>
    </Alert>
  )
}

function McpLoading() {
  return (
    <McpFrame>
      <StatusLoading />
    </McpFrame>
  )
}

function StatusLoading() {
  return (
    <div className="grid gap-4 sm:grid-cols-2">
      <Skeleton className="h-48 rounded-xl" />
      <Skeleton className="h-48 rounded-xl" />
    </div>
  )
}

function clientName(client: "claude_code" | "codex") {
  return client === "claude_code" ? "Claude Code" : "Codex"
}

function actionPastTense(action: McpClientAction) {
  switch (action) {
    case "connect":
      return "connected"
    case "reconnect":
      return "reconnected"
    case "verify":
      return "verified"
    case "repair":
      return "repaired"
    case "rotateCredential":
      return "rotated its credential"
    case "revokeCredential":
      return "had its access revoked"
    case "disconnect":
      return "disconnected"
  }
}
