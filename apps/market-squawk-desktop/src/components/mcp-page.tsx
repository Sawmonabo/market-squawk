import type { ReactNode } from "react"
import { CircleAlert, FileTerminal, MonitorOff } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

export function McpPage() {
  const product = useProduct()

  if (product.status !== "ready") {
    return (
      <PageFrame>
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Local MCP state is unavailable</AlertTitle>
          <AlertDescription>
            Return to Overview and restore the local application connection.
          </AlertDescription>
        </Alert>
      </PageFrame>
    )
  }

  const { mcp, mcpClient } = product.bootstrap
  return (
    <PageFrame>
      <Alert>
        <FileTerminal aria-hidden="true" />
        <AlertTitle>{mcp.label}</AlertTitle>
        <AlertDescription>{mcp.detail}</AlertDescription>
      </Alert>

      {mcpClient ? (
        <section className="mt-5 rounded-xl border border-border bg-card/45 p-5">
          <div className="flex items-start gap-3">
            <MonitorOff
              className="mt-0.5 size-5 text-amber-400"
              aria-hidden="true"
            />
            <div>
              <h2 className="text-sm font-semibold">
                Desktop exit required
              </h2>
              <p className="mt-1 max-w-2xl text-xs leading-relaxed text-muted-foreground">
                This installed client instruction starts the same bounded local
                stdio MCP service. Close the desktop first so the service can
                acquire the local application lock.
              </p>
            </div>
          </div>
          <dl className="mt-5 space-y-4 border-t border-border pt-5">
            <Instruction label="Program" value={mcpClient.program} />
            <Instruction
              label="Arguments"
              value={
                mcpClient.arguments.length
                  ? mcpClient.arguments.join("\n")
                  : "No arguments"
              }
            />
            <Instruction
              label="Environment"
              value={
                Object.keys(mcpClient.environment).length
                  ? Object.entries(mcpClient.environment)
                      .map(([key, value]) => `${key}=${value}`)
                      .join("\n")
                  : "No additional environment variables"
              }
            />
          </dl>
        </section>
      ) : (
        <Alert className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>MCP client setup is unavailable</AlertTitle>
          <AlertDescription>
            The installed CLI path, effective local workspace paths, or bounded
            MCP contract could not be verified. Repair the complete native
            package and refresh status.
          </AlertDescription>
        </Alert>
      )}

      <section className="mt-5 rounded-xl border border-border bg-card/35 p-5">
        <div className="flex flex-wrap items-baseline justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold">Bounded tool surface</h2>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              These operation contracts come from the same Rust application
              registry served through local stdio MCP.
            </p>
          </div>
          <span className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {product.bootstrap.operations.length} operations
          </span>
        </div>
        {product.bootstrap.operations.length ? (
          <ul className="mt-5 divide-y divide-border border-y border-border">
            {product.bootstrap.operations.map((operation) => (
              <li
                key={operation.name}
                className="grid gap-2 py-3 lg:grid-cols-[220px_1fr_120px]"
              >
                <code className="break-all font-mono text-[11px] text-foreground/90">
                  {operation.name}
                </code>
                <p className="text-[11px] leading-relaxed text-muted-foreground">
                  {operation.description}
                </p>
                <span className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground lg:text-right">
                  {operation.authorization.replaceAll("_", " ")}
                </span>
              </li>
            ))}
          </ul>
        ) : (
          <Alert className="mt-5">
            <CircleAlert aria-hidden="true" />
            <AlertTitle>MCP tool contract is unavailable</AlertTitle>
            <AlertDescription>
              The application returned no typed operation descriptors. Repair
              the complete native package before configuring an MCP client.
            </AlertDescription>
          </Alert>
        )}
      </section>
    </PageFrame>
  )
}

function Instruction({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div className="grid gap-2 sm:grid-cols-[120px_1fr]">
      <dt className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd>
        <code className="block whitespace-pre-wrap break-all rounded-md border border-border bg-background px-3 py-2 font-mono text-xs text-foreground/85">
          {value}
        </code>
      </dd>
    </div>
  )
}

function PageFrame({ children }: { children: ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1120px] p-5 lg:p-7">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
        Market Squawk
      </p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">MCP</h1>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
        Inspect the verified local stdio client instruction without starting a
        second service or granting the WebView shell access.
      </p>
      <div className="mt-6">{children}</div>
    </div>
  )
}
