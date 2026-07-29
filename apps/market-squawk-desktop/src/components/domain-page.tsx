import * as React from "react"
import { CircleAlert, DatabaseZap, ListTree } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import type { ApplicationResult } from "@/lib/schemas"

export function DomainPage({
  title,
  domain,
  description,
}: {
  title: string
  domain?: string
  description: string
}) {
  const product = useProduct()
  const [result, setResult] = React.useState<ApplicationResult | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [loading, setLoading] = React.useState(false)

  if (product.status !== "ready") {
    return (
      <PageFrame title={title} description={description}>
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Local state is unavailable</AlertTitle>
          <AlertDescription>
            Return to Overview and restore the local application connection.
          </AlertDescription>
        </Alert>
      </PageFrame>
    )
  }

  const operations = domain
    ? product.bootstrap.operations.filter(
        (operation) => operation.domain === domain,
      )
    : []
  const automatic = operations.find((operation) => {
    const required = operation.inputSchema.required
    const userRequired = Array.isArray(required)
      ? required.filter((field) => field !== "resultLimits")
      : []
    return (
      operation.readOnly &&
      userRequired.length === 0
    )
  })

  const load = async () => {
    if (!automatic) {
      return
    }
    setLoading(true)
    setError(null)
    try {
      const response = await product.transport.invoke({
        operation: automatic.name,
      })
      setResult(response)
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "The local read could not be completed.",
      )
    } finally {
      setLoading(false)
    }
  }

  return (
    <PageFrame title={title} description={description}>
      <div className="grid gap-4 xl:grid-cols-[1fr_320px]">
        <section className="rounded-xl border border-border bg-card/45 p-5">
          <div className="flex items-start gap-3">
            <DatabaseZap
              className="mt-0.5 size-5 text-primary"
              aria-hidden="true"
            />
            <div>
              <h2 className="text-sm font-semibold">Current local state</h2>
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                Values appear only after their owning Rust service returns them.
              </p>
            </div>
            {automatic ? (
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="ml-auto"
                onClick={load}
                disabled={loading}
              >
                {loading ? "Reading…" : "Read current state"}
              </Button>
            ) : null}
          </div>
          {error ? (
            <p role="alert" className="mt-4 text-sm text-red-400">
              {error}
            </p>
          ) : null}
          {result ? (
            <div className="mt-4 max-h-[420px] overflow-auto rounded-lg border border-border bg-background p-4">
              <dl className="mb-4 grid gap-3 border-b border-border pb-4 sm:grid-cols-3">
                <ResultFact
                  label="Completeness"
                  value={result.metadata.completeness}
                />
                <ResultFact
                  label="Returned"
                  value={result.metadata.returnedItems}
                />
                <ResultFact
                  label="Available"
                  value={result.metadata.availableItems}
                />
              </dl>
              <ReadableValue value={result.data} />
            </div>
          ) : (
            <div className="mt-6 flex min-h-44 items-center justify-center rounded-lg border border-dashed border-border text-center">
              <div className="max-w-sm px-6">
                <ListTree
                  className="mx-auto size-5 text-muted-foreground"
                  aria-hidden="true"
                />
                <p className="mt-2 text-xs text-muted-foreground">
                  {automatic
                    ? "Read the bounded local service when you need current data."
                    : "Complete the required setup fields before this domain can be queried."}
                </p>
              </div>
            </div>
          )}
        </section>
        <section className="rounded-xl border border-border bg-card/35 p-5">
          <h2 className="text-sm font-semibold">Available operations</h2>
          <ul className="mt-3 space-y-3">
            {operations.length ? (
              operations.map((operation) => (
                <li key={operation.name} className="border-b border-border pb-3 last:border-0">
                  <p className="font-mono text-[10px] text-foreground/85">
                    {operation.name}
                  </p>
                  <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">
                    {operation.description}
                  </p>
                </li>
              ))
            ) : (
              <li className="text-xs text-muted-foreground">
                This route uses local operating controls rather than a business
                operation.
              </li>
            )}
          </ul>
        </section>
      </div>
    </PageFrame>
  )
}

function ResultFact({
  label,
  value,
}: {
  label: string
  value: string | number
}) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 font-mono text-xs text-foreground">{value}</dd>
    </div>
  )
}

function ReadableValue({
  value,
  depth = 0,
}: {
  value: unknown
  depth?: number
}): React.ReactNode {
  if (value === null || value === undefined) {
    return <span className="text-xs text-muted-foreground">Not available</span>
  }
  if (typeof value === "string" || typeof value === "number") {
    return (
      <span className="break-words font-mono text-xs text-foreground/85">
        {String(value)}
      </span>
    )
  }
  if (typeof value === "boolean") {
    return (
      <span className="text-xs text-foreground/85">{value ? "Yes" : "No"}</span>
    )
  }
  if (depth >= 4) {
    return (
      <span className="text-xs text-muted-foreground">
        Additional nested details are available through the CLI or MCP.
      </span>
    )
  }
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return <span className="text-xs text-muted-foreground">No items</span>
    }
    return (
      <ol className="space-y-2">
        {value.slice(0, 100).map((item, index) => (
          <li
            key={index}
            className="rounded-md border border-border/70 px-3 py-2"
          >
            <ReadableValue value={item} depth={depth + 1} />
          </li>
        ))}
        {value.length > 100 ? (
          <li className="text-xs text-muted-foreground">
            {value.length - 100} additional items are available through the CLI
            or MCP.
          </li>
        ) : null}
      </ol>
    )
  }
  if (typeof value === "object") {
    const entries = Object.entries(value)
    if (entries.length === 0) {
      return <span className="text-xs text-muted-foreground">No records</span>
    }
    return (
      <dl className="space-y-3">
        {entries.slice(0, 100).map(([key, item]) => (
          <div
            key={key}
            className="grid gap-1 border-b border-border/70 pb-3 last:border-0 last:pb-0 sm:grid-cols-[180px_1fr]"
          >
            <dt className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
              {humanize(key)}
            </dt>
            <dd>
              <ReadableValue value={item} depth={depth + 1} />
            </dd>
          </div>
        ))}
        {entries.length > 100 ? (
          <div className="text-xs text-muted-foreground">
            {entries.length - 100} additional fields are available through the
            CLI or MCP.
          </div>
        ) : null}
      </dl>
    )
  }
  return <span className="text-xs text-muted-foreground">Unavailable value</span>
}

function humanize(value: string): string {
  const words = value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim()
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : "Value"
}

function PageFrame({
  title,
  description,
  children,
}: {
  title: string
  description: string
  children: React.ReactNode
}) {
  return (
    <div className="mx-auto w-full max-w-[1120px] p-5 lg:p-7">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
        Market Squawk
      </p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">{title}</h1>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
        {description}
      </p>
      <div className="mt-6">{children}</div>
    </div>
  )
}
