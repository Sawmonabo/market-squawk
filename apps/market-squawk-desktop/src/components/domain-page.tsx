import * as React from "react"
import { Braces, CircleAlert, DatabaseZap } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

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
  const [result, setResult] = React.useState<unknown>(null)
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
            <pre className="mt-4 max-h-[420px] overflow-auto rounded-lg border border-border bg-background p-4 font-mono text-[10px] leading-relaxed text-foreground/75">
              {JSON.stringify(result, null, 2)}
            </pre>
          ) : (
            <div className="mt-6 flex min-h-44 items-center justify-center rounded-lg border border-dashed border-border text-center">
              <div className="max-w-sm px-6">
                <Braces
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
