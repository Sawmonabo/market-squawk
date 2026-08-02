import { CircleAlert, DatabaseZap, ShieldCheck } from "lucide-react"
import { useLocation } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { navigationAdmission, navigationForPath } from "@/lib/navigation"

export function DomainPage({
  title,
  domain,
  description,
}: {
  title: string
  domain?: string | readonly string[]
  description: string
}) {
  const product = useProduct()
  const location = useLocation()

  if (product.status !== "ready") {
    return (
      <PageFrame title={title} description={description}>
        <Unavailable message="Return to Overview and restore the local application connection." />
      </PageFrame>
    )
  }
  const admission = navigationAdmission(
    navigationForPath(location.pathname),
    product.bootstrap,
  )
  if (!admission.admitted) {
    return (
      <PageFrame title={title} description={description}>
        <Unavailable message={admission.reason ?? "This area is not ready."} />
      </PageFrame>
    )
  }

  const operations = domain
    ? product.bootstrap.operations.filter((operation) =>
        typeof domain === "string"
          ? operation.domain === domain
          : domain.includes(operation.domain),
      )
    : []
  const reads = operations.filter((operation) => operation.readOnly).length
  const protectedChanges = operations.length - reads

  return (
    <PageFrame title={title} description={description}>
      <div className="grid gap-4 lg:grid-cols-3">
        <CapabilityFact
          icon={DatabaseZap}
          label="Available views"
          value={reads}
          detail="Bounded reads admitted by the installed service."
        />
        <CapabilityFact
          icon={ShieldCheck}
          label="Protected actions"
          value={protectedChanges}
          detail="Changes remain behind confirmation and owning risk authority."
        />
        <CapabilityFact
          icon={DatabaseZap}
          label="Service generation"
          value={product.bootstrap.runtime.serviceGeneration}
          detail="Cached data is isolated to this exact running service."
        />
      </div>
      <details className="mt-5 rounded-xl border border-border bg-card/35 p-5">
        <summary className="cursor-pointer text-sm font-semibold">
          Technical capability details
        </summary>
        <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
          This diagnostic list describes the installed contract. Product pages
          use purpose-built views and controls; they do not expose a raw command
          or JSON editor.
        </p>
        <ul className="mt-4 grid gap-3 md:grid-cols-2">
          {operations.map((operation) => (
            <li
              key={operation.name}
              className="rounded-lg border border-border bg-background/50 p-3"
            >
              <p className="font-mono text-[11px] text-foreground/85">
                {operation.name}
              </p>
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                {operation.description}
              </p>
            </li>
          ))}
        </ul>
      </details>
    </PageFrame>
  )
}

function CapabilityFact({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof DatabaseZap
  label: string
  value: number
  detail: string
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <Icon className="size-5 text-primary" aria-hidden="true" />
      <p className="mt-4 text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
      <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
        {detail}
      </p>
    </section>
  )
}

function Unavailable({ message }: { message: string }) {
  return (
    <Alert>
      <CircleAlert aria-hidden="true" />
      <AlertTitle>Local state is unavailable</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
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
