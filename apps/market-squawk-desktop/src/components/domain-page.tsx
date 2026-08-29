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
        <Unavailable message="Return to Home and restore the local application connection." />
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
          detail="Information available in this workspace."
        />
        <CapabilityFact
          icon={ShieldCheck}
          label="Protected actions"
          value={protectedChanges}
          detail="Changes require review and confirmation."
        />
        <CapabilityFact
          icon={DatabaseZap}
          label="Available tools"
          value={operations.length}
          detail="Features available in this area."
        />
      </div>
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
