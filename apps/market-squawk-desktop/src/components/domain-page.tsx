import { CircleAlert, DatabaseZap, ShieldCheck } from "lucide-react"
import { useLocation } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { navigationAdmission, navigationForPath } from "@/lib/navigation"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { ProductCapability } from "@/lib/schemas"

export function DomainPage({
  title,
  capabilities,
  description,
}: {
  title: string
  capabilities?: readonly ProductCapability[]
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

  const availableCapabilities = productCapabilitySet(product.bootstrap)
  const availableFeatures = capabilities?.filter((capability) =>
    availableCapabilities.has(capability),
  ).length ?? 0
  const unavailableFeatures = (capabilities?.length ?? 0) - availableFeatures

  return (
    <PageFrame title={title} description={description}>
      <div className="grid gap-4 lg:grid-cols-3">
        <CapabilityFact
          icon={DatabaseZap}
          label="Available features"
          value={availableFeatures}
          detail="Features ready to use in this workspace."
        />
        <CapabilityFact
          icon={ShieldCheck}
          label="Unavailable features"
          value={unavailableFeatures}
          detail="Features that still need setup or a product update."
        />
        <CapabilityFact
          icon={DatabaseZap}
          label="Area features"
          value={capabilities?.length ?? 0}
          detail="Features included in this area."
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
