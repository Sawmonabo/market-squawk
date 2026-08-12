import { CircleAlert } from "lucide-react"
import type { ReactNode } from "react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"

import { LookupSurface } from "./lookup-surface"

export function LookupPage() {
  const product = useProduct()

  if (product.status === "loading") {
    return (
      <LookupFrame>
        <LookupLoading />
      </LookupFrame>
    )
  }

  if (product.status === "error") {
    return (
      <LookupFrame>
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Lookup workspace unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </LookupFrame>
    )
  }

  const lookupAvailable = product.bootstrap.operations.some(
    (operation) => operation.name === "Analysis.Lookup",
  )

  return (
    <LookupFrame>
      <header className="border-b border-border pb-6">
        <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
          Local workspace index
        </p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Search</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Find instruments, local sources, research datasets, screens, jobs, and safe application
          actions. Results come from bounded local indexes in the installed service.
        </p>
      </header>

      {lookupAvailable ? (
        <section className="rounded-xl border border-border bg-card/35 p-5" aria-label="Workspace lookup">
          <LookupSurface
            transport={product.transport}
            scope={product.bootstrap.runtime}
            autoFocus
          />
        </section>
      ) : (
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Search is unavailable</AlertTitle>
          <AlertDescription>
            The installed service does not advertise the bounded Analysis.Lookup operation. Restore
            or update the local service before relying on workspace search.
          </AlertDescription>
        </Alert>
      )}
    </LookupFrame>
  )
}

function LookupFrame({ children }: { children: ReactNode }) {
  return <main className="mx-auto w-full max-w-[1120px] space-y-5 p-5 lg:p-7">{children}</main>
}

function LookupLoading() {
  return (
    <div className="space-y-5" aria-label="Loading lookup workspace" aria-live="polite">
      <div className="space-y-3 border-b border-border pb-6">
        <Skeleton className="h-3 w-32" />
        <Skeleton className="h-9 w-36" />
        <Skeleton className="h-5 max-w-2xl" />
      </div>
      <section className="space-y-4 rounded-xl border border-border bg-card/35 p-5">
        <Skeleton className="h-11 w-full" />
        <div className="flex flex-wrap gap-2">
          <Skeleton className="h-7 w-20 rounded-full" />
          <Skeleton className="h-7 w-28 rounded-full" />
          <Skeleton className="h-7 w-24 rounded-full" />
        </div>
        <Skeleton className="h-36 w-full rounded-xl" />
      </section>
    </div>
  )
}
