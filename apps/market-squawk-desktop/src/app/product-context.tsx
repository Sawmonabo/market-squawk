import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"

import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { affectedDomain, requiresResync } from "./product-events"
import { productKeys } from "./query-client"

type ProductState =
  | { status: "loading"; bootstrap: null; error: null }
  | { status: "ready"; bootstrap: DesktopBootstrap; error: null }
  | { status: "error"; bootstrap: null; error: string }

type ProductContextValue = ProductState & {
  transport: ProductTransport
  refresh: () => void
}

const ProductContext = React.createContext<ProductContextValue | null>(null)

export function ProductProvider({
  transport,
  children,
}: {
  transport: ProductTransport
  children: React.ReactNode
}) {
  const queryClient = useQueryClient()
  const bootstrap = useQuery({
    queryKey: productKeys.bootstrap,
    queryFn: () => transport.bootstrap(),
    staleTime: 5_000,
    gcTime: 60_000,
  })

  React.useEffect(() => {
    if (!bootstrap.data) return
    const scope = bootstrap.data.runtime
    let active = true
    let previousSequence = 0
    let unsubscribe: (() => void) | undefined

    const resync = () => {
      if (!active) return
      void queryClient.cancelQueries({ queryKey: productKeys.root(scope) })
      queryClient.removeQueries({ queryKey: productKeys.root(scope) })
      void queryClient.invalidateQueries({ queryKey: productKeys.bootstrap })
    }

    transport
      .subscribe((event) => {
        if (!active) return
        if (requiresResync(scope, previousSequence, event)) {
          resync()
          return
        }
        previousSequence = event.sequence
        const domain = affectedDomain(event)
        void queryClient.invalidateQueries({
          queryKey: domain
            ? productKeys.domain(scope, domain)
            : productKeys.root(scope),
        })
      })
      .then((release) => {
        if (active) unsubscribe = release
        else release()
      })
      .catch(resync)

    return () => {
      active = false
      unsubscribe?.()
    }
  }, [bootstrap.data, queryClient, transport])

  const state: ProductState = bootstrap.data
    ? { status: "ready", bootstrap: bootstrap.data, error: null }
    : bootstrap.isError
      ? {
          status: "error",
          bootstrap: null,
          error: messageFrom(bootstrap.error),
        }
      : { status: "loading", bootstrap: null, error: null }

  const value = React.useMemo<ProductContextValue>(
    () => ({
      ...state,
      transport,
      refresh: () => {
        void bootstrap.refetch()
      },
    }),
    [bootstrap, state, transport],
  )

  return (
    <ProductContext.Provider value={value}>
      {children}
    </ProductContext.Provider>
  )
}

export function useProduct() {
  const context = React.useContext(ProductContext)
  if (!context) throw new Error("useProduct must be used inside ProductProvider")
  return context
}

export function messageFrom(error: unknown) {
  if (error instanceof Error) return error.message
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message
  }
  return "Market Squawk could not complete this local request."
}
