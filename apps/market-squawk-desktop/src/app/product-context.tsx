import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"

import type { DesktopBootstrap, DesktopServiceBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { affectedDomain, requiresResync } from "./product-events"
import { productKeys } from "./query-client"

type ProductState =
  | {
      status: "loading"
      availability: "loading"
      bootstrap: null
      serviceBootstrap: null
      error: null
    }
  | {
      status: "ready"
      availability: "ready"
      bootstrap: DesktopBootstrap
      serviceBootstrap: null
      error: null
    }
  | {
      status: "error"
      availability: "degraded"
      bootstrap: null
      serviceBootstrap: DesktopServiceBootstrap
      error: string
    }
  | {
      status: "error"
      availability: "unavailable"
      bootstrap: null
      serviceBootstrap: null
      error: string
    }

type ProductActions = {
  transport: ProductTransport
  refresh: () => void
  recoverService: (unlock?: string) => Promise<void>
  recoveryPending: boolean
  recoveryError: string | null
}

type ProductContextValue = ProductState & ProductActions

const ProductContext = React.createContext<ProductContextValue | null>(null)

export function ProductProvider({
  transport,
  children,
}: {
  transport: ProductTransport
  children: React.ReactNode
}) {
  const queryClient = useQueryClient()
  const recoveryInFlight = React.useRef<Promise<void> | null>(null)
  const [recoveryPending, setRecoveryPending] = React.useState(false)
  const [recoveryError, setRecoveryError] = React.useState<string | null>(null)
  const bootstrap = useQuery({
    queryKey: productKeys.bootstrap,
    queryFn: () => transport.bootstrap(),
    staleTime: 5_000,
    gcTime: 60_000,
  })

  React.useEffect(() => {
    if (!bootstrap.data || "status" in bootstrap.data) return
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

  React.useEffect(() => {
    if (bootstrap.data && !("status" in bootstrap.data)) {
      setRecoveryError(null)
    }
  }, [bootstrap.data])

  const recoverService = React.useCallback(
    (unlock?: string): Promise<void> => {
      if (recoveryInFlight.current) return recoveryInFlight.current
      const startup = bootstrap.data
      if (!startup || !("status" in startup)) return Promise.resolve()
      if (startup.requirement === "encrypted_fallback_locked" && !unlock) {
        setRecoveryError("Enter the local security password before continuing.")
        return Promise.resolve()
      }

      setRecoveryPending(true)
      setRecoveryError(null)
      const attempt = (async () => {
        try {
          let actionError: unknown = null
          let actionFailed = false
          try {
            await transport.bootstrapService(
              startup.requirement === "encrypted_fallback_locked"
                ? {
                    action: "unlock_encrypted_fallback",
                    unlock: unlock ?? "",
                  }
                : { action: "retry_after_foreground_keyring" },
            )
          } catch (error) {
            actionError = error
            actionFailed = true
          }
          const refreshed = await bootstrap.refetch()
          if (actionFailed) {
            setRecoveryError(messageFrom(actionError))
          } else if (refreshed.isError) {
            setRecoveryError(messageFrom(refreshed.error))
          }
        } catch (error) {
          setRecoveryError(messageFrom(error))
        } finally {
          recoveryInFlight.current = null
          setRecoveryPending(false)
        }
      })()
      recoveryInFlight.current = attempt
      return attempt
    },
    [bootstrap, transport],
  )

  const state: ProductState = bootstrap.data
    ? "status" in bootstrap.data
      ? {
          status: "error",
          availability: "degraded",
          bootstrap: null,
          serviceBootstrap: bootstrap.data,
          error:
            "Secure local storage needs the foreground recovery action shown above. Navigation and stored workspace routes remain available.",
        }
      : {
          status: "ready",
          availability: "ready",
          bootstrap: bootstrap.data,
          serviceBootstrap: null,
          error: null,
        }
    : bootstrap.isError
      ? {
          status: "error",
          availability: "unavailable",
          bootstrap: null,
          serviceBootstrap: null,
          error: messageFrom(bootstrap.error),
        }
      : {
          status: "loading",
          availability: "loading",
          bootstrap: null,
          serviceBootstrap: null,
          error: null,
        }

  const value = React.useMemo<ProductContextValue>(
    () => ({
      ...state,
      transport,
      recoverService,
      recoveryPending,
      recoveryError,
      refresh: () => {
        void bootstrap.refetch()
      },
    }),
    [bootstrap, recoverService, recoveryError, recoveryPending, state, transport],
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
