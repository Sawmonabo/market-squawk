import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"

import type {
  DesktopBootstrap,
  DesktopServiceBootstrap,
  DesktopStartup,
} from "@/lib/schemas"
import type { DesktopEventSubscription, ProductTransport } from "@/lib/transport"

import {
  affectedDomain,
  isRetryableDisconnect,
  requiresResync,
  sameRuntime,
} from "./product-events"
import { productKeys } from "./query-client"

const RECONNECT_DELAYS_MS = [250, 500, 1_000, 2_000, 4_000] as const
const STABLE_CONNECTION_MS = 5_000

export type EventConnectionState =
  | { status: "inactive" }
  | { status: "connecting" }
  | { status: "connected"; resumed: boolean }
  | { status: "reconnecting"; attempt: number; maximumAttempts: number }
  | { status: "resynchronizing" }
  | { status: "unavailable"; attempts: number }

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
  eventConnection: EventConnectionState
  retryEventConnection: () => void
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
  const nativeReconnectInFlight = React.useRef<Promise<void> | null>(null)
  const [recoveryPending, setRecoveryPending] = React.useState(false)
  const [recoveryError, setRecoveryError] = React.useState<string | null>(null)
  const [eventConnection, setEventConnection] =
    React.useState<EventConnectionState>({ status: "inactive" })
  const [eventAdmissionPending, setEventAdmissionPending] = React.useState(true)
  const [eventAdmittedRuntime, setEventAdmittedRuntime] =
    React.useState<DesktopBootstrap["runtime"] | null>(null)
  const [eventRetryGeneration, setEventRetryGeneration] = React.useState(0)
  const eventCursor = React.useRef<{
    runtime: DesktopBootstrap["runtime"]
    sequence: string
  } | null>(null)
  const bootstrap = useQuery({
    queryKey: productKeys.bootstrap,
    queryFn: () => transport.bootstrap(),
    staleTime: 5_000,
    gcTime: 60_000,
  })

  React.useEffect(() => {
    if (!bootstrap.data || "status" in bootstrap.data) {
      setEventConnection({ status: "inactive" })
      setEventAdmissionPending(true)
      return
    }
    const scope = bootstrap.data.runtime
    let active = true
    let resyncing = false
    let previousSequence =
      eventCursor.current && sameRuntime(scope, eventCursor.current.runtime)
        ? eventCursor.current.sequence
        : "0"
    let connectionEpoch = 0
    let reconnectAttempts = 0
    let subscription: DesktopEventSubscription | null = null
    let reconnectTimer: number | null = null
    let stableTimer: number | null = null

    const clearTimers = () => {
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer)
      if (stableTimer !== null) window.clearTimeout(stableTimer)
      reconnectTimer = null
      stableTimer = null
    }

    const releaseSubscription = () => {
      const current = subscription
      subscription = null
      if (current) void current.unsubscribe().catch(() => undefined)
    }

    const reconnectNativeService = () => {
      if (nativeReconnectInFlight.current) return
      resyncing = true
      setEventAdmissionPending(true)
      connectionEpoch += 1
      clearTimers()
      releaseSubscription()
      eventCursor.current = null
      if (active) setEventConnection({ status: "resynchronizing" })
      const attempt = (async () => {
        try {
          const startup = await transport.reconnectService(scope)
          if (!active) return
          await queryClient.cancelQueries({ queryKey: productKeys.root(scope) })
          if (!active) return
          queryClient.removeQueries({ queryKey: productKeys.root(scope) })
          const current = queryClient.getQueryData<DesktopStartup>(
            productKeys.bootstrap,
          )
          if (
            current &&
            !("status" in current) &&
            !sameRuntime(current.runtime, scope)
          ) {
            return
          }
          queryClient.setQueryData(productKeys.bootstrap, startup)
        } catch {
          if (active) {
            setEventConnection({
              status: "unavailable",
              attempts: reconnectAttempts,
            })
          }
        } finally {
          nativeReconnectInFlight.current = null
        }
      })()
      nativeReconnectInFlight.current = attempt
    }

    const resync = () => {
      if (!active || resyncing) return
      resyncing = true
      setEventAdmissionPending(true)
      connectionEpoch += 1
      clearTimers()
      releaseSubscription()
      eventCursor.current = null
      setEventConnection({ status: "resynchronizing" })
      void queryClient.cancelQueries({ queryKey: productKeys.root(scope) })
      queryClient.removeQueries({ queryKey: productKeys.root(scope) })
      void bootstrap.refetch().then((result) => {
        if (!active) return
        const startup = result.data
        if (result.isError || !startup) {
          reconnectNativeService()
          return
        }
        if ("status" in startup) {
          setEventConnection({ status: "inactive" })
          return
        }
        if (!sameRuntime(scope, startup.runtime)) {
          return
        }
        previousSequence = "0"
        reconnectAttempts = 0
        resyncing = false
        startConnection()
      })
    }

    const scheduleReconnect = () => {
      if (!active) return
      connectionEpoch += 1
      releaseSubscription()
      if (stableTimer !== null) window.clearTimeout(stableTimer)
      stableTimer = null
      if (reconnectAttempts >= RECONNECT_DELAYS_MS.length) {
        reconnectNativeService()
        return
      }
      const delay = RECONNECT_DELAYS_MS[reconnectAttempts]
      reconnectAttempts += 1
      setEventConnection({
        status: "reconnecting",
        attempt: reconnectAttempts,
        maximumAttempts: RECONNECT_DELAYS_MS.length,
      })
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = null
        startConnection()
      }, delay)
    }

    const startConnection = () => {
      if (!active) return
      const epoch = connectionEpoch + 1
      connectionEpoch = epoch
      const requestedSequence = previousSequence
      if (reconnectAttempts === 0) {
        setEventConnection({ status: "connecting" })
      }
      transport
        .subscribe(
          { runtime: scope, afterSequence: requestedSequence },
          (event) => {
            if (!active || epoch !== connectionEpoch) return
            if (requiresResync(scope, previousSequence, event)) {
              resync()
              return
            }
            if (isRetryableDisconnect(event)) {
              scheduleReconnect()
              return
            }
            previousSequence = event.sequence
            eventCursor.current = { runtime: scope, sequence: previousSequence }
            const domain = affectedDomain(event)
            void queryClient.invalidateQueries({
              queryKey: domain
                ? productKeys.domain(scope, domain)
                : productKeys.root(scope),
            })
          },
          () => {
            if (active && epoch === connectionEpoch) resync()
          },
        )
        .then((connected) => {
          if (!active || epoch !== connectionEpoch) {
            void connected.unsubscribe().catch(() => undefined)
            return
          }
          const { receipt } = connected
          if (
            !sameRuntime(scope, receipt.runtime) ||
            receipt.sequence !== requestedSequence ||
            (requestedSequence !== "0" && !receipt.resumed)
          ) {
            subscription = connected
            resync()
            return
          }
          subscription = connected
          setEventAdmittedRuntime(receipt.runtime)
          setEventAdmissionPending(false)
          setEventConnection({
            status: "connected",
            resumed: receipt.resumed,
          })
          stableTimer = window.setTimeout(() => {
            stableTimer = null
            reconnectAttempts = 0
          }, STABLE_CONNECTION_MS)
        })
        .catch(() => {
          if (active && epoch === connectionEpoch) scheduleReconnect()
        })
    }

    startConnection()

    return () => {
      active = false
      connectionEpoch += 1
      clearTimers()
      releaseSubscription()
    }
  }, [bootstrap.data, eventRetryGeneration, queryClient, transport])

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

  const readyBootstrap =
    bootstrap.data && !("status" in bootstrap.data) ? bootstrap.data : null
  const generationHandoffPending =
    readyBootstrap !== null &&
    (eventAdmissionPending ||
      eventAdmittedRuntime === null ||
      !sameRuntime(readyBootstrap.runtime, eventAdmittedRuntime))
  const generationHandoffUnavailable =
    generationHandoffPending && eventConnection.status === "unavailable"
  let state: ProductState
  if (generationHandoffUnavailable) {
    state = {
      status: "error",
      availability: "unavailable",
      bootstrap: null,
      serviceBootstrap: null,
      error:
        "The Desktop could not establish an authenticated event connection for the current service generation.",
    }
  } else if (generationHandoffPending) {
    state = {
      status: "loading",
      availability: "loading",
      bootstrap: null,
      serviceBootstrap: null,
      error: null,
    }
  } else if (bootstrap.data && "status" in bootstrap.data) {
    state = {
      status: "error",
      availability: "degraded",
      bootstrap: null,
      serviceBootstrap: bootstrap.data,
      error:
        "Secure local storage needs the foreground recovery action shown above. Navigation and stored workspace routes remain available.",
    }
  } else if (bootstrap.data) {
    state = {
      status: "ready",
      availability: "ready",
      bootstrap: bootstrap.data,
      serviceBootstrap: null,
      error: null,
    }
  } else if (bootstrap.isError) {
    state = {
      status: "error",
      availability: "unavailable",
      bootstrap: null,
      serviceBootstrap: null,
      error: messageFrom(bootstrap.error),
    }
  } else {
    state = {
      status: "loading",
      availability: "loading",
      bootstrap: null,
      serviceBootstrap: null,
      error: null,
    }
  }

  const value = React.useMemo<ProductContextValue>(
    () => ({
      ...state,
      transport,
      eventConnection,
      retryEventConnection: () => {
        setEventAdmissionPending(true)
        setEventRetryGeneration((generation) => generation + 1)
      },
      recoverService,
      recoveryPending,
      recoveryError,
      refresh: () => {
        void bootstrap.refetch()
      },
    }),
    [
      bootstrap,
      eventConnection,
      recoverService,
      recoveryError,
      recoveryPending,
      state,
      transport,
    ],
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
