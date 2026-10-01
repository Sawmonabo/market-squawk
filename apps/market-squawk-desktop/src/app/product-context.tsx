import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"

import {
  projectDesktopBootstrap,
  type DesktopBootstrap,
  type DesktopServiceBootstrap,
  type DesktopSystemBootstrap,
  type DesktopSystemStartup,
} from "@/lib/schemas"
import type {
  DesktopEventSubscription,
  DesktopTransport,
  ProductTransport,
  SystemTransport,
} from "@/lib/transport"

import { createDomainRefresh } from "./domain-refresh"
import {
  affectedDomains,
  rejectsProductEvent,
  sameProductSession,
} from "./product-events"
import { productKeys } from "./query-client"

export type EventConnectionState =
  | { status: "inactive" }
  | { status: "connecting" }
  | { status: "connected"; resumed: boolean }
  | { status: "unavailable" }

type ProductState =
  | {
      status: "loading"
      availability: "loading"
      bootstrap: null
      error: null
    }
  | {
      status: "ready"
      availability: "ready"
      bootstrap: DesktopBootstrap
      error: null
    }
  | {
      status: "error"
      availability: "unavailable"
      bootstrap: null
      error: string
    }

type ProductContextValue = ProductState & {
  transport: ProductTransport
  refresh: () => void
}

type SystemState =
  | {
      status: "loading"
      bootstrap: null
      serviceBootstrap: null
      error: null
    }
  | {
      status: "ready"
      bootstrap: DesktopSystemBootstrap
      serviceBootstrap: null
      error: null
    }
  | {
      status: "recovery_required"
      bootstrap: null
      serviceBootstrap: DesktopServiceBootstrap
      error: null
    }
  | {
      status: "unavailable"
      bootstrap: null
      serviceBootstrap: null
      error: string
    }

type SystemContextValue = SystemState & {
  transport: SystemTransport
  eventConnection: EventConnectionState
  refresh: () => void
  recoverService: (unlock?: string) => Promise<void>
  recoveryPending: boolean
  recoveryError: string | null
}

const ProductContext = React.createContext<ProductContextValue | null>(null)
const SystemContext = React.createContext<SystemContextValue | null>(null)

export function ProductProvider({
  transport,
  children,
}: {
  transport: DesktopTransport
  children: React.ReactNode
}) {
  const queryClient = useQueryClient()
  const recoveryInFlight = React.useRef<Promise<void> | null>(null)
  const serviceReconnectInFlight = React.useRef<{
    scope: DesktopBootstrap["productSessionToken"]
    attempt: Promise<DesktopSystemStartup>
  } | null>(null)
  const [recoveryPending, setRecoveryPending] = React.useState(false)
  const [recoveryError, setRecoveryError] = React.useState<string | null>(null)
  const [eventConnection, setEventConnection] =
    React.useState<EventConnectionState>({ status: "inactive" })
  const [eventAdmittedProductSession, setEventAdmittedProductSession] =
    React.useState<DesktopBootstrap["productSessionToken"] | null>(null)
  const [explicitRefreshGeneration, setExplicitRefreshGeneration] =
    React.useState(0)
  const eventCursor = React.useRef<{
    productSessionToken: DesktopBootstrap["productSessionToken"]
    sequence: string
  } | null>(null)
  const bootstrap = useQuery({
    queryKey: productKeys.bootstrap,
    queryFn: () => transport.system.bootstrap(),
    staleTime: Number.POSITIVE_INFINITY,
    gcTime: Number.POSITIVE_INFINITY,
    retry: false,
    refetchOnReconnect: false,
    refetchOnWindowFocus: false,
  })
  const reconnectService = React.useCallback(
    (scope: DesktopBootstrap["productSessionToken"]) => {
      const current = serviceReconnectInFlight.current
      if (current && sameProductSession(scope, current.scope)) {
        return current.attempt
      }
      const attempt: Promise<DesktopSystemStartup> = transport.system.reconnect(scope).finally(() => {
        if (serviceReconnectInFlight.current?.attempt === attempt) {
          serviceReconnectInFlight.current = null
        }
      })
      serviceReconnectInFlight.current = { scope, attempt }
      return attempt
    },
    [transport.system],
  )

  React.useEffect(() => {
    const startup = bootstrap.data
    if (!startup || "status" in startup) {
      setEventConnection({ status: "inactive" })
      setEventAdmittedProductSession(null)
      return
    }

    const scope = startup.productSessionToken
    let domainRefresh = createDomainRefresh(queryClient, scope)
    let active = true
    let failed = false
    let subscription: DesktopEventSubscription | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let reconnectDelay = 1_000
    let serviceReconnectRequired = false
    let previousSequence =
      eventCursor.current &&
      sameProductSession(scope, eventCursor.current.productSessionToken)
        ? eventCursor.current.sequence
        : "0"
    const release = () => {
      const current = subscription
      subscription = null
      return current ? current.unsubscribe() : Promise.resolve()
    }
    const unavailable = (retainAdmittedSession = false) => {
      if (!active) return
      failed = true
      domainRefresh.dispose()
      if (reconnectTimer !== null) clearTimeout(reconnectTimer)
      reconnectTimer = null
      if (!retainAdmittedSession) setEventAdmittedProductSession(null)
      setEventConnection({ status: "unavailable" })
      void release().catch(() => undefined)
    }
    const connecting = () => {
      // Transport recovery does not invalidate the workspace that was already admitted.
      // A replacement scope or rejected event/receipt still requires fresh admission.
      setEventConnection({ status: "connecting" })
    }
    const scheduleReconnect = () => {
      if (!active || failed) return
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        void connect()
      }, reconnectDelay)
      reconnectDelay = Math.min(reconnectDelay * 2, 30_000)
    }
    const reconnect = async () => {
      try {
        await release()
        scheduleReconnect()
      } catch {
        unavailable(true)
      }
    }
    const connect = async () => {
      const requestedSequence = previousSequence
      const recovering = serviceReconnectRequired || explicitRefreshGeneration > 0
      let disconnected = false
      try {
        if (serviceReconnectRequired) {
          const restored = await reconnectService(scope)
          if (!active || failed) return
          serviceReconnectRequired = false
          if (
            "status" in restored ||
            !sameProductSession(scope, restored.productSessionToken)
          ) {
            queryClient.setQueryData(productKeys.bootstrap, restored)
            return
          }
        }
        if (recovering) {
          domainRefresh.dispose()
          domainRefresh = createDomainRefresh(queryClient, scope)
        }
        const connected = await transport.system.subscribe(
          { productSessionToken: scope, afterSequence: requestedSequence },
          (event) => {
            if (!active || failed || disconnected) return
            if (
              event.body.type === "stream_disconnected" &&
              sameProductSession(scope, event.productSessionToken) &&
              event.sequence === previousSequence
            ) {
              disconnected = true
              domainRefresh.dispose()
              serviceReconnectRequired = true
              connecting()
              // Preserve last successful query results when in-flight reads lose transport.
              void queryClient.cancelQueries({ queryKey: productKeys.root(scope) })
              if (subscription) void reconnect()
              return
            }
            if (rejectsProductEvent(scope, previousSequence, event)) {
              unavailable()
              return
            }
            reconnectDelay = 1_000
            previousSequence = event.sequence
            eventCursor.current = {
              productSessionToken: scope,
              sequence: previousSequence,
            }
            domainRefresh.invalidate(affectedDomains(event))
          },
          () => unavailable(),
        )
        if (!active || failed) {
          void connected.unsubscribe().catch(() => undefined)
          return
        }
        const { receipt } = connected
        if (
          !sameProductSession(scope, receipt.productSessionToken) ||
          receipt.sequence !== requestedSequence ||
          (requestedSequence !== "0" && !receipt.resumed)
        ) {
          subscription = connected
          unavailable()
          return
        }
        subscription = connected
        if (disconnected) {
          await reconnect()
          return
        }
        setEventAdmittedProductSession(receipt.productSessionToken)
        setEventConnection({
          status: "connected",
          resumed: receipt.resumed,
        })
        if (recovering) {
          domainRefresh.invalidate()
        }
      } catch {
        unavailable(true)
      }
    }

    connecting()
    void connect()

    return () => {
      active = false
      domainRefresh.dispose()
      if (reconnectTimer !== null) clearTimeout(reconnectTimer)
      void release().catch(() => undefined)
    }
  }, [
    bootstrap.data,
    explicitRefreshGeneration,
    queryClient,
    reconnectService,
    transport.system,
  ])

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
          await transport.system.bootstrapService(
            startup.requirement === "encrypted_fallback_locked"
              ? {
                  action: "unlock_encrypted_fallback",
                  unlock: unlock ?? "",
                }
              : { action: "complete_foreground_keyring" },
          )
          await bootstrap.refetch()
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
    [bootstrap, transport.system],
  )

  const refresh = React.useCallback(() => {
    const startup = bootstrap.data
    if (
      startup &&
      !("status" in startup) &&
      (eventConnection.status === "unavailable" ||
        eventConnection.status === "connecting")
    ) {
      setEventConnection({ status: "connecting" })
      void reconnectService(startup.productSessionToken)
        .then((restored) => {
          const current = queryClient.getQueryData<DesktopSystemStartup>(
            productKeys.bootstrap,
          )
          if (
            !current ||
            "status" in current ||
            !sameProductSession(startup.productSessionToken, current.productSessionToken)
          ) return
          queryClient.setQueryData(productKeys.bootstrap, restored)
          setExplicitRefreshGeneration((generation) => generation + 1)
        })
        .catch(() => {
          setEventConnection({ status: "unavailable" })
        })
      return
    }
    setExplicitRefreshGeneration((generation) => generation + 1)
    void bootstrap.refetch()
  }, [bootstrap, eventConnection.status, queryClient, reconnectService])

  const readySystemBootstrap =
    bootstrap.data && !("status" in bootstrap.data) ? bootstrap.data : null
  const generationHandoffPending =
    readySystemBootstrap !== null &&
    (eventAdmittedProductSession === null ||
      !sameProductSession(
        readySystemBootstrap.productSessionToken,
        eventAdmittedProductSession,
      ))

  let productState: ProductState
  if (
    readySystemBootstrap !== null &&
    generationHandoffPending &&
    eventConnection.status === "unavailable"
  ) {
    productState = {
      status: "error",
      availability: "unavailable",
      bootstrap: null,
      error: "Market Squawk could not finish opening this workspace. Try again.",
    }
  } else if (readySystemBootstrap !== null && generationHandoffPending) {
    productState = {
      status: "loading",
      availability: "loading",
      bootstrap: null,
      error: null,
    }
  } else if (readySystemBootstrap !== null) {
    productState = {
      status: "ready",
      availability: "ready",
      bootstrap: projectDesktopBootstrap(readySystemBootstrap),
      error: null,
    }
  } else if (bootstrap.data || bootstrap.isError) {
    productState = {
      status: "error",
      availability: "unavailable",
      bootstrap: null,
      error: "Market Squawk could not open this workspace. Try again.",
    }
  } else {
    productState = {
      status: "loading",
      availability: "loading",
      bootstrap: null,
      error: null,
    }
  }

  let systemState: SystemState
  if (readySystemBootstrap !== null) {
    systemState = {
      status: "ready",
      bootstrap: readySystemBootstrap,
      serviceBootstrap: null,
      error: null,
    }
  } else if (bootstrap.data && "status" in bootstrap.data) {
    systemState = {
      status: "recovery_required",
      bootstrap: null,
      serviceBootstrap: bootstrap.data,
      error: null,
    }
  } else if (bootstrap.isError) {
    systemState = {
      status: "unavailable",
      bootstrap: null,
      serviceBootstrap: null,
      error: "The local system is unavailable.",
    }
  } else {
    systemState = {
      status: "loading",
      bootstrap: null,
      serviceBootstrap: null,
      error: null,
    }
  }

  const productValue = React.useMemo<ProductContextValue>(
    () => ({
      ...productState,
      transport: transport.product,
      refresh,
    }),
    [productState, refresh, transport.product],
  )
  const systemValue = React.useMemo<SystemContextValue>(
    () => ({
      ...systemState,
      transport: transport.system,
      eventConnection,
      refresh,
      recoverService,
      recoveryPending,
      recoveryError,
    }),
    [
      eventConnection,
      recoverService,
      recoveryError,
      recoveryPending,
      refresh,
      systemState,
      transport.system,
    ],
  )

  return (
    <SystemContext.Provider value={systemValue}>
      <ProductContext.Provider value={productValue}>
        {children}
      </ProductContext.Provider>
    </SystemContext.Provider>
  )
}

export function useProduct() {
  const context = React.useContext(ProductContext)
  if (!context) throw new Error("useProduct must be used inside ProductProvider")
  return context
}

export function useSystem() {
  const context = React.useContext(SystemContext)
  if (!context) throw new Error("useSystem must be used inside ProductProvider")
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
