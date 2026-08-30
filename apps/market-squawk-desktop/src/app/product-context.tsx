import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"

import {
  projectDesktopBootstrap,
  type DesktopBootstrap,
  type DesktopServiceBootstrap,
  type DesktopSystemBootstrap,
} from "@/lib/schemas"
import type {
  DesktopEventSubscription,
  DesktopTransport,
  ProductTransport,
  SystemTransport,
} from "@/lib/transport"

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
  const [recoveryPending, setRecoveryPending] = React.useState(false)
  const [recoveryError, setRecoveryError] = React.useState<string | null>(null)
  const [eventConnection, setEventConnection] =
    React.useState<EventConnectionState>({ status: "inactive" })
  const [eventAdmissionPending, setEventAdmissionPending] = React.useState(true)
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

  React.useEffect(() => {
    const startup = bootstrap.data
    if (!startup || "status" in startup) {
      setEventConnection({ status: "inactive" })
      setEventAdmissionPending(true)
      setEventAdmittedProductSession(null)
      return
    }

    const scope = startup.productSessionToken
    let active = true
    let failed = false
    let subscription: DesktopEventSubscription | null = null
    let previousSequence =
      eventCursor.current &&
      sameProductSession(scope, eventCursor.current.productSessionToken)
        ? eventCursor.current.sequence
        : "0"
    const requestedSequence = previousSequence

    const release = () => {
      const current = subscription
      subscription = null
      if (current) void current.unsubscribe().catch(() => undefined)
    }
    const unavailable = () => {
      if (!active) return
      failed = true
      setEventAdmissionPending(false)
      setEventAdmittedProductSession(null)
      setEventConnection({ status: "unavailable" })
      release()
    }

    setEventAdmissionPending(true)
    setEventAdmittedProductSession(null)
    setEventConnection({ status: "connecting" })
    transport.system
      .subscribe(
        { productSessionToken: scope, afterSequence: requestedSequence },
        (event) => {
          if (!active) return
          if (rejectsProductEvent(scope, previousSequence, event)) {
            unavailable()
            return
          }
          previousSequence = event.sequence
          eventCursor.current = {
            productSessionToken: scope,
            sequence: previousSequence,
          }
          void Promise.all(
            affectedDomains(event).map((domain) =>
              queryClient.invalidateQueries({
                queryKey: productKeys.domain(scope, domain),
              }),
            ),
          )
        },
        unavailable,
      )
      .then((connected) => {
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
        setEventAdmittedProductSession(receipt.productSessionToken)
        setEventAdmissionPending(false)
        setEventConnection({
          status: "connected",
          resumed: receipt.resumed,
        })
      })
      .catch(unavailable)

    return () => {
      active = false
      release()
    }
  }, [
    bootstrap.data,
    explicitRefreshGeneration,
    queryClient,
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
              : { action: "retry_after_foreground_keyring" },
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
    setEventAdmissionPending(true)
    setExplicitRefreshGeneration((generation) => generation + 1)
    void bootstrap.refetch()
  }, [bootstrap])

  const readySystemBootstrap =
    bootstrap.data && !("status" in bootstrap.data) ? bootstrap.data : null
  const generationHandoffPending =
    readySystemBootstrap !== null &&
    (eventAdmissionPending ||
      eventAdmittedProductSession === null ||
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
