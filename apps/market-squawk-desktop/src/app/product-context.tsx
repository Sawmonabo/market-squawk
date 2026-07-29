import * as React from "react"

import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

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
  const [revision, setRevision] = React.useState(0)
  const [state, setState] = React.useState<ProductState>({
    status: "loading",
    bootstrap: null,
    error: null,
  })

  React.useEffect(() => {
    let active = true
    setState((current) =>
      current.status === "ready"
        ? current
        : { status: "loading", bootstrap: null, error: null },
    )
    transport
      .bootstrap()
      .then((bootstrap) => {
        if (active) {
          setState({ status: "ready", bootstrap, error: null })
        }
      })
      .catch((error: unknown) => {
        if (active) {
          setState({
            status: "error",
            bootstrap: null,
            error: messageFrom(error),
          })
        }
      })
    return () => {
      active = false
    }
  }, [revision, transport])

  const value = React.useMemo<ProductContextValue>(
    () => ({
      ...state,
      transport,
      refresh: () => setRevision((current) => current + 1),
    }),
    [state, transport],
  )

  return (
    <ProductContext.Provider value={value}>
      {children}
    </ProductContext.Provider>
  )
}

export function useProduct() {
  const context = React.useContext(ProductContext)
  if (!context) {
    throw new Error("useProduct must be used inside ProductProvider")
  }
  return context
}

export function messageFrom(error: unknown) {
  if (error instanceof Error) {
    return error.message
  }
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
