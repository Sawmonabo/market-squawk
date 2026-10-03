import * as React from "react"

import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { PreparationStatus, usePreparationController } from "./preparation-controls"

export function FinancialPreparation({ selectionToken, bootstrap, transport, needsData, onPrepared, onSettled }: {
  selectionToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  needsData: boolean
  onPrepared: () => Promise<void>
  onSettled: () => Promise<void>
}) {
  const controller = usePreparationController({ kind: "financial", token: selectionToken,
    bootstrap, transport, onPrepared, onSettled })
  const attempted = React.useRef(false)
  React.useEffect(() => {
    // Start only from a successful read identifying missing reports. An existing
    // request (including a lost acknowledgment or failure) must be reconciled,
    // never replaced by opening another tab or rendering this component again.
    if (!needsData || attempted.current || controller.preparation || !controller.canStart) return
    attempted.current = true
    controller.start()
  }, [needsData, controller])
  return <PreparationStatus kind="financial" controller={controller} />
}
