import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { PreparationControls } from "./preparation-controls"

export function FinancialPreparation({ selectionToken, bootstrap, transport, onPrepared, onSettled }: {
  selectionToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onPrepared: () => Promise<void>
  onSettled: () => Promise<void>
}) {
  return <PreparationControls kind="financial" token={selectionToken} bootstrap={bootstrap}
    transport={transport} onPrepared={onPrepared} onSettled={onSettled} />
}
