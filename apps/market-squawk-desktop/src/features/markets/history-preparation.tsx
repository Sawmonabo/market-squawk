import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { PreparationControls } from "./preparation-controls"

export function HistoryPreparation({ historyToken, bootstrap, transport, hasSavedHistory, onPrepared }: {
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  hasSavedHistory: boolean
  onPrepared: () => Promise<void>
}) {
  return <PreparationControls kind="history" token={historyToken} bootstrap={bootstrap}
    transport={transport} hasSavedData={hasSavedHistory} onPrepared={onPrepared} />
}
