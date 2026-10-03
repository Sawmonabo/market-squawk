import { PreparationStatus, type PreparationController } from "./preparation-controls"

export function HistoryPreparation({ controller, windowDays, onWindowChange, showStatus = true }: {
  controller: PreparationController
  windowDays: string
  onWindowChange: (days: string) => void
  showStatus?: boolean
}) {
  return <div className="min-w-0 flex-1">
    <label className="flex items-center gap-2 text-xs">History window
      <select className="rounded-md border border-input bg-background px-2 py-1.5" value={windowDays}
        disabled={controller.busy || controller.active || controller.unresolved}
        onChange={(event) => onWindowChange(event.target.value)}>
        <option value="all">All saved</option><option value="30">30 days</option>
        <option value="90">90 days</option><option value="365">1 year</option>
        <option value="3650">10 years</option>
      </select>
    </label>
    <PreparationStatus kind="history" controller={controller} showMessage={showStatus} />
  </div>
}
