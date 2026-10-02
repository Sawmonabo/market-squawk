import { useMutation } from "@tanstack/react-query"

import { Button } from "@/components/ui/button"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"

import { parsePlanningSave } from "./saved-planning-contracts"
import type { PlanningCalculationIdentity, SavedPlanningSummary } from "./saved-planning-contracts"

// Save is an explicit durable mutation. It has no detail-read abort signal;
// closing a report releases rendering but cannot undo a save already accepted.
export function PlanningSaveControl({ accountToken, calculation, kind, bootstrap, transport }: {
  accountToken: string
  calculation: PlanningCalculationIdentity
  kind: SavedPlanningSummary["kind"]
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const available = hasProductCapability(bootstrap, "portfolio_planning_save")
  const identity = { calculationToken: calculation.calculationToken,
    calculatedAtUnixNanos: calculation.calculatedAtUnixNanos, snapshotToken: calculation.snapshotToken }
  const save = useMutation({
    gcTime: 0,
    retry: false,
    mutationFn: async () => parsePlanningSave(await transport.query({
      query: "portfolioSavePlanningResult", accountToken,
      calculationToken: identity.calculationToken, confirmed: true,
    }), accountToken, identity, kind),
  })
  return <div className="space-y-2 rounded-lg border border-border/70 p-3" aria-label="Save this planning result">
    <p className="text-xs leading-5 text-muted-foreground">
      Calculated {formatUnixNanos(identity.calculatedAtUnixNanos)}. Save keeps this exact calculation and its original assumptions for reopening after restart.
      Saving does not recalculate, update prices, or place an order.
    </p>
    <Button type="button" variant="outline" disabled={!available || save.isPending || save.isSuccess}
      onClick={() => save.mutate()}>{save.isPending ? "Saving planning result…" : save.isSuccess ? "Planning result saved" : "Save planning result"}</Button>
    {!available ? <p className="text-xs text-muted-foreground">Saving planning results is currently unavailable.</p> : null}
    {save.isSuccess ? <p role="status" className="text-xs text-muted-foreground">
      Saved {formatUnixNanos(save.data.savedAtUnixNanos)}. Open or refresh Saved planning results to reopen it.
    </p> : null}
    {save.isError ? <p role="alert" className="text-xs text-destructive">
      The save could not be confirmed. Try again to save this same calculation.
    </p> : null}
  </div>
}
