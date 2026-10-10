import * as React from "react"
import { hashKey, skipToken, useQuery, useQueryClient, type QueryKey } from "@tanstack/react-query"

import { snapshotQueryMeta } from "@/app/query-client"

const retentionMs = 5 * 60_000
const maximumDisplays = 16
export const detailDisplayOperation = "Desktop.InvestmentDetailDisplay"

// These queries contain rendering fields only. Read leases, cursor identities
// and history generations belong exclusively to the live readers.
export function useDetailDisplayCache<T>(queryKey: QueryKey) {
  const queryClient = useQueryClient()
  const cache = queryClient.getQueryCache()
  const queryHash = hashKey(queryKey)
  const display = useQuery<T>({ queryKey, queryFn: skipToken, gcTime: retentionMs, meta: snapshotQueryMeta })
  // A disabled observer does not refetch or expose invalidation through isStale.
  // Subscribe to that flag so source/reconnect invalidation also hides the display.
  const invalidated = React.useSyncExternalStore(
    React.useCallback((notify: () => void) => cache.subscribe((event) => {
      if (event.query.queryHash === queryHash) notify()
    }), [cache, queryHash]),
    () => queryClient.getQueryState(queryKey)?.isInvalidated ?? false,
  )
  const prune = React.useCallback(() => {
    const displays = cache.findAll({ predicate: (query) => query.queryKey[4] === detailDisplayOperation })
      .sort((a, b) => b.state.dataUpdatedAt - a.state.dataUpdatedAt)
    const retained = displays.filter((query) => query.queryHash === queryHash || query.getObserversCount() > 0)
    for (const query of displays) {
      if (retained.includes(query)) continue
      if (retained.length < maximumDisplays) retained.push(query)
      else queryClient.removeQueries({ queryKey: query.queryKey, exact: true })
    }
  }, [cache, queryClient, queryHash])
  React.useEffect(prune, [prune])
  return {
    data: invalidated ? undefined : display.data,
    // Capture the cache state when a read begins. An authority event during that
    // read cannot resurrect the display before the subsequent revalidation.
    capture: () => queryClient.getQueryState(queryKey),
    save: (data: T, started: ReturnType<typeof queryClient.getQueryState>) => {
      const current = queryClient.getQueryState(queryKey)
      if (current?.isInvalidated && current !== started) return
      queryClient.setQueryData(queryKey, data)
      prune()
    },
    clear: () => { void queryClient.invalidateQueries({ queryKey, exact: true, refetchType: "none" }) },
  }
}
