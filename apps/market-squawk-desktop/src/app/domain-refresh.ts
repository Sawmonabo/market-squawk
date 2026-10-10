import type { Query, QueryClient } from "@tanstack/react-query"

import type { DesktopInvalidationDomain } from "@/lib/schemas"

import { productKeys, type ProductScope } from "./query-client"

export function createDomainRefresh(queryClient: QueryClient, scope: ProductScope) {
  const cache = queryClient.getQueryCache()
  const pending = new Set<Query>()
  let disposed = false
  let scheduled = false

  const schedule = () => {
    if (disposed || scheduled) return
    scheduled = true
    queueMicrotask(() => {
      scheduled = false
      if (disposed) return
      for (const query of pending) {
        if (query.state.fetchStatus !== "idle") continue
        pending.delete(query)
        // A successful in-flight read clears TanStack's invalidation flag. Restore
        // it before refreshing, including when its last observer has left.
        query.invalidate()
        void queryClient.refetchQueries(
          { type: "active", predicate: (candidate) => candidate === query },
          { cancelRefetch: false },
        )
      }
    })
  }

  const unsubscribe = cache.subscribe((event) => {
    if (!pending.has(event.query)) return
    if (event.type === "removed") {
      pending.delete(event.query)
    } else if (event.type === "updated") {
      if (event.action.type === "error") {
        // A failed read remains invalidated with its saved data. Recovery, a
        // later event or an explicit retry owns the next attempt; do not spin.
        pending.delete(event.query)
      } else if (event.query.state.fetchStatus === "idle") {
        schedule()
      }
    }
  })

  return {
    invalidate(domains?: readonly DesktopInvalidationDomain[]) {
      if (disposed) return
      const authorityChanged = domains?.includes("source") === true
      const affected = domains ? new Set<DesktopInvalidationDomain>(domains) : undefined
      if (authorityChanged && affected) {
        // Source authority changes must revalidate both saved financial evidence
        // and price snapshots, including Source-only lifecycle events.
        affected.add("market")
        affected.add("research")
      }
      const keys = affected
        ? [...affected].map((domain) => productKeys.domain(scope, domain))
        : [productKeys.root(scope)]
      for (const queryKey of keys) {
        for (const query of cache.findAll({ queryKey })) {
          if (domains !== undefined && !authorityChanged && query.meta?.domainRefresh === "explicit") continue
          pending.add(query)
          query.invalidate()
          if (query.state.fetchStatus === "idle") schedule()
        }
      }
    },
    dispose() {
      disposed = true
      pending.clear()
      unsubscribe()
    },
  }
}
