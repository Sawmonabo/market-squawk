import * as React from "react"

import { Button } from "@/components/ui/button"

// Keep navigation identities, never earlier page payloads. Evicted pages are
// fetched again using the exact cursor returned by the service.
export function useCursorNavigation() {
  const [position, setPosition] = React.useState<{
    after: string | undefined
    prior: (string | undefined)[]
  }>({ after: undefined, prior: [] })
  return {
    after: position.after,
    page: position.prior.length + 1,
    canGoPrevious: position.prior.length > 0,
    isRepeated: (cursor: string) => cursor === position.after || position.prior.includes(cursor),
    next: (cursor: string | null | undefined) => {
      if (cursor === null || cursor === undefined || cursor === position.after || position.prior.includes(cursor)) return
      setPosition((current) => ({ after: cursor, prior: [...current.prior, current.after] }))
    },
    previous: () => setPosition((current) => current.prior.length === 0 ? current : ({
      after: current.prior.at(-1), prior: current.prior.slice(0, -1),
    })),
    restart: () => setPosition({ after: undefined, prior: [] }),
  }
}

export function CursorNavigation({ navigation, next, busy, error = false, onRestart, onNavigate }: {
  navigation: ReturnType<typeof useCursorNavigation>
  next: string | null | undefined
  busy: boolean
  error?: boolean
  onNavigate?: () => void
  onRestart?: () => void
}) {
  const invalidNext = next !== null && next !== undefined && navigation.isRepeated(next)
  return <nav className="mt-4 flex flex-wrap items-center justify-between gap-3" aria-label="Result pages">
    <span className="text-xs text-muted-foreground" aria-live="polite">Page {navigation.page}</span>
    <div className="flex gap-2">
      <Button size="sm" variant="outline" disabled={busy} onClick={() => { navigation.restart(); onNavigate?.(); onRestart?.() }}>Restart from first page</Button>
      <Button size="sm" variant="outline" disabled={busy || !navigation.canGoPrevious} onClick={() => { navigation.previous(); onNavigate?.() }}>Previous</Button>
      <Button size="sm" variant="outline" disabled={busy || next === null || next === undefined || invalidNext} onClick={() => { navigation.next(next); onNavigate?.() }}>Next</Button>
    </div>
    {error ? <p role="alert" className="w-full text-xs text-destructive">This page could not be opened. Restart from the first page to read a fresh snapshot.</p> : null}
    {invalidNext ? <p role="alert" className="w-full text-xs text-destructive">The next page could not be opened. Restart from the first page to refresh these results.</p> : null}
  </nav>
}
