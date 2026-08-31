import { Search } from "lucide-react"

import type { ProductScope } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import type { ProductTransport } from "@/lib/transport"

import { LookupSurface } from "./lookup-surface"

export function GlobalLookup({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 gap-1.5 px-2 font-mono text-[9px] uppercase tracking-wide text-muted-foreground"
        >
          <Search className="size-3" aria-hidden="true" />
          Search
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[min(760px,calc(100vh-2rem))] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Search Market Squawk</DialogTitle>
          <DialogDescription>
            Find an investment by ticker or company name, or search your saved research and
            investment workspace.
          </DialogDescription>
        </DialogHeader>
        <LookupSurface transport={transport} scope={scope} autoFocus />
      </DialogContent>
    </Dialog>
  )
}
