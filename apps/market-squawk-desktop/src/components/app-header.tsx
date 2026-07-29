import * as React from "react"
import { Search } from "lucide-react"
import { useLocation, useNavigate } from "react-router-dom"

import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command"
import { Separator } from "@/components/ui/separator"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { allNavigation, navigationForPath } from "@/lib/navigation"

export function AppHeader() {
  const location = useLocation()
  const navigate = useNavigate()
  const current = navigationForPath(location.pathname)
  const [open, setOpen] = React.useState(false)

  React.useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        setOpen((value) => !value)
      }
    }
    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [])

  const choose = (path: string) => {
    navigate(path)
    setOpen(false)
  }

  return (
    <>
      <header className="flex h-14 shrink-0 items-center border-b border-border/80 bg-background/95 px-4">
        <SidebarTrigger className="mr-3 text-muted-foreground" />
        <Separator orientation="vertical" className="mr-3 h-4" />
        <div className="flex min-w-0 items-center gap-2 text-xs">
          <span className="text-muted-foreground">
            {current.path.startsWith("/updates") ||
            current.path.startsWith("/backup") ||
            current.path.startsWith("/logs") ||
            current.path.startsWith("/settings")
              ? "Operations"
              : current.label}
          </span>
          <span className="text-muted-foreground/50" aria-hidden="true">
            ›
          </span>
          <span className="truncate font-medium text-foreground">
            {current.path === "/overview" ? "Welcome" : current.label}
          </span>
        </div>
        <button
          type="button"
          onClick={() => setOpen(true)}
          className="ml-auto hidden h-8 min-w-52 items-center gap-2 rounded-lg border border-border bg-card/40 px-3 text-left text-[11px] text-muted-foreground transition-colors hover:bg-accent focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary sm:flex"
          aria-label="Search or run a command"
        >
          <Search className="size-3.5" aria-hidden="true" />
          <span>Search or run a command</span>
          <kbd className="ml-auto rounded border border-border bg-background px-1.5 py-0.5 font-mono text-[9px]">
            ⌘K
          </kbd>
        </button>
      </header>

      <CommandDialog
        open={open}
        onOpenChange={setOpen}
        title="Navigate Market Squawk"
        description="Search the available product routes."
      >
        <CommandInput placeholder="Search Market Squawk…" />
        <CommandList>
          <CommandEmpty>No matching route.</CommandEmpty>
          <CommandGroup heading="Product">
            {allNavigation.map((item) => (
              <CommandItem
                key={item.path}
                value={item.label}
                onSelect={() => choose(item.path)}
              >
                <item.icon aria-hidden="true" />
                <span>{item.label}</span>
                <CommandShortcut>Go</CommandShortcut>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </>
  )
}

