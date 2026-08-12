import * as React from "react"
import { CircleAlert, KeyRound, LoaderCircle, Search } from "lucide-react"
import { useLocation, useNavigate } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { Button } from "@/components/ui/button"
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Separator } from "@/components/ui/separator"
import { SidebarTrigger } from "@/components/ui/sidebar"
import {
  navigationForPath,
  navigationSectionForPath,
  navigationSections,
} from "@/lib/navigation"

export function AppHeader() {
  const location = useLocation()
  const navigate = useNavigate()
  const product = useProduct()
  const current = navigationForPath(location.pathname)
  const currentSection = navigationSectionForPath(location.pathname)
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
  const shortcut =
    product.status === "ready" && product.bootstrap.platform === "macos"
      ? "⌘K"
      : "Ctrl+K"
  const serviceBootstrap =
    product.availability === "degraded" ? product.serviceBootstrap : null
  const requiresUnlock =
    serviceBootstrap?.requirement === "encrypted_fallback_locked"

  const recover = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const fields = new FormData(event.currentTarget)
    const unlock = String(fields.get("unlock") ?? "")
    event.currentTarget.reset()
    void product.recoverService(unlock)
  }

  return (
    <>
      <header className="flex h-14 shrink-0 items-center border-b border-border/80 bg-background/95 px-4">
        <SidebarTrigger className="mr-3 text-muted-foreground" />
        <Separator orientation="vertical" className="mr-3 h-4" />
        <div className="flex min-w-0 items-center gap-2 text-xs">
          <span className="text-muted-foreground">{currentSection.label}</span>
          <span className="text-muted-foreground/50" aria-hidden="true">
            ›
          </span>
          <span className="truncate font-medium text-foreground">
            {current.label}
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
            {shortcut}
          </kbd>
        </button>
      </header>

      {serviceBootstrap ? (
        <section
          aria-label="Secure local storage recovery"
          aria-live="polite"
          className="shrink-0 border-b border-amber-400/25 bg-amber-400/[0.07] px-4 py-3"
        >
          <div className="mx-auto flex max-w-[1180px] flex-wrap items-center gap-3">
            <CircleAlert
              className="size-4 shrink-0 text-amber-300"
              aria-hidden="true"
            />
            <div className="min-w-60 flex-1">
              <p className="text-xs font-semibold text-foreground">
                Secure local storage needs your attention
              </p>
              <p className="mt-0.5 text-[11px] leading-5 text-muted-foreground">
                {requiresUnlock
                  ? "Enter the local security password to unlock Market Squawk's encrypted credential fallback."
                  : "Continue once to let your operating system approve Market Squawk's secure credential storage."}
                {" The workspace shell and navigation remain available."}
              </p>
            </div>
            <form
              onSubmit={recover}
              className="flex min-w-0 flex-wrap items-end gap-2"
            >
              {requiresUnlock ? (
                <div className="min-w-52">
                  <Label htmlFor="service-fallback-unlock" className="sr-only">
                    Local security password
                  </Label>
                  <div className="relative">
                    <KeyRound
                      className="pointer-events-none absolute top-2.5 left-3 size-4 text-muted-foreground"
                      aria-hidden="true"
                    />
                    <Input
                      id="service-fallback-unlock"
                      name="unlock"
                      type="password"
                      autoComplete="current-password"
                      spellCheck={false}
                      className="h-9 pl-9 font-mono"
                      placeholder="Local security password"
                      disabled={product.recoveryPending}
                    />
                  </div>
                </div>
              ) : null}
              <Button type="submit" size="sm" disabled={product.recoveryPending}>
                {product.recoveryPending ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : null}
                {product.recoveryPending
                  ? "Finishing secure setup…"
                  : requiresUnlock
                    ? "Unlock secure storage"
                    : "Continue securely"}
              </Button>
            </form>
            {product.recoveryError ? (
              <p role="alert" className="w-full pl-7 text-xs text-red-300">
                {product.recoveryError}
              </p>
            ) : null}
          </div>
        </section>
      ) : null}

      <CommandDialog
        open={open}
        onOpenChange={setOpen}
        title="Navigate Market Squawk"
        description="Search the available product routes."
      >
        <CommandInput placeholder="Search Market Squawk…" />
        <CommandList>
          <CommandEmpty>No matching route.</CommandEmpty>
          {navigationSections.map((section) => (
            <CommandGroup key={section.label} heading={section.label}>
              {section.items.map((item) => (
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
          ))}
        </CommandList>
      </CommandDialog>
    </>
  )
}
