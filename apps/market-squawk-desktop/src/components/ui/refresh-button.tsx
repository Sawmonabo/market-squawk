import type { ComponentProps } from "react"
import { RefreshCw } from "lucide-react"

import { cn } from "@/lib/utils"

import { Button } from "./button"

type RefreshButtonProps = Omit<ComponentProps<"button">, "children" | "aria-label" | "title"> & {
  label?: string
  refreshing?: boolean
}

export function RefreshButton({
  label = "Refresh",
  refreshing = false,
  disabled,
  className,
  ...props
}: RefreshButtonProps) {
  return <Button
    {...props}
    type="button"
    variant="ghost"
    size="icon-sm"
    className={cn("size-7 text-muted-foreground/60 hover:text-foreground", className)}
    aria-label={label}
    title={label}
    aria-busy={refreshing}
    disabled={disabled || refreshing}
  >
    <RefreshCw className={cn("size-3.5", refreshing && "motion-safe:animate-spin")} aria-hidden="true" />
  </Button>
}
