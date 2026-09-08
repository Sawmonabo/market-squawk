import type { ReactNode } from "react"
import { Database } from "lucide-react"
import { Link } from "react-router-dom"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

export function MacroAvailabilityNotice({
  title,
  detail,
  showSetup = false,
}: {
  title: string
  detail: string
  showSetup?: boolean
}) {
  return (
    <Alert>
      <Database aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <p>{detail}</p>
        {showSetup ? (
          <Button asChild className="mt-3" size="sm">
            <Link to="/connections/sources">Open setup</Link>
          </Button>
        ) : null}
      </AlertDescription>
    </Alert>
  )
}

export function MacroEvidenceFact({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="min-w-0">
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd
        className={`mt-1 break-all text-[11px] leading-4 text-foreground/85 ${
          mono ? "font-mono" : ""
        }`}
      >
        {value}
      </dd>
    </div>
  )
}

export function MacroEvidenceBadge({
  children,
  tone = "neutral",
}: {
  children: ReactNode
  tone?: "good" | "neutral"
}) {
  return (
    <span
      className={`rounded border px-2 py-1 text-[9px] font-medium uppercase tracking-wider ${
        tone === "good"
          ? "border-[var(--success)]/35 text-[var(--success)]"
          : "border-border text-muted-foreground"
      }`}
    >
      {children}
    </span>
  )
}
