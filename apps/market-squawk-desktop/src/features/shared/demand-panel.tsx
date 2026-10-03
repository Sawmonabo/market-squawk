import * as React from "react"

// Unmount closed sections so their read observers release requests and payloads.
// Durable analysis actions are owned by the service and continue independently.
export function DemandPanel({ title, children, className = "" }: {
  title: string
  children: React.ReactNode
  className?: string
}) {
  const [open, setOpen] = React.useState(false)
  return <details className={className} onToggle={(event) => setOpen(event.currentTarget.open)}>
    <summary className="cursor-pointer text-sm font-semibold">{title}</summary>
    {open ? <div className="mt-4">{children}</div> : null}
  </details>
}
