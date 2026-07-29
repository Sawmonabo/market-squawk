export function SquawkSignal({ status }: { status: string }) {
  return (
    <section
      aria-label="Squawk Signal"
      className="flex min-h-32 flex-col justify-between rounded-xl border border-border bg-card/45 p-4"
    >
      <div className="flex items-center justify-between font-mono text-[9px] uppercase tracking-widest text-muted-foreground">
        <span className="text-foreground/85">Squawk Signal</span>
        <span>System idle</span>
      </div>
      <svg
        viewBox="0 0 320 52"
        className="w-full overflow-visible"
        role="img"
        aria-label="Idle local market signal"
      >
        <path
          d="M0 28H28L38 23L47 34L57 17L68 43L80 8L92 47L105 20L117 31L130 24L143 29L158 27H185L197 22L207 35L218 13L231 41L243 24L257 29L270 26H320"
          fill="none"
          stroke="var(--primary)"
          strokeWidth="1.6"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
      <div className="flex items-center justify-between font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
        <span>Local control plane</span>
        <span>{status}</span>
      </div>
    </section>
  )
}
