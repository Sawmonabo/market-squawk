import "@fontsource-variable/geist"
import "@fontsource-variable/geist-mono"
import React, { Component, type ErrorInfo, type ReactNode } from "react"
import ReactDOM from "react-dom/client"
import { HashRouter } from "react-router-dom"

import { App } from "@/app/app"
import { createDesktopTransport } from "@/lib/tauri-transport"
import "@/styles/globals.css"

const root = document.getElementById("root")
if (!root) {
  throw new Error("Market Squawk desktop root is missing")
}

function DesktopRoot() {
  return <App transport={createDesktopTransport()} />
}

class DesktopRootErrorBoundary extends Component<
  { children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  componentDidCatch(error: Error, information: ErrorInfo) {
    console.error("Market Squawk desktop failed to render", error, information)
  }

  render() {
    if (!this.state.error) return this.props.children
    return (
      <main className="grid min-h-screen place-items-center bg-background px-6 text-foreground">
        <section className="w-full max-w-lg rounded-xl border border-border bg-card/70 p-6 shadow-2xl">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-primary">
            Workspace recovery
          </p>
          <h1 className="mt-2 text-xl font-semibold">Market Squawk could not open</h1>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            The desktop interface encountered a local startup problem. Your stored data was not
            changed.
          </p>
          <p className="mt-4 rounded-lg border border-border bg-background/55 p-3 text-xs text-muted-foreground">
            Reload the workspace. If the problem continues, review Logs &amp; Diagnostics.
          </p>
          <button
            className="mt-5 inline-flex h-9 items-center rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground"
            onClick={() => window.location.reload()}
            type="button"
          >
            Reload workspace
          </button>
        </section>
      </main>
    )
  }
}

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <DesktopRootErrorBoundary>
      <HashRouter>
        <DesktopRoot />
      </HashRouter>
    </DesktopRootErrorBoundary>
  </React.StrictMode>,
)
