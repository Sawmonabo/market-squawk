import "@fontsource-variable/geist"
import "@fontsource-variable/geist-mono"
import React from "react"
import ReactDOM from "react-dom/client"
import { HashRouter } from "react-router-dom"

import { App } from "@/app/app"
import { createProductTransport } from "@/lib/tauri-transport"
import "@/styles/globals.css"

const root = document.getElementById("root")
if (!root) {
  throw new Error("Market Squawk desktop root is missing")
}

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <HashRouter>
      <App transport={createProductTransport()} />
    </HashRouter>
  </React.StrictMode>,
)
