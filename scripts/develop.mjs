#!/usr/bin/env node
import { spawn } from "node:child_process";
import { access, chmod, copyFile, mkdir, mkdtemp, readFile, rm, rmdir } from "node:fs/promises";
import { createServer } from "node:net";
import path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const ROOT = fileURLToPath(new URL("../", import.meta.url));
const WINDOWS = process.platform === "win32";
const SUFFIX = WINDOWS ? ".exe" : "";
export const SERVICE = 1;
export const NATIVE = 2;
const BOTH = SERVICE | NATIVE;
const IGNORED = new Set(["target", ".git", ".agents", ".market-squawk", "node_modules", "logs", ".venv", "__pycache__", "dist"]);

export function changeMask(relative) {
  const name = relative.split(path.sep).join("/");
  if (name.startsWith("../") || name === ".." || name.split("/").some((part) => IGNORED.has(part))) return 0;
  if (name.startsWith("python/")) return 0; // The model-runtime refresh workflow owns these inputs.
  if (/^(?:apps|adapters|crates)\/[^/]+\/tests(?:\/|$)/.test(name)) return 0;
  if (name.startsWith("apps/market-squawk-desktop/src-tauri/")) return NATIVE;
  if (name.startsWith("apps/market-squawk-desktop/")) return 0; // Vite owns frontend HMR.
  if (name === "apps/market-squawk/src/bin/market-squawk-service.rs"
    || name.startsWith("apps/market-squawk/src/live_source/")
    || name.startsWith("apps/market-squawk/src/application/research/")) return SERVICE;
  if (name.startsWith("apps/market-squawk/") || name.startsWith("crates/")
    || /\.(rs|toml)$/.test(name) || /(^|\/)Cargo\.lock$/.test(name)) return BOTH;
  return 0;
}

function ownedProcess(program, args, { env, cwd, pipe = false } = {}) {
  const child = spawn(program, args, {
    cwd, env, detached: !WINDOWS, windowsHide: true,
    stdio: ["pipe", pipe ? "pipe" : "inherit", "inherit"],
  });
  const record = { child, exited: false, stopping: false };
  record.done = new Promise((resolve) => {
    child.once("error", (error) => { record.error = error; });
    child.once("close", (code, signal) => {
      record.exited = true;
      record.result = { code, signal, error: record.error };
      resolve(record.result);
    });
  });
  // EOF, rather than a Windows emulation of SIGTERM, controls service shutdown.
  child.stdin?.on("error", () => {});
  return record;
}

async function deadline(promise, milliseconds, message) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}

async function stopOwned(record, label, timeout, eof = false) {
  if (record?.stopError) throw record.stopError;
  if (!record || record.exited) return;
  if (record.stopWork) return record.stopWork;
  record.stopping = true;
  record.stopWork = (async () => {
    if (eof) record.child.stdin.end();
    else if (label === "watcher") record.child.kill("SIGTERM"); // Event emitter owns no application state or children.
    else if (WINDOWS) {
      // Node's child.kill("SIGTERM") force-terminates on Windows. Use the owned tree,
      // without /F, and require actual exit; this is not a service graceful-stop hook.
      const stopper = ownedProcess("taskkill.exe", ["/PID", String(record.child.pid), "/T"]);
      const result = await stopper.done;
      if (result.error || (result.code !== 0 && !record.exited)) {
        throw new Error(`Could not stop owned ${label} PID ${record.child.pid}: ${result.error?.message ?? result.code}`);
      }
    } else {
      try { process.kill(-record.child.pid, "SIGTERM"); }
      catch (error) { if (error.code !== "ESRCH") throw error; }
    }
    const result = await deadline(record.done, timeout,
      `Owned ${label} PID ${record.child.pid} did not exit within ${timeout / 1000}s. No force kill or replacement was attempted.`);
    if (label === "service" && (result.error || ![0, 75].includes(result.code))) {
      record.stopError = new Error(`Owned service PID ${record.child.pid} failed graceful shutdown (${result.error?.message ?? result.signal ?? result.code}); replacement is blocked.`);
      throw record.stopError;
    }
  })().catch((error) => {
    if (label === "service") record.stopError = error;
    throw error;
  });
  return record.stopWork;
}

export class DevelopmentSupervisor {
  constructor({ root = ROOT, args = [], env = process.env, log = console.log,
    commands = {}, debounce = 200, stopTimeout = 60_000 } = {}) {
    Object.assign(this, { root, args, env, log, commands, debounce, stopTimeout });
    this.pending = 0;
    this.stages = new Set();
    this.stopping = false;
  }

  request(mask) {
    if (this.stopping) return;
    this.pending |= mask;
    if ((this.pending || this.restartRequested) && !this.running && !this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.flush().catch((error) => this.log(`[dev] ${error.message}`));
      }, this.debounce);
    }
  }

  flush() {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (!this.running) {
      let completed = false;
      this.running = this.drain().then(() => { completed = true; }).finally(() => {
        this.running = undefined;
        if (completed) this.request(0);
        else this.pending = 0;
      });
    }
    return this.running;
  }

  async build(kind) {
    // Select both packages in one invocation so Cargo shares their feature-unified
    // dependency build. Fresh targets are skipped; the change mask owns restart.
    const defaults = ["build", "--locked", "--jobs", "1", "-p", "market-squawk", "-p", "market-squawk-desktop",
      "--bin", "market-squawk-service", "--bin", "market-squawk-mcp-relay",
      "--bin", "market-squawk-capture-helper", "--bin", "market-squawk-desktop",
      "--features", "market-squawk/release-evidence,market-squawk-desktop/desktop-automation"];
    const command = this.commands[`build${kind[0].toUpperCase()}${kind.slice(1)}`] ?? { program: "cargo", args: defaults };
    this.log(`[dev] Building ${kind}; current processes stay running.`);
    this.buildProcess = ownedProcess(command.program, command.args, { cwd: this.root, env: this.env });
    const result = await this.buildProcess.done;
    this.buildProcess = undefined;
    if (result.error || result.code !== 0) {
      this.log(`[dev] ${kind} build failed (${result.error?.message ?? result.signal ?? result.code}); keeping the current processes. Fix the source and save again.`);
      return false;
    }
    return true;
  }

  async stage(mask) {
    this.session ??= await mkdtemp(path.join(this.root, ".market-squawk", "dev-runtime", "session-"));
    const directory = await mkdtemp(path.join(this.session, "generation-"));
    this.stages.add(directory);
    await chmod(directory, 0o700);
    const programs = [
      ...(mask & SERVICE ? ["market-squawk-service", "market-squawk-mcp-relay", "market-squawk-capture-helper"] : []),
      ...(mask & NATIVE ? ["market-squawk-desktop"] : []),
    ];
    try {
      await mkdir(path.join(directory, "bin"), { mode: 0o700 });
      // Tauri resolves macOS resources beside the executable directory. An
      // unbundled development generation has no packaged release to install.
      if ((mask & NATIVE) && process.platform === "darwin") {
        await mkdir(path.join(directory, "Resources"), { mode: 0o700 });
      }
      for (const program of programs) {
        const output = path.join(directory, "bin", `${program}${SUFFIX}`);
        await copyFile(path.join(this.root, "target", "debug", `${program}${SUFFIX}`), output);
        await chmod(output, 0o700);
      }
    } catch (error) {
      await rm(directory, { recursive: true });
      this.stages.delete(directory);
      throw error;
    }
    return directory;
  }

  start(kind, directory) {
    const native = kind === "native";
    const serviceDirectory = native ? this.service?.stage : directory;
    const env = { ...this.env,
      MARKET_SQUAWK_DEVELOPMENT_EXTERNAL_SERVICE: "1",
      MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM: path.join(serviceDirectory, "bin", `market-squawk-service${SUFFIX}`),
      MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM: path.join(serviceDirectory, "bin", `market-squawk-mcp-relay${SUFFIX}`),
    };
    const command = this.commands[kind]?.(directory) ?? {
      program: path.join(directory, "bin", `market-squawk-${native ? "desktop" : "service"}${SUFFIX}`),
      args: native ? this.args : [...this.args.filter((_, index, all) =>
        all[index] !== "--webdriver-visible" && all[index] !== "--webdriver-port"
        && all[index - 1] !== "--webdriver-port"), "--development-control-stdin"],
    };
    const record = ownedProcess(command.program, command.args, { cwd: this.root, env });
    record.stage = directory;
    record.relayStage = native ? serviceDirectory : undefined;
    this[kind] = record;
    this.log(`[dev] Started ${kind} PID ${record.child.pid}.`);
    record.done.then((result) => {
      if (record.stopping || this.stopping) return;
      this.log(`[dev] ${kind} exited (${result.error?.message ?? result.signal ?? result.code}).${native ? " Service and watcher remain active until Ctrl+C." : ""}`);
      if (!native && result.code === 75 && this.service === record) {
        this.restartRequested = true;
        this.request(0);
      }
    });
    return record;
  }

  async cleanStages() {
    const retained = new Set([
      !this.service?.exited && this.service?.stage,
      this.restartRequested && this.service?.stage,
      !this.native?.exited && this.native?.stage,
      !this.native?.exited && this.native?.relayStage,
    ]);
    for (const directory of this.stages) {
      if (!retained.has(directory)) {
        try { await rm(directory, { recursive: true }); }
        catch (error) {
          throw new Error(`Could not remove unreferenced owned runtime stage ${directory}: ${error.message}. New generations are blocked until it can be removed.`);
        }
        this.stages.delete(directory);
      }
    }
  }

  async drain() {
    while ((this.pending || this.restartRequested) && !this.stopping) {
      await this.cleanStages();
      if (this.restartRequested) {
        this.restartRequested = false;
        this.log("[dev] Service requested restart; reusing its last successful executable.");
        await this.startService(this.service.stage);
        continue;
      }
      // A repaired initial build must still launch the whole app, even when the
      // edit that repairs it is service-only.
      let mask = this.pending | (!this.native ? NATIVE : 0);
      if ((mask & NATIVE) && (!this.service || this.service.exited)) mask |= SERVICE;
      this.pending = 0;
      if (!await this.build(mask === BOTH ? "both" : mask & SERVICE ? "service" : "native")) continue;
      if (this.stopping) return;
      const directory = await this.stage(mask);
      try {
        if (mask & NATIVE) await stopOwned(this.native, "native", this.stopTimeout);
        if (mask & SERVICE) {
          await stopOwned(this.service, "service", this.stopTimeout, true);
          if (this.stopping) return;
          this.restartRequested = false;
          await this.startService(directory);
        }
        if (!this.stopping && mask & NATIVE) this.start("native", directory);
      } catch (error) {
        this.pending = 0;
        throw error; // A failed stop retains its process reference and prevents a duplicate.
      } finally { await this.cleanStages(); }
    }
  }

  evidenceFile() {
    const index = this.args.indexOf("--installation-data-root");
    return path.join(this.args[index + 1], "control", "installed-service-startup", "state.json");
  }

  async startService(directory) {
    let previous;
    if (!this.commands.serviceOwned) {
      try { previous = await readFile(this.evidenceFile(), "utf8"); }
      catch (error) { if (error.code !== "ENOENT") throw error; }
    }
    if (this.stopping) return;
    const record = this.start("service", directory);
    if (this.commands.serviceOwned) await this.commands.serviceOwned(record);
    else await this.waitForServiceOwnership(record, previous);
  }

  async waitForServiceOwnership(record, previous) {
    const started = Date.now();
    while (!this.stopping && !record.exited && Date.now() - started < 60_000) {
      try {
        const bytes = await readFile(this.evidenceFile(), "utf8");
        const evidence = JSON.parse(bytes);
        // This is only an owned-process startup barrier. Rust authenticates readiness.
        if (bytes !== previous && evidence.schemaVersion === 1 && evidence.process?.processId === record.child.pid) {
          if (evidence.state?.state === "failed" || evidence.state?.state === "stopped") {
            throw new Error(`Owned service PID ${record.child.pid} reported ${evidence.state.state} during startup.`);
          }
          return;
        }
      } catch (error) { if (error.code !== "ENOENT") throw error; }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if (!this.stopping) throw new Error(`Owned service PID ${record.child.pid} did not publish its startup identity. No other service was adopted.`);
  }

  shutdown() {
    if (this.shutdownWork) return this.shutdownWork;
    this.stopping = true;
    this.pending = 0;
    this.restartRequested = false;
    clearTimeout(this.timer);
    this.shutdownWork = (async () => {
      const failures = [];
      // Stop the watcher first; a running Cargo command must finish before cleanup.
      for (const [record, label, eof] of [[this.watcher, "watcher", false], [this.native, "native", false],
        [this.service, "service", true], [this.vite, "Vite", false]]) {
        try { await stopOwned(record, label, this.stopTimeout, eof); }
        catch (error) { failures.push(error.message); }
      }
      await this.running?.catch((error) => failures.push(error.message));
      await this.cleanStages();
      if (!this.stages.size && this.session) await rmdir(this.session);
      if (failures.length) throw new Error(failures.join("\n"));
    })();
    return this.shutdownWork;
  }
}

async function windowsPnpm() {
  // npm/Corepack's maintained CMD shim names its JavaScript entry point. Run that
  // with the selected Node directly, preserving paths without CMD interpolation.
  for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
    try {
      const shim = await readFile(path.join(directory, "pnpm.cmd"), "utf8");
      const relative = shim.match(/%dp0%[\\/]([^"\r\n]*?pnpm\.(?:c?js))/i)?.[1];
      if (relative) {
        const entry = path.resolve(directory, relative.replaceAll("\\", "/"));
        await access(entry);
        return { program: process.execPath, prefix: [entry] };
      }
    } catch (error) { if (error.code !== "ENOENT") throw error; }
  }
  throw new Error("Could not resolve the selected pnpm CMD shim's JavaScript entry point. Use pnpm installed by npm/Corepack and run just setup.");
}

async function main() {
  const { values } = parseArgs({ options: {
    "data-dir": { type: "string" }, "installation-data-root": { type: "string" },
    "training-release-root": { type: "string" }, "webdriver-port": { type: "string" },
    "webdriver-visible": { type: "boolean" },
  } });
  const args = [];
  for (const name of ["data-dir", "installation-data-root", "training-release-root"]) {
    if (!values[name]) throw new Error(`Required argument: --${name}`);
    args.push(`--${name}`, path.resolve(values[name]));
  }
  if (values["webdriver-port"] !== undefined) {
    if (!/^\d+$/.test(values["webdriver-port"]) || +values["webdriver-port"] < 1 || +values["webdriver-port"] > 65535) {
      throw new Error("--webdriver-port must be an integer from 1 to 65535.");
    }
    args.push("--webdriver-port", values["webdriver-port"]);
  }
  if (values["webdriver-visible"]) {
    if (values["webdriver-port"] === undefined) throw new Error("--webdriver-visible requires --webdriver-port.");
    args.push("--webdriver-visible");
  }
  const desktop = path.join(ROOT, "apps", "market-squawk-desktop");
  const config = JSON.parse(await readFile(path.join(desktop, "src-tauri", "tauri.conf.json"), "utf8"));
  const env = { ...process.env, CARGO_BUILD_JOBS: "1",
    MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT: await readFile(path.join(path.resolve(values["training-release-root"]), "share", "market-squawk", "training-foundation.json"), "utf8"),
    TAURI_CONFIG: JSON.stringify({ build: { devUrl: "http://127.0.0.1:1420" },
      app: { security: { devCsp: config.app.security.devCsp.replaceAll("localhost:1420", "127.0.0.1:1420") } } }),
  };
  delete env.TAURI_DEV_HOST; // Keep Vite's websocket on the same owned loopback port.
  // strictPort also protects the interval after this preflight check.
  await new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", () => reject(new Error("Vite port 127.0.0.1:1420 is already owned or unavailable. Stop its owner before starting dev.")));
    server.listen({ host: "127.0.0.1", port: 1420, exclusive: true }, () => server.close(resolve));
  });
  await mkdir(path.join(ROOT, ".market-squawk", "dev-runtime"), { recursive: true, mode: 0o700 });
  const supervisor = new DevelopmentSupervisor({ args, env });
  const stop = async () => {
    try { await supervisor.shutdown(); }
    catch (error) { console.error(`[dev] ${error.message}`); process.exitCode = 1; }
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  try {
    if (WINDOWS) {
      const pnpm = await windowsPnpm();
      supervisor.vite = ownedProcess(pnpm.program,
        [...pnpm.prefix, "--dir", desktop, "dev", "--host", "127.0.0.1", "--port", "1420", "--strictPort"], { cwd: desktop, env });
    } else {
      supervisor.vite = ownedProcess("pnpm", ["--dir", desktop, "dev", "--host", "127.0.0.1", "--port", "1420", "--strictPort"], { cwd: desktop, env });
    }
    supervisor.vite.done.then(() => { if (!supervisor.stopping) { console.error("[dev] Owned Vite exited; stopping development."); process.exitCode = 1; void stop(); } });
    // Watchexec's event-only handler ignores stdin EOF; it exits on termination.
    supervisor.watcher = ownedProcess("watchexec", ["--only-emit-events", "--emit-events-to=json-stdio",
      "--debounce=50ms", "--no-meta", "--no-follow-symlinks", "--project-origin", ROOT, "--watch", ROOT,
      ...[...IGNORED].flatMap((name) => ["--ignore", `**/${name}/**`])], { cwd: ROOT, env, pipe: true });
    const lines = createInterface({ input: supervisor.watcher.child.stdout });
    lines.on("line", (line) => {
      try {
        const event = JSON.parse(line);
        for (const tag of event.tags ?? []) {
          if (tag.kind !== "path") continue;
          const relative = path.relative(ROOT, tag.absolute);
          if (relative.startsWith("python/") || relative === "scripts/build_python_release.py") {
            if (!supervisor.modelInputsChanged) console.error("[dev] Model-runtime inputs changed. Run just refresh-model-runtime, then restart just dev; the running model runtime has not been refreshed.");
            supervisor.modelInputsChanged = true;
          }
          if (["scripts/develop.mjs", "Justfile", "justfile"].includes(relative)) {
            console.error("[dev] Development launcher inputs changed. Restart just dev to apply them.");
          }
          supervisor.request(changeMask(relative));
        }
      } catch (error) {
        console.error(`[dev] Invalid Watchexec event: ${error.message}`);
        process.exitCode = 1;
        void stop();
      }
    });
    supervisor.watcher.done.then(() => { if (!supervisor.stopping) { console.error("[dev] Watchexec exited; stopping development."); process.exitCode = 1; void stop(); } });
    const started = Date.now();
    while (!supervisor.stopping) {
      try {
        const response = await fetch("http://127.0.0.1:1420/", { signal: AbortSignal.timeout(500) });
        if (response.ok) break;
      } catch { /* Await the owned Vite startup, never adopt a pre-existing listener. */ }
      if (Date.now() - started > 30_000) throw new Error("Owned Vite did not become available within 30s.");
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    console.log("[dev] Vite HMR active. Rust saves use one build queue. Closing Desktop leaves the service and watcher running; Ctrl+C stops development.");
    supervisor.request(BOTH);
    await supervisor.flush();
  } catch (error) {
    console.error(`[dev] ${error.message}`);
    process.exitCode = 1;
    await stop();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => { console.error(`[dev] ${error.message}`); process.exitCode = 1; });
}
