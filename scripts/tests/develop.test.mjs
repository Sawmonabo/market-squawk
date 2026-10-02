import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { DevelopmentSupervisor, SERVICE, NATIVE, changeMask } from "../develop.mjs";

test("serialized builds retain the running service on failure and replace only after clean EOF shutdown", async (t) => {
  if (process.platform === "win32") return t.skip("Native fixture uses POSIX SIGTERM; Windows native tree shutdown needs its platform check.");
  const root = await mkdtemp(path.join(os.tmpdir(), "market-squawk-develop-"));
  const events = path.join(root, "events");
  const fixture = path.join(root, "child.mjs");
  await mkdir(path.join(root, "target", "debug"), { recursive: true });
  await mkdir(path.join(root, ".market-squawk", "dev-runtime"), { recursive: true });
  for (const name of ["service", "mcp-relay", "capture-helper", "desktop"]) {
    await writeFile(path.join(root, "target", "debug", `market-squawk-${name}`), "staged fixture");
  }
  await writeFile(fixture, `
import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
const [mode, root, generation] = process.argv.slice(2);
const log = (entry) => appendFileSync(root + '/events', entry + '\\n');
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));
if (mode === 'build') {
  const countFile = root + '/count';
  const number = existsSync(countFile) ? +readFileSync(countFile, 'utf8') + 1 : 1;
  writeFileSync(countFile, String(number));
  log('build-start-' + number);
  if (number === 2 || number === 3) while (!existsSync(root + '/release-' + number)) await delay(10);
  log('build-end-' + number);
  process.exit(number === 2 ? 1 : 0);
} else if (mode === 'native-build') {
  log('native-build');
} else if (mode === 'service') {
  log('service-start-' + generation);
  const keepAlive = setInterval(() => {}, 1000);
  process.stdin.resume();
  process.stdin.on('end', async () => {
    log('service-eof-' + generation);
    await delay(60);
    log('service-stop-' + generation);
    clearInterval(keepAlive);
    process.exit(existsSync(root + '/fail-stop') ? 1 : 0);
  });
} else {
  log('native-start');
  const keepAlive = setInterval(() => {}, 1000);
  process.on('SIGTERM', () => { clearInterval(keepAlive); process.exit(0); });
}
`);
  const entries = async () => (await readFile(events, "utf8").catch(() => "")).trim().split("\n");
  const until = async (predicate) => {
    const end = Date.now() + 5000;
    while (Date.now() < end) {
      if (await predicate()) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error("Fixture did not reach the expected lifecycle barrier.");
  };
  const supervisor = new DevelopmentSupervisor({ root, stopTimeout: 1000, log: () => {}, commands: {
    buildService: { program: process.execPath, args: [fixture, "build", root] },
    buildBoth: { program: process.execPath, args: [fixture, "build", root] },
    buildNative: { program: process.execPath, args: [fixture, "native-build", root] },
    service: (directory) => ({ program: process.execPath, args: [fixture, "service", root, path.basename(directory)] }),
    native: () => ({ program: process.execPath, args: [fixture, "native", root] }),
    serviceOwned: async (record) => until(async () => (await entries()).includes(`service-start-${path.basename(record.stage)}`)),
  } });
  try {
    assert.equal(changeMask("apps/market-squawk-desktop/src/pages/home.tsx"), 0);
    assert.equal(changeMask("apps/market-squawk/tests/production_mcp_composition.rs"), 0);
    assert.equal(changeMask("adapters/market-squawk-adapter-sec/tests/official_fixtures.rs"), 0);
    assert.equal(changeMask("vendor/kernel.rs"), SERVICE | NATIVE);
    supervisor.request(SERVICE);
    await supervisor.flush();
    const original = supervisor.service;
    const generation = path.basename(original.stage);
    supervisor.request(SERVICE);
    const rebuilding = supervisor.flush();
    await until(async () => (await entries()).includes("build-start-2"));
    supervisor.request(SERVICE | NATIVE);
    supervisor.request(SERVICE | NATIVE);
    await writeFile(path.join(root, "release-2"), "");
    await until(async () => (await entries()).includes("build-start-3"));
    assert.equal(supervisor.service, original);
    assert.equal(original.exited, false);
    assert.equal((await entries()).includes(`service-eof-${generation}`), false);
    await writeFile(path.join(root, "release-3"), "");
    await rebuilding;
    const audit = await entries();
    assert.ok(audit.indexOf("build-end-2") < audit.indexOf("build-start-3"));
    assert.equal(audit.filter((entry) => entry.startsWith("build-start-")).length, 3);
    assert.ok(audit.indexOf(`service-stop-${generation}`) < audit.indexOf(`service-start-${path.basename(supervisor.service.stage)}`));
    assert.notEqual(supervisor.service, original);
    await writeFile(path.join(root, "fail-stop"), "");
    const last = supervisor.service;
    supervisor.request(SERVICE);
    await assert.rejects(supervisor.flush(), /failed graceful shutdown.*replacement is blocked/);
    assert.equal(supervisor.service, last);
    assert.equal((await entries()).filter((entry) => entry.startsWith("service-start-")).length, 2);
    // A later edit must not bypass the rejected shutdown and start a duplicate.
    supervisor.request(SERVICE);
    await assert.rejects(supervisor.flush(), /replacement is blocked/);
  } finally {
    try {
      // Cleanup must finish and preserve the intentional shutdown failure.
      await assert.rejects(supervisor.shutdown(), /replacement is blocked/);
    } finally { await rm(root, { recursive: true, force: true }); }
  }
});
