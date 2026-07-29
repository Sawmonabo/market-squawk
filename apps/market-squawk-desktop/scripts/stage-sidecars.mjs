import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  statSync,
} from "node:fs"
import { execFileSync } from "node:child_process"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const applicationRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const repositoryRoot = resolve(applicationRoot, "../..")
const manifestPath = join(repositoryRoot, "Cargo.toml")
const metadata = JSON.parse(
  execFileSync(
    "cargo",
    ["metadata", "--format-version", "1", "--no-deps", "--manifest-path", manifestPath],
    { encoding: "utf8" },
  ),
)
const targetTriple = execFileSync("rustc", ["--print", "host-tuple"], {
  encoding: "utf8",
}).trim()
const executableSuffix = process.platform === "win32" ? ".exe" : ""
const releaseDirectory = join(metadata.target_directory, "release")
const stagingDirectory = join(applicationRoot, "src-tauri", "binaries")
const programs = [
  "market-squawk",
  "market-squawk-capture-helper",
  "market-squawk-onnx-worker",
]

mkdirSync(stagingDirectory, { recursive: true })
for (const program of programs) {
  const source = join(releaseDirectory, `${program}${executableSuffix}`)
  const sourceMetadata = statSync(source)
  if (!sourceMetadata.isFile() || sourceMetadata.size === 0) {
    throw new Error(`Required release program is unavailable: ${program}`)
  }
  const destination = join(
    stagingDirectory,
    `${program}-${targetTriple}${executableSuffix}`,
  )
  copyFileSync(source, destination)
  if (process.platform !== "win32") {
    chmodSync(destination, 0o755)
  }
}
