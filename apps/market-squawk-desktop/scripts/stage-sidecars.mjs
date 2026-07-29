import {
  chmodSync,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  realpathSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import { execFileSync } from "node:child_process"
import { createHash } from "node:crypto"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const LINUX_BUNDLER_TOOL_LOCK = Object.freeze([
  {
    name: "AppRun-x86_64",
    size: 31_552,
    sha256: "f30140a43a0a59e46db21bdefdf749b9e9f2c6946e92afabbacf98b8ae73fb4f",
    url: "https://api.github.com/repos/tauri-apps/binary-releases/releases/assets/274691722",
  },
  {
    name: "linuxdeploy-x86_64.AppImage",
    size: 13_264_064,
    sourceSha256: "e762bea85c8eb0d4b3508d46e5c1f037f717d0f9303ae3b4aafc8b04991fa1ef",
    sha256: "20eebde3c18ae2e44279bd624fc72482503aece216d5d77f10932235342f71c1",
    zeroAppImageTypeMagic: true,
    url: "https://api.github.com/repos/tauri-apps/binary-releases/releases/assets/182515537",
  },
  {
    name: "linuxdeploy-plugin-gtk.sh",
    size: 11_648,
    sha256: "cb379f9b0733e9ad9f8bd78f8c2fa038aef2478523bb7d4c8e64ff6a1ea3501a",
    url: "https://raw.githubusercontent.com/tauri-apps/linuxdeploy-plugin-gtk/b5eb8d05b4c0ed40107fe2158c5d8527f94568ef/linuxdeploy-plugin-gtk.sh",
  },
  {
    name: "linuxdeploy-plugin-gstreamer.sh",
    size: 4_857,
    sha256: "c107b49d84edbffc6ab226ed1007e0626a4f7aa2c3a36b7782bef62351d49e94",
    url: "https://raw.githubusercontent.com/tauri-apps/linuxdeploy-plugin-gstreamer/2a2e67491c32995a3f279ad0ecbe77abd512b42a/linuxdeploy-plugin-gstreamer.sh",
  },
  {
    name: "linuxdeploy-plugin-appimage.AppImage",
    size: 16_484_856,
    sha256: "1da16a46fa5e058ae740e7c35ed0d36d86cb869ac9cc8a5fd9a1847d7978d99a",
    url: "https://api.github.com/repos/linuxdeploy/linuxdeploy-plugin-appimage/releases/assets/462804774",
  },
])

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
const releaseDirectory = join(metadata.target_directory, targetTriple, "release")
const stagingDirectory = join(applicationRoot, "src-tauri", "binaries")
const generatedReleaseDirectory = join(
  applicationRoot,
  "src-tauri",
  "generated-release",
)
const packageVersion = metadata.packages.find(
  (candidate) => candidate.name === "market-squawk-desktop",
)?.version
const programs = [
  "market-squawk",
  "market-squawk-capture-helper",
  "market-squawk-onnx-worker",
]

if (packageVersion === undefined) {
  throw new Error("The desktop package version is unavailable.")
}
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

stageCompleteRelease()

if (process.platform === "linux") {
  await prepareLinuxBundlerTools(metadata.target_directory)
}

function stageCompleteRelease() {
  const configuredOutput = process.env.MARKET_SQUAWK_RELEASE_OUTPUT
  if (!configuredOutput) {
    throw new Error(
      "MARKET_SQUAWK_RELEASE_OUTPUT must identify the verified current-target release output.",
    )
  }
  const releaseOutput = realpathSync(resolve(configuredOutput))
  const outputMetadata = lstatSync(releaseOutput)
  if (
    outputMetadata.isSymbolicLink() ||
    !outputMetadata.isDirectory() ||
    (process.platform !== "win32" && outputMetadata.uid !== process.getuid())
  ) {
    throw new Error("The complete release output is not a controlled directory.")
  }

  const bundleName = `market-squawk-${packageVersion}-${targetTriple}.zip`
  const bootstrapName =
    `market-squawk-bootstrap-${targetTriple}${executableSuffix}`
  const manifestName = "market-squawk-release.json"
  const checksumName = "SHA256SUMS"
  const expected = [bootstrapName, bundleName, checksumName, manifestName].sort()
  const observed = readdirSync(releaseOutput).sort()
  if (
    observed.length !== expected.length ||
    observed.some((name, index) => name !== expected[index])
  ) {
    throw new Error("The complete release output has an unexpected file set.")
  }

  const artifacts = [bootstrapName, bundleName, manifestName]
  const checksums = parseChecksums(
    readFileSync(join(releaseOutput, checksumName), "utf8"),
  )
  if (
    checksums.size !== artifacts.length ||
    artifacts.some((name) => checksums.get(name) !== fileSha256(join(releaseOutput, name)))
  ) {
    throw new Error("The complete release checksums are incomplete or invalid.")
  }
  const manifestBytes = readFileSync(join(releaseOutput, manifestName))
  if (manifestBytes.byteLength === 0 || manifestBytes.byteLength > 1024 * 1024) {
    throw new Error("The complete release manifest exceeds its fixed byte bound.")
  }
  const manifest = JSON.parse(manifestBytes.toString("utf8"))
  const target = manifest?.targets?.[0]
  const bundleMetadata = controlledReleaseFile(
    join(releaseOutput, bundleName),
    2 * 1024 * 1024 * 1024,
  )
  if (
    manifest?.schema_version !== 1 ||
    manifest?.product !== "market-squawk" ||
    manifest?.repository !== "Sawmonabo/market-squawk" ||
    manifest?.version !== packageVersion ||
    manifest?.tag !== `v${packageVersion}` ||
    !Array.isArray(manifest.targets) ||
    manifest.targets.length !== 1 ||
    target?.target !== targetTriple ||
    target?.archive?.size !== bundleMetadata.size ||
    target?.archive?.sha256 !== checksums.get(bundleName) ||
    typeof target?.archive?.url !== "string" ||
    !target.archive.url.endsWith(`/${bundleName}`)
  ) {
    throw new Error("The complete release manifest does not match this native package.")
  }
  controlledReleaseFile(join(releaseOutput, bootstrapName), 256 * 1024 * 1024)
  controlledReleaseFile(join(releaseOutput, manifestName), 1024 * 1024)
  controlledReleaseFile(join(releaseOutput, checksumName), 64 * 1024)

  const temporary = `${generatedReleaseDirectory}.new-${process.pid}`
  if (existsSync(temporary)) {
    rmSync(temporary, { recursive: true })
  }
  mkdirSync(temporary, { recursive: false, mode: 0o700 })
  try {
    for (const name of expected) {
      const destination = join(temporary, name)
      copyFileSync(join(releaseOutput, name), destination)
      if (process.platform !== "win32") {
        chmodSync(destination, name === bootstrapName ? 0o755 : 0o644)
      }
    }
    if (existsSync(generatedReleaseDirectory)) {
      const previous = lstatSync(generatedReleaseDirectory)
      if (previous.isSymbolicLink() || !previous.isDirectory()) {
        throw new Error("The generated release resource path is unsafe.")
      }
      rmSync(generatedReleaseDirectory, { recursive: true })
    }
    renameSync(temporary, generatedReleaseDirectory)
  } finally {
    if (existsSync(temporary)) {
      rmSync(temporary, { recursive: true })
    }
  }
}

function parseChecksums(value) {
  const parsed = new Map()
  const lines = value.split("\n").filter((line) => line.length > 0)
  for (const line of lines) {
    const match = /^([0-9a-f]{64})  ([A-Za-z0-9][A-Za-z0-9._-]*)$/.exec(line)
    if (!match || parsed.has(match[2])) {
      throw new Error("The complete release checksum file is malformed.")
    }
    parsed.set(match[2], match[1])
  }
  return parsed
}

function controlledReleaseFile(path, maximumBytes) {
  const metadata = lstatSync(path)
  if (
    metadata.isSymbolicLink() ||
    !metadata.isFile() ||
    metadata.size === 0 ||
    metadata.size > maximumBytes
  ) {
    throw new Error("A complete release file violates its fixed identity bounds.")
  }
  return metadata
}

function fileSha256(path) {
  return sha256(readFileSync(path))
}

async function prepareLinuxBundlerTools(targetDirectory) {
  if (process.arch !== "x64") {
    throw new Error(
      `No reviewed Tauri Linux bundler-tool lock exists for ${process.arch}.`,
    )
  }
  const toolsDirectory = join(targetDirectory, ".tauri")
  mkdirSync(toolsDirectory, { recursive: true, mode: 0o700 })
  const directoryMetadata = lstatSync(toolsDirectory)
  if (
    !directoryMetadata.isDirectory() ||
    directoryMetadata.isSymbolicLink() ||
    directoryMetadata.uid !== process.getuid()
  ) {
    throw new Error("The Tauri local-tools path is not a controlled directory.")
  }
  chmodSync(toolsDirectory, 0o700)

  for (const tool of LINUX_BUNDLER_TOOL_LOCK) {
    const destination = join(toolsDirectory, tool.name)
    if (isLockedTool(destination, tool)) {
      chmodSync(destination, 0o755)
      continue
    }

    const temporary = `${destination}.download-${process.pid}`
    if (existsSync(temporary)) {
      unlinkSync(temporary)
    }
    try {
      const data = await downloadLockedTool(tool)
      writeFileSync(temporary, data, { flag: "wx", mode: 0o700 })
      renameSync(temporary, destination)
      chmodSync(destination, 0o755)
      if (!isLockedTool(destination, tool)) {
        throw new Error(`Installed bundler tool failed verification: ${tool.name}`)
      }
    } finally {
      if (existsSync(temporary)) {
        unlinkSync(temporary)
      }
    }
  }
}

function isLockedTool(path, tool) {
  if (!existsSync(path)) {
    return false
  }
  const metadata = lstatSync(path)
  if (
    metadata.isSymbolicLink() ||
    !metadata.isFile() ||
    metadata.uid !== process.getuid() ||
    metadata.size !== tool.size
  ) {
    return false
  }
  return sha256(readFileSync(path)) === tool.sha256
}

async function downloadLockedTool(tool) {
  const response = await fetch(tool.url, {
    redirect: "follow",
    headers: {
      Accept: "application/octet-stream",
      "User-Agent": "market-squawk-release-builder",
      "X-GitHub-Api-Version": "2022-11-28",
    },
  })
  if (!response.ok || response.body === null) {
    throw new Error(`Bundler tool download failed: ${tool.name}`)
  }
  const declaredLength = response.headers.get("content-length")
  const contentEncoding = response.headers.get("content-encoding")
  if (
    declaredLength !== null &&
    contentEncoding === null &&
    Number(declaredLength) !== tool.size
  ) {
    throw new Error(`Bundler tool length changed: ${tool.name}`)
  }

  const chunks = []
  let length = 0
  for await (const chunk of response.body) {
    length += chunk.byteLength
    if (length > tool.size) {
      throw new Error(`Bundler tool exceeded its locked size: ${tool.name}`)
    }
    chunks.push(Buffer.from(chunk))
  }
  const data = Buffer.concat(chunks, length)
  const sourceSha256 = tool.sourceSha256 ?? tool.sha256
  if (data.byteLength !== tool.size || sha256(data) !== sourceSha256) {
    throw new Error(`Bundler tool digest changed: ${tool.name}`)
  }
  if (tool.zeroAppImageTypeMagic === true) {
    data.fill(0, 8, 11)
  }
  if (sha256(data) !== tool.sha256) {
    throw new Error(`Bundler tool execution identity changed: ${tool.name}`)
  }
  return data
}

function sha256(data) {
  return createHash("sha256").update(data).digest("hex")
}
