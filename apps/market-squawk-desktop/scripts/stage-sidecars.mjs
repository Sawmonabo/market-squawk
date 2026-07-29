import {
  chmodSync,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  renameSync,
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

if (process.platform === "linux") {
  await prepareLinuxBundlerTools(metadata.target_directory)
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
