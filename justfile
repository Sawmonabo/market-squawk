set dotenv-load := false
set positional-arguments
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]

root := justfile_directory()
desktop := join(root, "apps", "market-squawk-desktop")
python_project := join(root, "python")
python_requirements := join(python_project, "requirements.lock")
python_environment := join(python_project, ".venv")
python_executable := if os_family() == "windows" { join(python_environment, "Scripts", "python.exe") } else { join(python_environment, "bin", "python") }
development_data := join(root, ".market-squawk", "development")
development_installation := join(root, ".market-squawk", "development-installation")
development_model_runtime := join(root, ".market-squawk", "development-model-runtime")
development_model_release := join(development_model_runtime, "python", "release-cp314")
python_release_builder := join(root, "scripts", "build_python_release.py")

# List supported developer commands.
default:
    @just --list

[private]
_tools:
    @just --version
    @rustc --version
    @cargo --version
    @uv --version

[private]
[unix]
_frontend $frontend_action:
    #!/usr/bin/env bash
    set -euo pipefail

    action="${frontend_action}"
    case "${action}" in
        setup|dev|dev-web|doctor|typecheck|test|build) ;;
        *)
            printf 'Unsupported frontend action: %s\n' "${action}" >&2
            exit 2
            ;;
    esac

    IFS= read -r required_node < "{{ root }}/.nvmrc"
    required_node="${required_node%$'\r'}"
    if [[ ! "${required_node}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        printf 'The repository .nvmrc does not contain an exact Node.js version.\n' >&2
        exit 2
    fi

    nvm_directory="${NVM_DIR:-}"
    if [[ -z "${nvm_directory}" && -n "${HOME:-}" ]]; then
        nvm_directory="${HOME}/.nvm"
    fi
    nvm_script="${nvm_directory:+${nvm_directory}/nvm.sh}"
    if [[ -n "${nvm_script}" && -s "${nvm_script}" ]]; then
        source "${nvm_script}"
    fi
    if command -v nvm >/dev/null 2>&1; then
        if [[ "${action}" == "setup" ]]; then
            nvm install "${required_node}"
        fi
        nvm use "${required_node}"
    fi

    if ! actual_node="$(node --version 2>/dev/null)"; then
        printf 'Node.js %s is required; install or activate it before continuing.\n' "${required_node}" >&2
        exit 1
    fi
    if [[ "${actual_node}" != "v${required_node}" ]]; then
        printf 'Node.js %s is required by .nvmrc; found %s.\n' "${required_node}" "${actual_node}" >&2
        exit 1
    fi

    if [[ "${action}" == "setup" ]]; then
        corepack enable
        corepack prepare pnpm@10.31.0 --activate
    fi
    if ! actual_pnpm="$(pnpm --version 2>/dev/null)"; then
        printf 'pnpm 10.31.0 is required; run just setup first.\n' >&2
        exit 1
    fi
    if [[ "${actual_pnpm}" != "10.31.0" ]]; then
        printf 'pnpm 10.31.0 is required; found %s.\n' "${actual_pnpm}" >&2
        exit 1
    fi

    case "${action}" in
        setup)
            exec pnpm --dir "{{ desktop }}" install --frozen-lockfile
            ;;
        dev)
            export MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM="{{ root }}/target/debug/market-squawk-service"
            export MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM="{{ root }}/target/debug/market-squawk-mcp-relay"
            printf 'Market Squawk development data: %s\n' "{{ development_data }}"
            printf 'Market Squawk development service state: %s\n' "{{ development_installation }}"
            exec pnpm --dir "{{ desktop }}" tauri dev -- -- --data-dir "{{ development_data }}" --installation-data-root "{{ development_installation }}" --training-release-root "{{ development_model_release }}"
            ;;
        dev-web)
            exec pnpm --dir "{{ desktop }}" dev
            ;;
        doctor)
            exec pnpm --dir "{{ desktop }}" tauri info
            ;;
        typecheck)
            exec pnpm --dir "{{ desktop }}" typecheck
            ;;
        test)
            exec pnpm --dir "{{ desktop }}" test --run
            ;;
        build)
            exec pnpm --dir "{{ desktop }}" build
            ;;
    esac

[private]
[script("powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File")]
[windows]
_frontend $frontend_action:
    $ErrorActionPreference = "Stop"
    $action = $env:frontend_action
    if ($action -notin @("setup", "dev", "dev-web", "doctor", "typecheck", "test", "build")) {
        throw "Unsupported frontend action: $action"
    }

    $requiredNode = (Get-Content -LiteralPath "{{ root }}/.nvmrc" -Raw).Trim()
    if ($requiredNode -notmatch '^\d+\.\d+\.\d+$') {
        throw "The repository .nvmrc does not contain an exact Node.js version."
    }

    if (Get-Command nvm -ErrorAction SilentlyContinue) {
        if ($action -eq "setup") {
            & nvm install $requiredNode
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
        & nvm use $requiredNode
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    $actualNode = (& node --version).Trim()
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    if ($actualNode -cne "v$requiredNode") {
        throw "Node.js $requiredNode is required by .nvmrc; found $actualNode."
    }

    if ($action -eq "setup") {
        & corepack enable
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        & corepack prepare pnpm@10.31.0 --activate
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    $actualPnpm = (& pnpm --version).Trim()
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    if ($actualPnpm -cne "10.31.0") {
        throw "pnpm 10.31.0 is required; found $actualPnpm."
    }

    switch ($action) {
        "setup" {
            & pnpm --dir "{{ desktop }}" install --frozen-lockfile
            exit $LASTEXITCODE
        }
        "dev" {
            $env:MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM = "{{ root }}/target/debug/market-squawk-service.exe"
            $env:MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM = "{{ root }}/target/debug/market-squawk-mcp-relay.exe"
            try {
                Write-Output "Market Squawk development data: {{ development_data }}"
                Write-Output "Market Squawk development service state: {{ development_installation }}"
                & pnpm --dir "{{ desktop }}" tauri dev -- -- --data-dir "{{ development_data }}" --installation-data-root "{{ development_installation }}" --training-release-root "{{ development_model_release }}"
                $exitCode = $LASTEXITCODE
            } finally {
                Remove-Item Env:MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM -ErrorAction SilentlyContinue
                Remove-Item Env:MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM -ErrorAction SilentlyContinue
            }
            exit $exitCode
        }
        "dev-web" {
            & pnpm --dir "{{ desktop }}" dev
            exit $LASTEXITCODE
        }
        "doctor" {
            & pnpm --dir "{{ desktop }}" tauri info
            exit $LASTEXITCODE
        }
        "typecheck" {
            & pnpm --dir "{{ desktop }}" typecheck
            exit $LASTEXITCODE
        }
        "test" {
            & pnpm --dir "{{ desktop }}" test --run
            exit $LASTEXITCODE
        }
        "build" {
            & pnpm --dir "{{ desktop }}" build
            exit $LASTEXITCODE
        }
    }

[private]
_python-setup: _tools
    uv --directory "{{ python_project }}" python install 3.14.6
    uv --directory "{{ python_project }}" venv --python 3.14.6 --allow-existing .venv
    uv --directory "{{ python_project }}" pip sync --python "{{ python_executable }}" --require-hashes --strict "{{ python_requirements }}"
    uv --directory "{{ python_project }}" pip install --python "{{ python_executable }}" --no-deps --strict --reinstall-package market-squawk "{{ python_project }}"

[private]
[unix]
_prepare-model-runtime-cache:
    MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK=1 "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --prepare-cache-only

[private]
[windows]
_prepare-model-runtime-cache:
    $env:MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK = "1"; try { & "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --prepare-cache-only; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } finally { Remove-Item Env:MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK -ErrorAction SilentlyContinue }

[private]
_refresh-model-runtime:
    "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --refresh-source-closure --lock "{{ python_project }}/wheelhouse-lock.json"
    just _prepare-model-runtime-cache
    "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --offline
    "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --verify-development-runtime

[private]
[unix]
_ensure-model-runtime:
    if "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --verify-development-runtime; then echo "Reusing verified Market Squawk model runtime."; else just _refresh-model-runtime; fi

[private]
[windows]
_ensure-model-runtime:
    & "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --verify-development-runtime; if ($LASTEXITCODE -eq 0) { Write-Output "Reusing verified Market Squawk model runtime." } else { just _refresh-model-runtime; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }

[private]
[unix]
_build-development-service-runtime:
    #!/usr/bin/env bash
    set -euo pipefail
    export MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT="$(< "{{ development_model_runtime }}/python/training-foundation.json")"
    exec cargo build --locked -p market-squawk --features release-evidence --bin market-squawk-service --bin market-squawk-mcp-relay --bin market-squawk-capture-helper

[private]
[windows]
_build-development-service-runtime:
    $env:MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT = [System.IO.File]::ReadAllText("{{ development_model_runtime }}/python/training-foundation.json"); try { cargo build --locked -p market-squawk --features release-evidence --bin market-squawk-service --bin market-squawk-mcp-relay --bin market-squawk-capture-helper; exit $LASTEXITCODE } finally { Remove-Item Env:MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT -ErrorAction SilentlyContinue }

[private]
[unix]
_python-tests:
    "{{ python_executable }}" -m pytest "{{ python_project }}/tests"

[private]
[windows]
_python-tests:
    & "{{ python_executable }}" -m pytest "{{ python_project }}/tests"

# Prepare frozen frontend, Python, Rust, and the verified development model runtime.
setup: _tools _python-setup
    just _frontend setup
    cargo fetch --locked
    just _ensure-model-runtime

# Rebuild the verified development model runtime after Rust, Python, or model changes.
refresh-model-runtime: _python-setup
    just _refresh-model-runtime

# Verify and reuse the current development model runtime without rebuilding it.
verify-model-runtime:
    "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --verify-development-runtime

# Run the complete desktop product with Vite hot reload and a verified reusable service runtime.
dev: _ensure-model-runtime _build-development-service-runtime
    just _frontend dev

# Run the shared application service in the foreground against development data.
[unix]
dev-service: _ensure-model-runtime _build-development-service-runtime
    #!/usr/bin/env bash
    set -euo pipefail
    exec "{{ root }}/target/debug/market-squawk-service" --data-dir "{{ development_data }}" --installation-data-root "{{ development_installation }}" --training-release-root "{{ development_model_release }}"

# Run the shared application service in the foreground against development data.
[windows]
dev-service: _ensure-model-runtime _build-development-service-runtime
    & "{{ root }}/target/debug/market-squawk-service.exe" --data-dir "{{ development_data }}" --installation-data-root "{{ development_installation }}" --training-release-root "{{ development_model_release }}"; exit $LASTEXITCODE

# Run only the Vite frontend for visual diagnostics; this is not the complete product.
dev-web:
    just _frontend dev-web

# Report the pinned frontend toolchain and Tauri host prerequisites.
doctor: _tools
    just _frontend doctor

# Check Rust formatting and compilation plus frontend types.
check:
    cargo fmt --all -- --check
    cargo check --workspace --all-targets --all-features --locked
    just _frontend typecheck

# Run the existing critical application, frontend, and Python tests.
test: _python-tests
    cargo test --package market-squawk --all-features --locked
    just _frontend test

# Run one focused Rust package test suite.
test-package package:
    cargo test --package "{{ package }}" --all-features --locked

# Run the complete deterministic Rust, frontend, and Python suites.
test-all: _python-tests
    cargo test --workspace --all-features --locked --no-fail-fast
    just _frontend test

# Produce a local production-profile Rust build and compiled frontend without packaging.
build:
    cargo build --workspace --all-features --release --locked
    just _frontend build

# Remove the ignored development workspace and service state after explicit confirmation.
[confirm("The desktop and shared service must be stopped. Remove the development workspace-data and service-authority roots?")]
reset-dev:
    node -e "require('node:fs').rmSync(process.argv[1], { recursive: true, force: true })" "{{ development_data }}"
    node -e "require('node:fs').rmSync(process.argv[1], { recursive: true, force: true })" "{{ development_installation }}"

# Remove only the ignored, reproducible development model-runtime cache.
[confirm("The desktop and shared service must be stopped. Remove the verified development model-runtime cache?")]
reset-model-runtime:
    "{{ python_executable }}" -I "{{ python_release_builder }}" --development-runtime-root "{{ development_model_runtime }}" --reset-development-runtime
