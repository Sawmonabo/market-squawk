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

# List supported developer commands.
default:
    @just --list

[private]
_tools:
    @just --version
    @rustc --version
    @cargo --version
    @node --version
    @pnpm --version
    @uv --version

[private]
_build-dev-siblings:
    cargo build --locked --package market-squawk --bins
    cargo build --locked --package market-squawk-modeling --features onnx-tract --bin market-squawk-onnx-worker

[private]
[unix]
_python-tests:
    "{{ python_executable }}" -m pytest "{{ python_project }}/tests"

[private]
[windows]
_python-tests:
    & "{{ python_executable }}" -m pytest "{{ python_project }}/tests"

# Prepare frozen frontend, Python, and Rust development inputs.
setup: _tools
    pnpm --dir "{{ desktop }}" install --frozen-lockfile
    uv --directory "{{ python_project }}" python install 3.14.6
    uv --directory "{{ python_project }}" venv --python 3.14.6 .venv
    uv --directory "{{ python_project }}" pip sync --python "{{ python_executable }}" --require-hashes --strict "{{ python_requirements }}"
    cargo fetch --locked

# Run the complete desktop product with Vite hot reload and isolated development data.
dev: _build-dev-siblings
    @echo "Market Squawk development data: {{ development_data }}"
    pnpm --dir "{{ desktop }}" tauri dev -- -- --data-dir "{{ development_data }}"

# Run the shared application service in the foreground against development data.
dev-service:
    cargo run --locked --package market-squawk --bin market-squawk-service -- --data-dir "{{ development_data }}"

# Run only the Vite frontend for visual diagnostics; this is not the complete product.
dev-web:
    pnpm --dir "{{ desktop }}" dev

# Report active tools and Tauri host prerequisites without changing them.
doctor: _tools
    pnpm --dir "{{ desktop }}" tauri info

# Check Rust formatting and compilation plus frontend types.
check:
    cargo fmt --all -- --check
    cargo check --workspace --all-targets --all-features --locked
    pnpm --dir "{{ desktop }}" typecheck

# Run the existing critical application, frontend, and Python tests.
test: _python-tests
    cargo test --package market-squawk --all-features --locked
    pnpm --dir "{{ desktop }}" test --run

# Run one focused Rust package test suite.
test-package package:
    cargo test --package "{{ package }}" --all-features --locked

# Run the complete deterministic Rust, frontend, and Python suites.
test-all: _python-tests
    cargo test --workspace --all-features --locked --no-fail-fast
    pnpm --dir "{{ desktop }}" test --run

# Produce a local production-profile Rust build and compiled frontend without packaging.
build:
    cargo build --workspace --all-features --release --locked
    pnpm --dir "{{ desktop }}" build

# Remove only ignored Market Squawk development data after explicit confirmation.
[confirm("The desktop and shared service must be stopped. Remove all ignored Market Squawk development data?")]
reset-dev:
    node -e "require('node:fs').rmSync(process.argv[1], { recursive: true, force: true })" "{{ development_data }}"
