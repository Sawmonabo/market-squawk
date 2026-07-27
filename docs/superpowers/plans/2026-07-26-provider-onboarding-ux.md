# Provider Onboarding UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unstyled provider console with a dark, guided, beginner-friendly local setup
experience without changing its security or provider-authority boundaries.

**Architecture:** Keep the Rust loopback server and typed application services authoritative.
Compile three static assets into the binary, serve them through the existing bounded same-origin
portal, and make native JavaScript render a recommended wizard plus an advanced provider view.
Provider forms translate into existing typed activation requests; FRED gains a portal-specific
exact-series grant request while the API key remains in the existing write-only secret path.

**Tech Stack:** Rust 1.97, Hyper HTTP/1 loopback server, compile-time `include_str!`, semantic HTML,
native CSS, and dependency-free browser JavaScript.

## Global Constraints

- Dark mode is the V1 default and only theme.
- Primary copy must be understandable without finance, trading, provider, or software expertise.
- No remote fonts, scripts, analytics, telemetry, frontend framework, package manager, bundler,
  browser storage, or hosted service.
- No inline script or style; CSP adds only `style-src 'self'`.
- Provider login, consent, key creation, and account actions remain on exact official provider
  pages.
- Secret submission remains write-only through the existing OS-keyring-first/encrypted-fallback
  service.
- Raw JSON and internal identifiers appear only inside an explicit Technical details disclosure.
- The existing provider state machine, evidence, rights, rate, activation, and recovery authorities
  remain authoritative.
- No new test executable, snapshot suite, prose test, screenshot-golden test, or browser automation
  framework.
- Use `CARGO_INCREMENTAL=0`, one Cargo process at a time, and stop before the root target reaches
  20 GiB.

---

### Task 1: Extract and securely serve compile-time portal assets

**Files:**

- Create: `apps/market-squawk/assets/provider-onboarding/index.html`
- Create: `apps/market-squawk/assets/provider-onboarding/portal.css`
- Create: `apps/market-squawk/assets/provider-onboarding/portal.js`
- Modify: `apps/market-squawk/src/provider_onboarding/portal.rs:410-432, 790-817, 977-1247`

**Interfaces:**

- Consumes: the existing `/api/v1/bootstrap` and mutation routes.
- Produces: same-origin `/`, `/portal.css`, and `/portal.js` assets embedded in the application
  executable.

- [ ] **Step 1: Confirm the current missing style route**

Run the already-built portal against a temporary data root and request `/portal.css`.

Expected: `404` before the implementation.

- [ ] **Step 2: Replace Rust string assets with compile-time files**

Use package-relative compile-time embedding:

```rust
const INDEX_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/provider-onboarding/index.html"
));
const PORTAL_CSS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/provider-onboarding/portal.css"
));
const PORTAL_JAVASCRIPT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/provider-onboarding/portal.js"
));
```

Delete the prior inline HTML and JavaScript constants.

- [ ] **Step 3: Add the exact stylesheet route**

Add the route beside the existing index and script routes:

```rust
if method == Method::GET && path == "/portal.css" {
    return Ok(text_response(
        StatusCode::OK,
        "text/css; charset=utf-8",
        PORTAL_CSS,
    ));
}
```

- [ ] **Step 4: Preserve CSP while admitting same-origin CSS**

Use this exact policy:

```text
default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'
```

Do not add inline-style, inline-script, image, font, frame, or remote-origin allowances.

- [ ] **Step 5: Create the semantic application shell**

`index.html` contains only static structure and asset links:

```html
<body>
  <a class="skip-link" href="#app-main">Skip to setup</a>
  <div id="app" class="app-shell">
    <header id="app-header" class="app-header"></header>
    <main id="app-main" tabindex="-1"></main>
    <div id="announcer" class="sr-only" aria-live="polite" aria-atomic="true"></div>
  </div>
  <script defer src="/portal.js"></script>
</body>
```

Link `/portal.css` in `<head>`. Include a useful description and dark browser color metadata.

- [ ] **Step 6: Validate the asset boundary without adding tests**

Run:

```bash
node --check apps/market-squawk/assets/provider-onboarding/portal.js
git diff --check
```

Expected: both exit `0`; no remote URL exists in CSS or JavaScript.

### Task 2: Implement the dark guided setup application

**Files:**

- Modify: `apps/market-squawk/assets/provider-onboarding/portal.css`
- Modify: `apps/market-squawk/assets/provider-onboarding/portal.js`

**Interfaces:**

- Consumes: `BootstrapResponse` fields `profiles`, `sessions`, and
  `encrypted_file_fallback`.
- Produces: welcome, goal selection, plan review, one-provider-at-a-time setup, completion, and
  advanced-provider views.

- [ ] **Step 1: Define the dark visual tokens**

Begin `:root` with fixed local tokens:

```css
:root {
  color-scheme: dark;
  --page: #080d18;
  --page-glow: #0c1830;
  --surface: #111a2a;
  --surface-raised: #172236;
  --border: #263650;
  --text: #f3f7fc;
  --muted: #aab8ca;
  --accent: #38d6c7;
  --accent-strong: #65e8dc;
  --success: #55d98b;
  --warning: #f0b95b;
  --danger: #ff7b86;
  --focus: #8de8ff;
  --radius-sm: 0.625rem;
  --radius-lg: 1.25rem;
  --shadow: 0 1.5rem 4rem rgb(0 0 0 / 0.32);
}
```

Use a system font stack, visible `:focus-visible`, 44-pixel minimum controls, responsive grid,
reduced-motion handling, and no color-only status.

- [ ] **Step 2: Define the closed client state**

Use one in-memory state object:

```javascript
const state = {
  csrf: '',
  profiles: [],
  sessions: new Map(),
  fallback: 'disabled',
  route: 'welcome',
  goals: new Set(),
  plan: [],
  activeIndex: 0,
  busy: false,
  notice: null,
  technical: null
};
```

No state is written to cookies, `localStorage`, `sessionStorage`, IndexedDB, URL parameters, or
browser history. Durable resume continues to come only from `/api/v1/bootstrap`.

- [ ] **Step 3: Implement one rendering boundary**

Implement and use these functions:

```javascript
function render() {}
function renderWelcome() {}
function renderGoalSelection() {}
function renderPlanReview() {}
function renderProviderStep(profile) {}
function renderCompletion() {}
function renderAdvanced() {}
function renderNotice() {}
function announce(message) {}
```

Every transition updates `state.route`, calls `render()`, and focuses the new page heading.

- [ ] **Step 4: Add plain-language provider presentation**

Define a closed `PROVIDER_COPY` map keyed by the exact provider profile IDs. Each entry supplies:

- friendly name;
- one-sentence purpose;
- two or three examples;
- goal categories;
- user-effort label;
- account/key explanation.

Unknown future profiles remain visible in Advanced setup using their server display name, but they
are never silently added to the recommended plan.

- [ ] **Step 5: Build the recommended plan**

Goal selection maps only to code-owned provider IDs. “Everything recommended” selects:

```text
coinbase.public-market-data
kraken.spot-public-market-data
sec.edgar-public
fred-alfred.api-v1-v2
bls.v1-unregistered
treasury.daily-rates-xml
treasury.fiscal-data
local.files
local.portfolio-imports
local.paper-execution
```

If two profiles represent the same provider tier, prefer the zero-credential tier unless the user
explicitly selects the credentialed tier in Advanced setup.

- [ ] **Step 6: Replace raw JSON status with typed presentation**

`mutate()` returns the parsed response but never writes it directly to the page. Map known API error
codes into a concise title, explanation, preservation statement, and next action. Place the
redacted machine response only in a collapsed `<details>` element labelled Technical details.

- [ ] **Step 7: Preserve expert capability**

Advanced setup lists all profiles with status, coverage, account/key need, active session action,
official page, configuration, renew, cleanup, and remove-local-authority controls. Put capability
revision, evidence digest, rights duties, quality ceiling, and provider ID inside Technical details.

### Task 3: Make specialist provider configuration beginner-safe

**Files:**

- Modify: `apps/market-squawk/src/provider_onboarding/contracts.rs`
- Modify: `apps/market-squawk/src/local_product/cli_provider.rs`
- Modify: `apps/market-squawk/assets/provider-onboarding/portal.js`
- Modify existing assertions in:
  `apps/market-squawk/tests/research_vertical.rs`

**Interfaces:**

- Consumes: active onboarding leases, existing write-only API-key submission, code-owned FRED terms
  evidence, and typed adapter activation services.
- Produces: beginner presets and the portal-only FRED exact-series activation request.

- [ ] **Step 1: Add the typed FRED portal contract**

Add:

```rust
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FredSeriesRightsBasis {
    PublicDomain,
    OwnerPermission,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FredSeriesGrantInput {
    pub series: SourceIdentifier,
    pub owner: SourceIdentifier,
    pub rights_basis: FredSeriesRightsBasis,
    pub evidence_url: String,
    pub authorization_document: Option<String>,
}
```

Extend `ProviderPortalActivationRequest`:

```rust
Fred {
    series: Vec<FredSeriesGrantInput>,
}
```

The backend rejects an empty list, duplicate series, invalid HTTPS URLs, public-domain inputs with
an authorization document, owner-permission inputs without one, documents above the existing
bounded portal body policy, and unsupported fields.

- [ ] **Step 2: Translate the FRED portal request into internal authority**

In `portal_provider_request`, convert each portal input into the existing exact-series
`FredSeriesRightsGrant` path. The allowed operation set is fixed in code to local
retrieve/display/persist/cache/archive/train. It does not expose redistribution or commercial-use
switches.

Use the current code-owned FRED terms/profile evidence for the terms bundle; do not ask a novice to
locate or upload API terms, service terms, privacy-policy files, or a rights-artifact JSON file.
Compute every submitted owner-authorization digest in the backend. Keep the existing advanced CLI
file-reference request for recovery/audit compatibility.

- [ ] **Step 3: Add a lawful named FRED starter**

The recommended FRED/ALFRED option starts with:

```javascript
{
  series: 'UNRATE',
  display_name: 'U.S. unemployment rate',
  owner: 'us-bureau-of-labor-statistics',
  rights_basis: 'public_domain',
  evidence_url: 'https://www.bls.gov/opub/copyright-information.htm',
  authorization_document: null
}
```

The official FRED series metadata identifies `UNRATE` as the monthly, seasonally adjusted
Unemployment Rate sourced from the U.S. Bureau of Labor Statistics at
`https://fred.stlouisfed.org/series/UNRATE`. The BLS copyright page states that BLS electronic
publications are public domain and asks for source citation. Preserve both the FRED series citation
and BLS source identity in the resulting grant and dataset provenance.

Custom exact-series setup remains available and requires the user to provide the series owner,
rights basis, and authoritative evidence. An owner-permission input additionally requires the
bounded authorization document.

- [ ] **Step 4: Add named BLS starter configuration**

The recommended BLS option is **U.S. unemployment rate** and emits the existing verified metadata:

```javascript
{
  series_id: 'LNS14000000',
  title: 'Unemployment Rate',
  unit: 'percent',
  frequency: 'monthly',
  seasonal_adjustment: 'seasonally-adjusted',
  measure: 'unemployment-rate'
}
```

The normal form asks only for a start and end year. Existing six-field custom series entry remains
under Advanced options.

- [ ] **Step 5: Add novice Treasury defaults**

Daily rates defaults to the most recent five complete years through the current UTC year. Fiscal
Data defaults to the prior twelve months with page size `1000`. Show the date range before
activation and allow it to be changed.

- [ ] **Step 6: Add local-capability explanations**

`local.files`, `local.portfolio-imports`, and `local.paper-execution` explain that no online account
or API key is needed. Their primary action activates the existing local profile or shows the exact
next local CLI action when no portal activation request is required.

- [ ] **Step 7: Reuse the existing portal harness**

Extend the existing
`provider_portal_rejects_csrf_and_keeps_imported_secrets_write_only` flow only where necessary to
prove that the new asset routes retain no-store/CSP headers and that the typed FRED request does not
contain the API key. Do not add a test target, snapshot, prose assertion, or visual assertion.

### Task 4: Verify, review, integrate, and publish the portal

**Files:**

- Modify if behavior changed: `docs/operations/source-operations.md`
- Modify: `docs/plans/delivery-ledger.md`
- Modify: `docs/project-memory.md`

**Interfaces:**

- Consumes: Tasks 1-3 and the current integrated provider fixes.
- Produces: one reviewed portal commit and V1 clean-machine acceptance input.

- [ ] **Step 1: Reclaim generated build state before compiling**

Confirm no Cargo/rustc process exists, record `du -sh target`, run `cargo clean`, and retain no
generated worktree target. Source and uncommitted changes must remain intact.

- [ ] **Step 2: Run focused automated verification**

```bash
node --check apps/market-squawk/assets/provider-onboarding/portal.js
CARGO_INCREMENTAL=0 cargo test -p market-squawk \
  --test research_vertical --all-features --locked \
  provider_portal_rejects_csrf_and_keeps_imported_secrets_write_only
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk \
  --lib --bin market-squawk --all-features --locked -- -D warnings
cargo fmt --all --check
git diff --check
```

Expected: every command exits `0`; target remains below 20 GiB.

- [ ] **Step 3: Perform one real browser walkthrough**

Against a temporary data root, inspect desktop and narrow-mobile widths and complete:

1. recommended no-credential setup;
2. API-key provider handoff without storing a value in the DOM after submit;
3. one recoverable error;
4. restart/resume of an active session;
5. Advanced setup.

Verify keyboard order, visible focus, readable contrast, reduced-motion behavior, friendly status
copy, no raw JSON in the normal flow, and no network request outside loopback except an explicit
official-provider link.

- [ ] **Step 4: Review the exact diff once**

Review security headers, secret flow, provider request typing, beginner copy, responsive CSS, and
all changed provider actions. Fix only concrete correctness, security, accessibility, or usability
defects; do not start a cosmetic review loop.

- [ ] **Step 5: Commit and push exact paths**

Stage only the portal assets, portal transport/contracts, required provider activation path,
existing focused assertion, and truthful maintained documentation. Inspect the staged diff, commit:

```bash
git commit -m "feat(onboarding): add guided dark provider setup"
git push origin release/market-squawk-v0.1.0
```

Update issue `#31`, PR `#26`, Project 5, the delivery ledger, and project memory with the exact
commit and focused evidence.
