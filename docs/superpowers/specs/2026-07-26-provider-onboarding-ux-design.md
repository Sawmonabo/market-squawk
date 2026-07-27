# Market Squawk Provider Onboarding UX Design

Date: 2026-07-26  
Status: Approved design  
Audience: Product, engineering, security, and release reviewers

## Purpose

Replace the provider portal's internal-console presentation with a modern, dark, welcoming setup
experience that a person without finance or market-data knowledge can complete confidently.

The redesign changes presentation and guidance, not provider authority, credential handling,
release admission, or the local-only trust boundary.

## Current problem

The current portal renders unstyled semantic HTML and creates provider controls directly from raw
profile fields. It presents every provider at once, exposes specialist terminology, requires users
to understand provider selection before receiving guidance, and writes raw JSON into the primary
status area.

Consequences:

- there is no clear starting action or recommended path;
- provider benefits and account requirements are difficult to understand;
- complex configuration, especially BLS series metadata, is exposed too early;
- success, warnings, and errors are not visually or linguistically distinguished;
- the interface looks like an internal diagnostic tool rather than a finished product.

## Product principles

1. **Guide first, configure second.** The normal path explains the goal and presents one decision
   or action at a time.
2. **Use plain language.** Internal identifiers, rights-state enums, capability revisions, and
   evidence digests stay out of the primary flow.
3. **Keep expert control.** The complete provider-by-provider interface remains available under
   Advanced setup.
4. **Make free access obvious.** Every provider step states whether it needs no account, a free
   account, or a provider-issued API key.
5. **Never hide a real human boundary.** Provider-controlled login, consent, key creation, or
   verification remains an explicit official-provider step.
6. **Preserve local trust.** No remote assets, analytics, telemetry, embedded provider pages, or
   new hosted service are introduced.

## Chosen interaction model

The portal opens on a welcome screen with:

- the heading **Connect your free data sources**;
- a short explanation that Market Squawk stores configuration locally;
- one primary action, **Set up recommended sources**;
- one secondary action, **Choose sources myself**;
- a compact privacy note explaining that credentials go directly to the local Market Squawk
  process and are not displayed after submission.

### Recommended setup

The recommended path is a step-by-step wizard:

1. **Choose goals**
   - Live markets
   - Economic data and interest rates
   - Company filings and fundamentals
   - Portfolio research
   - Everything recommended
2. **Review the plan**
   - Show the selected providers, what each contributes, and whether an account or key is needed.
3. **Connect providers**
   - Present one provider at a time with one primary action.
   - Preserve progress if an official provider page must be opened.
   - Resume the exact durable onboarding session when the user returns.
4. **Confirm readiness**
   - Summarize connected, attention-needed, and unavailable providers.
   - Explain in plain language what Market Squawk can now do.
   - Offer **Finish setup** and **Review advanced settings**.

The wizard must never imply that a provider is connected until the existing onboarding and adapter
activation authorities report success.

### Provider step

Each provider step contains:

- friendly name and recognizable text mark;
- one-sentence purpose;
- two or three concrete data examples;
- a badge for **No account**, **Free account**, or **API key required**;
- expected user effort such as **Automatic** or **About 2 minutes**;
- a concise numbered procedure;
- one primary button;
- a back action that does not destroy the durable session;
- an expandable **Why this source?** explanation;
- an expandable **Technical details** section for coverage, quality, rights duties, revision,
  evidence, and provider identifiers.

Provider-facing copy uses these meanings:

| Provider | Primary explanation |
| --- | --- |
| Coinbase | Live cryptocurrency trades and order books |
| Kraken | A second live cryptocurrency market for comparison and resilience |
| SEC EDGAR | Company filings, statements, and reported facts |
| FRED and ALFRED | Economic indicators and their historical revisions |
| BLS | Employment, inflation, pay, and labor-market statistics |
| U.S. Treasury | Interest rates, yield curves, and federal financial data |

### Configuration disclosure

The normal path supplies safe, code-owned starter choices where the provider requires specialist
configuration:

- BLS starts with named beginner-friendly groups rather than six raw metadata fields.
- Treasury uses a clearly described default date range that can be changed.
- FRED/ALFRED offers named starter series or a clearly labelled custom-series path.
- Custom identifiers, raw metadata, date bounds, page sizes, and evidence details live under
  Advanced options.

Starter choices must resolve to the same typed provider activation requests as custom choices.
They must not infer or invent provider metadata.

### Credentials

Credential steps separate provider work from local Market Squawk work:

1. Open the exact official provider page.
2. Complete provider-controlled account or key creation.
3. Return and paste the requested value into a labelled, write-only field.
4. Let Market Squawk verify and activate the provider.

The interface states that submitted credentials are stored through the existing OS-keyring-first
route or encrypted local fallback. It never displays, echoes, logs, caches in browser storage, or
serializes a submitted secret into normal page state.

## Visual system

The portal is dark by default and uses no light-mode version in V1.

### Palette

- Page background: near-black navy
- Raised surfaces: layered charcoal/navy
- Primary accent: restrained cyan/teal
- Success: accessible green
- Warning: warm amber
- Error: accessible coral/red
- Primary text: soft near-white
- Secondary text: cool gray with WCAG AA contrast

Color is never the only carrier of status.

### Layout and typography

- System font stack only; no remote fonts.
- Centered application shell with a readable maximum width.
- Persistent desktop progress rail and compact mobile progress header.
- Spacious panels with restrained radius, border, and shadow.
- Clear heading scale, short line lengths, and consistent vertical rhythm.
- One visually dominant action per screen.
- Responsive from narrow mobile width through desktop.
- Motion is brief and functional and is disabled under `prefers-reduced-motion`.

### Accessibility

- Semantic landmarks, headings, lists, forms, labels, and buttons.
- Keyboard-complete navigation and visible focus indicators.
- Error text linked to its field.
- Polite live-region announcements for progress and success; assertive announcements only for
  blocking errors.
- Minimum practical 44-pixel pointer targets.
- No placeholder-only labels.
- Screen-reader text for icons and status badges.

## Status and error design

Raw JSON is removed from the primary experience.

The normal status system uses:

- inline field guidance for validation errors;
- a page-level alert for failed provider or storage operations;
- a progress state for network verification;
- a confirmation panel for successful activation;
- a recovery action when the session can resume;
- a technical-details disclosure containing the redacted machine response when useful.

Errors describe what happened, whether progress was preserved, and the next safe action. They never
include credentials, raw provider payloads, internal filesystem paths, or unrestricted debugging
details.

## Implementation architecture

The loopback server, API routes, session security, CSRF validation, CSP, bounded request handling,
and provider services remain in `provider_onboarding/portal.rs`.

Presentation assets move out of the Rust source string:

```text
apps/market-squawk/assets/provider-onboarding/
├── index.html
├── portal.css
└── portal.js
```

Rust embeds these files at compile time with `include_str!`; there is no runtime filesystem access.
The existing `/portal.js` route remains, and a same-origin `/portal.css` route is added. CSP is
extended only with `style-src 'self'`; inline scripts, inline styles, remote sources, frames, form
posts, and external connections remain denied.

The JavaScript is organized into focused native modules within the single embedded file:

- immutable presentation metadata and beginner copy;
- API transport and redacted error mapping;
- application state and durable-session reconciliation;
- router/step navigation;
- provider-plan selection;
- provider forms and starter choices;
- status, progress, and technical-details rendering.

No frontend framework, package manager, bundler, remote dependency, browser persistence layer, or
second provider business-logic implementation is added.

## Verification

Verification stays thin and release-relevant:

1. Reuse the existing portal integration path to verify bootstrap, mutation security, credential
   submission, activation, resume, and cancellation remain functional.
2. Run JavaScript syntax validation against the embedded asset.
3. Perform one keyboard and screen-reader-oriented accessibility inspection.
4. Perform one real browser walkthrough at mobile and desktop widths covering:
   - recommended no-credential provider;
   - provider API-key handoff;
   - recoverable error;
   - resumed active session;
   - advanced configuration.
5. Run focused Rust formatting, compile, and existing portal tests.

No snapshot suite, prose test, screenshot-golden test, new integration-test executable, browser
automation framework, or broad workspace gate is introduced for this redesign.

## Acceptance criteria

The redesign is complete when:

- a new user can identify the recommended first action without documentation;
- the recommended flow explains each selected source before requesting configuration;
- normal setup exposes no unexplained internal identifier or raw JSON;
- every provider state has a clear next action or truthful terminal explanation;
- credentials retain the existing secure, write-only lifecycle;
- starter choices produce typed existing activation requests;
- advanced controls preserve full provider capability;
- the interface is responsive, keyboard usable, readable in dark mode, and locally self-contained;
- existing portal security and provider activation behavior remains green;
- the final V1 clean-machine demonstration completes through the redesigned portal.

## Non-goals

- A remotely hosted portal
- A general Market Squawk dashboard
- Trading or research visualization
- Live-money execution controls
- Provider-page embedding
- Automatic completion of provider-controlled human actions
- A new credential, HTTP, catalog, or adapter stack
- A frontend framework or design-system dependency
