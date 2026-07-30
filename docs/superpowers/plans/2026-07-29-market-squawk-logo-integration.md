# Market Squawk Logo Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement
> this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock the user-approved no-eye S-bird logo as Market Squawk's source-controlled brand,
replace the desktop shell's generic waveform identity with the approved integrated wordmark, and
regenerate the bundled application icons without changing product authority or behavior.

**Architecture:** Preserve the exact approved PNG as the immutable visual source under the design
specification assets. Define one flat, source-owned SVG mark for runtime use, compose the visible
`Market` + S-bird + `quawk` lockup in the existing sidebar, and derive the platform icon family
from a square Obsidian application-icon SVG through Tauri's maintained icon generator. The raster
approval source is evidence of design intent; the SVGs are the production translations.

**Tech Stack:** SVG, React 19, TypeScript, Tailwind CSS 4, Vite 8, Vitest, Tauri CLI 2.11.4, pnpm,
and the existing Geist/Obsidian Signal visual system.

## Global constraints

- Audit base: `968631aba5f71c85341876f905795eb71065ba61`.
- Approved source:
  `/Users/sawmonabo/.codex/generated_images/019faf47-1c0c-7791-818c-b9c174e0d92b/option-3-selected-no-eye-final.png`.
- Approved source SHA-256:
  `567a3dc04acb791b67eb2a0bb5eed256cb6c9d84c0441ed2dcefc7ae9b7d6ee6`.
- This audit base identifies the inspected repository state; it is not implementation evidence or
  release approval.
- Refresh gate before this plan was written: require clean `feature/complete-installer` at the
  audit base, the approved source at the recorded digest, the existing `app-sidebar.tsx` brand
  surface, the existing Tauri icon configuration, and the 3/3-test green frontend baseline. That
  gate passed on 2026-07-29. Before Task 1, the plan file itself must be the only worktree change.
- Refresh again before commit if the branch, source digest, sidebar ownership, Tauri icon list, or
  package lock changes.
- Work only in the existing isolated lane at
  `/Users/sawmonabo/dev/market-squawk/.worktrees/complete-installer`; do not create a competing
  desktop worktree.
- Do not add a dependency, network asset, gradient, glow, eye, generic bird, generic waveform,
  screenshot-golden test, new frontend test file, or fourth frontend test.
- Keep the existing near-black, graphite, white, and `#155dfc` cobalt system. The bird body forms
  the capital S, faces right, and has an open beak; the three cobalt wing tiers carry the market
  motif.
- The horizontal lockup is `Market` + S-bird + `quawk`. Do not render a separate mark beside the
  full words `Market Squawk`.
- Preserve the protected browser, CLI, Rust command, provider, installation, release, and
  application-authority boundaries unchanged.
- The rejected generated previews may be cleaned up only after the approved source is committed,
  pushed, hash-verified in the repository, and the generated folder is re-inventoried. Cleanup is
  a recoverable move to macOS Trash, never a recursive deletion.
- Focused verification is scoped integration evidence only. It is not exact-head release-gate,
  signing, notarization, hosted-platform, or publication approval.

---

## Task 1: Lock the approved logo contract and visual source

**Files:**

- Create:
  `docs/superpowers/specs/assets/2026-07-29-market-squawk-logo-option-3-no-eye.png`
- Create: `docs/superpowers/specs/2026-07-29-market-squawk-logo-design.md`
- Modify:
  `docs/superpowers/specs/2026-07-28-market-squawk-obsidian-signal-interface-design.md`

- [ ] Re-run the refresh gate:

```bash
test "$(git branch --show-current)" = "feature/complete-installer"
test "$(git rev-parse HEAD)" = "968631aba5f71c85341876f905795eb71065ba61"
test "$(git status --porcelain)" = \
  "?? docs/superpowers/plans/2026-07-29-market-squawk-logo-integration.md"
shasum -a 256 \
  /Users/sawmonabo/.codex/generated_images/019faf47-1c0c-7791-818c-b9c174e0d92b/option-3-selected-no-eye-final.png
pnpm --dir apps/market-squawk-desktop test --run
```

- [ ] Copy the exact approved PNG into the specification asset path. Keep the generated source in
      place until final cleanup so a failed repository operation cannot lose the master.
- [ ] Write the logo specification with document control, source digest, supersession boundary,
      visual anatomy, exact colors, lockups, clear-space/minimum-size rules, accessibility rules,
      app-icon treatment, prohibited variants, and acceptance criteria.
- [ ] Amend the Obsidian Signal interface specification only to point logo identity at the new
      approved specification. Keep the existing signal motif for non-logo product contexts.
- [ ] Verify the repository copy is byte-identical:

```bash
shasum -a 256 \
  docs/superpowers/specs/assets/2026-07-29-market-squawk-logo-option-3-no-eye.png
cmp \
  /Users/sawmonabo/.codex/generated_images/019faf47-1c0c-7791-818c-b9c174e0d92b/option-3-selected-no-eye-final.png \
  docs/superpowers/specs/assets/2026-07-29-market-squawk-logo-option-3-no-eye.png
```

---

## Task 2: Replace the generic workspace identity with the approved lockup

**Files:**

- Create: `apps/market-squawk-desktop/src/assets/market-squawk-mark.svg`
- Modify: `apps/market-squawk-desktop/src/components/app-sidebar.tsx`
- Modify: `apps/market-squawk-desktop/src/test/app.test.tsx`
- Modify: `apps/market-squawk-desktop/index.html`

- [ ] Extend the existing accessible-navigation test, without adding a test case, to require the
      workspace-home link to expose the separated visible `Market` and `quawk` text around its
      source-owned mark. The break it catches is a regression to the generic waveform or a
      detached mark plus full product name.
- [ ] Run the focused test and confirm RED because exact visible `quawk` does not yet exist:

```bash
pnpm --dir apps/market-squawk-desktop test --run \
  -t "uses accessible product navigation"
```

- [ ] Create the flat transparent SVG mark with a three-tier cobalt market wing and a white,
      no-eye, open-beak bird whose body is the S.
- [ ] Import the SVG into `app-sidebar.tsx`. Replace `AudioWaveform` and the detached
      `Market Squawk` label with one tight lockup: white `Market`, the mark as S, and cobalt
      `quawk`. Keep the workspace link's accessible name and make the image decorative within
      that already-named link.
- [ ] Preserve icon-collapse behavior by hiding only the word fragments and retaining the mark at
      a legible compact size. Remove the unrelated workspace dropdown affordance; do not alter the
      local-system footer.
- [ ] Add the bundled SVG as the browser/WebView favicon in `index.html`.
- [ ] Run the focused test and confirm GREEN:

```bash
pnpm --dir apps/market-squawk-desktop test --run \
  -t "uses accessible product navigation"
```

---

## Task 3: Regenerate the desktop application-icon family

**Files:**

- Modify: `apps/market-squawk-desktop/src-tauri/icons/app-icon.svg`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/32x32.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/128x128.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/128x128@2x.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/icon.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/icon.icns`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/icon.ico`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square30x30Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square44x44Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square71x71Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square89x89Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square107x107Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square142x142Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square150x150Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square284x284Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/Square310x310Logo.png`
- Regenerate: `apps/market-squawk-desktop/src-tauri/icons/StoreLogo.png`

- [ ] Replace the generic waveform source with a square Obsidian icon: near-black rounded platform
      container, centered approved S-bird mark, flat cobalt/white fills, and sufficient internal
      padding for 16-pixel legibility.
- [ ] Generate the maintained platform icon family from that SVG:

```bash
pnpm --dir apps/market-squawk-desktop tauri icon \
  src-tauri/icons/app-icon.svg \
  --output src-tauri/icons
```

- [ ] Confirm Tauri generated only the expected tracked icon set and that required bundle inputs
      remain present:

```bash
git status --short apps/market-squawk-desktop/src-tauri/icons
file apps/market-squawk-desktop/src-tauri/icons/32x32.png \
  apps/market-squawk-desktop/src-tauri/icons/128x128.png \
  apps/market-squawk-desktop/src-tauri/icons/128x128@2x.png \
  apps/market-squawk-desktop/src-tauri/icons/icon.png \
  apps/market-squawk-desktop/src-tauri/icons/icon.icns \
  apps/market-squawk-desktop/src-tauri/icons/icon.ico
```

- [ ] Visually inspect `icon.png` and the compact `32x32.png`. If the bird-S or open beak does not
      survive at the compact size, adjust only padding/path geometry and regenerate the full set.

---

## Task 4: Verify and freeze the scoped candidate

**Files:**

- Modify only files named by Tasks 1–3.

- [ ] Review `git diff --stat`, `git diff --check`, and the full textual diff. Reject any
      dependency, lockfile, authority, bundle-configuration, or unrelated UI change.
- [ ] Run the complete frontend gates:

```bash
pnpm --dir apps/market-squawk-desktop test --run
pnpm --dir apps/market-squawk-desktop typecheck
pnpm --dir apps/market-squawk-desktop build
```

- [ ] Confirm the Tauri crate still compiles with the regenerated bundle inputs:

```bash
cargo check -p market-squawk-desktop --locked
```

- [ ] Recompute and record the approved PNG digest, inspect the generated icon metadata, and verify
      all HTML/SVG assets are local.
- [ ] Commit the bounded repository candidate:

```bash
git add \
  docs/superpowers/plans/2026-07-29-market-squawk-logo-integration.md \
  docs/superpowers/specs/2026-07-29-market-squawk-logo-design.md \
  docs/superpowers/specs/2026-07-28-market-squawk-obsidian-signal-interface-design.md \
  docs/superpowers/specs/assets/2026-07-29-market-squawk-logo-option-3-no-eye.png \
  apps/market-squawk-desktop/index.html \
  apps/market-squawk-desktop/src/assets/market-squawk-mark.svg \
  apps/market-squawk-desktop/src/components/app-sidebar.tsx \
  apps/market-squawk-desktop/src/test/app.test.tsx \
  apps/market-squawk-desktop/src-tauri/icons
git commit -m "feat(brand): adopt approved Market Squawk logo"
```

- [ ] On the clean committed head, rerun the frontend tests, type check, production build, and
      locked desktop compile. Then capture exact head/tree/status evidence and push that unchanged
      head to `origin/feature/complete-installer`.

---

## Task 5: Clean up rejected previews recoverably

**External files:**

- Source directory:
  `/Users/sawmonabo/.codex/generated_images/019faf47-1c0c-7791-818c-b9c174e0d92b`
- Destination:
  `/Users/sawmonabo/.Trash/market-squawk-logo-rejected-2026-07-29`

- [ ] Confirm the pushed repository asset still has SHA-256
      `567a3dc04acb791b67eb2a0bb5eed256cb6c9d84c0441ed2dcefc7ae9b7d6ee6`.
- [ ] Re-inventory the exact generated directory and confirm it still contains only the 36 logo
      design/inspection files recorded at planning time.
- [ ] Create the explicit Trash destination, move the 35 rejected/inspection files into it, and
      move the original approved generated PNG into a `source-moved-to-repo` subdirectory there.
      The source-controlled byte-identical copy remains the master.
- [ ] Remove the now-empty generated directory with `rmdir` only; do not use `rm -r`, globbed
      deletion, or force.
- [ ] Report the repository asset path, exact commit, pushed branch, verification evidence, Trash
      destination, and recoverability. Do not claim release approval.
