# Portable artifact-reference grammar

Date: 2026-07-17  
Status: implemented integration candidate; hosted Windows verification pending

## Decision

Market Squawk artifact references are canonical application identifiers, not arbitrary host paths.
The accepted grammar is deliberately narrower than any supported host filesystem:

```text
reference  = component *("/" component)
component  = (lowercase-ascii / digit)
             *(lowercase-ascii / digit / "-" / "_" / ".")
```

Each component is between 1 and 255 bytes, does not end in a period, is not a Windows device
basename, and the reference contains at most 32 components. Only `/` separates components.
The complete reference must be UTF-8. Empty components, dot components, parent components,
backslashes, whitespace, controls, uppercase ASCII, non-ASCII text, hidden-name prefixes, and
other punctuation are rejected. Callers receive a typed error; the platform never rewrites an
invalid reference into a valid one.

This grammar is for generated artifact references under the controlled local artifact root. It is
not a general user-file import grammar. Imported user files keep their source identity at the
adapter boundary and receive a separately generated artifact reference when materialized into the
controlled artifact store.

## Why validation precedes `Path` normalization

Rust documents that [`Path::components`] ignores repeated separators, non-leading `.` components,
and trailing separators. On Windows, both `/` and `\\` are path separators. Validating only the
parsed components therefore loses caller-authored syntax before the application can reject it.
For example, these distinct strings can otherwise resolve to the same filesystem object:

```text
reports/result.json
reports//result.json
reports/./result.json
reports/result.json/
```

Similarly, `back\\slash` is one Unix component but two Windows components. That caused the hosted
Windows portability failure which triggered this review.

The implementation consequently:

1. preserves the existing absolute-path error classification;
2. requires the original OS string to be UTF-8;
3. splits and validates the original text using `/` only;
4. compares every accepted raw component with the host `Path::components()` result and rejects any
   additional platform interpretation; and
5. opens the result relative to the retained `cap-std` directory capability.

The last step keeps filesystem access capability-confined. The raw grammar additionally gives
manifests, audit records, cleanup logic, and returned MCP artifact references one portable textual
identity per controlled object.

## Portability and alias rationale

Microsoft's [file and path naming rules] prohibit `<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`,
NUL, and control characters 1 through 31. They also reserve `CON`, `PRN`, `AUX`, `NUL`, `COM1`
through `COM9`, `LPT1` through `LPT9`, and the documented superscript-digit variants, including
when those basenames have extensions. Microsoft further warns applications not to assume case
sensitivity and not to end names in a space or period.

Merely implementing that deny-list would still leave filesystem-dependent aliases and Unicode
normalization differences. Controlled artifacts do not need arbitrary display names, so Market
Squawk instead uses a lowercase ASCII allow-list. Requiring an alphanumeric first character also
prevents hidden-file and command-option-looking names. Human labels and original source filenames
remain metadata rather than storage authority.

The 255-byte component and 32-component depth limits are application bounds. They are checked on
the unnormalized representation, including exact-limit and one-over-limit tests, so accepted
references remain bounded independently of host path configuration.

## Security and compatibility boundaries

- This validation prevents alternate textual identities and cross-platform parsing differences; it
  does not replace capability-relative filesystem access or unsafe-entry checks.
- Existing controlled artifact callers were audited before tightening the grammar. No production
  caller depended on uppercase, whitespace, non-ASCII, or backslash references.
- General local file ingestion must not call this validator on a user-selected source path.
- Rejected references must not be silently transliterated, lowercased, or separator-normalized.
- Windows device-name defenses remain in place even though the ASCII allow-list already rejects the
  documented superscript variants. This is intentional defense in depth if the grammar expands.

## Verification

The integration tests cover raw dot and parent components, repeated and trailing separators,
alternate separators, Windows device names and invalid characters, controls, case, Unicode,
hidden/option-like names, an accepted nested reference, and exact component/depth bounds. Hosted
Windows, macOS, and Linux jobs must all run the same public-boundary test before this candidate is
approved.

## Sources

- Rust 1.97.0, [`std::path`](https://doc.rust-lang.org/1.97.0/std/path/)
- Rust 1.97.0, [`Path::components`](https://doc.rust-lang.org/1.97.0/std/path/struct.Path.html#method.components)
- Microsoft Learn, [Naming Files, Paths, and Namespaces](https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file)
