# Official Documentation Batch 004: OAuth Security and Native Secret Stores


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch covers current OAuth security/revocation standards, Apple Keychain, Windows Credential
Manager/DPAPI, freedesktop Secret Service, and Kraken key-info verification. It establishes portable
contracts while preserving platform differences. It does not execute an OS store or provider call.

## Sources Reviewed

| ID | First-class source | Evidence role |
| --- | --- | --- |
| PAPER-006 | [RFC 9700 / BCP 240](https://www.rfc-editor.org/info/rfc9700/) | Current OAuth security profile |
| PAPER-007 | [RFC 7009](https://www.rfc-editor.org/info/rfc7009/) | Remote token revocation semantics |
| DOC-032 | [Apple Keychain Services](https://developer.apple.com/documentation/security/keychain-services/) | Native secret storage boundary |
| DOC-033 | [Apple update/delete](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items) | Exact item lifecycle |
| DOC-034 | [Apple accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility) | Unlock/device/access policy |
| DOC-035 | [Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management) | Credential identity/persistence |
| DOC-036 | [Windows `CredWriteW`](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew) | Create/replace/delete family |
| DOC-037 | [Windows `CryptProtectData`](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata) | Same-user/machine encryption caveats |
| DOC-038 | [Secret Service API 0.2 DRAFT](https://specifications.freedesktop.org/secret-service/latest-single/) | Sessions, collections, items, prompts, and attributes |
| DOC-014 | [Kraken Get API Key Info](https://docs.kraken.com/api/docs/rest-api/get-api-key-info) | Observed permissions/restrictions verification |

## Findings

1. **Confirmed:** RFC 9700 is a coherent security profile; isolated use of one mechanism does not
   establish a secure provider flow. Deprecated/insecure grants must not be enabled through a
   generic portal setting. [RFC 9700](https://www.rfc-editor.org/rfc/rfc9700)
2. **Confirmed:** token revocation is a provider operation when supported. **Inference:** remote
   revoke, local secret deletion, catalog retirement, and cleanup must be stored as separate facts;
   one cannot stand in for another. [RFC 7009](https://www.rfc-editor.org/rfc/rfc7009)
3. **Confirmed:** Apple, Windows, and Secret Service expose different item selectors, access policy,
   prompt/session behavior, persistence, deletion, and failure modes. A portable interface must
   preserve those typed distinctions rather than claim identical semantics.
   [Apple Keychain](https://developer.apple.com/documentation/security/keychain-services/),
   [Windows credentials](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management),
   [Secret Service](https://specifications.freedesktop.org/secret-service/latest-single/)
4. **Confirmed:** Secret Service attributes are not secret. **Inference:** provider secret bytes,
   authorization codes, device/user codes, PKCE verifiers, and tokens must never be encoded into
   attributes, catalog rows, logs, artifacts, or MCP responses.
5. **Confirmed:** DPAPI and Credential Manager are different Windows surfaces, and machine-wide
   DPAPI protection has a broader decryption boundary. They must not be collapsed into a vague
   “OS encrypted” claim. [DPAPI](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata)
6. **Confirmed:** Kraken key-info exposes observed key authority and restrictions.
   **Inference:** activation records requested versus observed authority, rejects unexpected write/
   funding/withdrawal/earn authority, and redacts provider-returned sensitive fields.
   [Kraken key info](https://docs.kraken.com/api/docs/rest-api/get-api-key-info)

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Release effect |
| --- | --- | --- | --- | --- |
| OAuth security controls form a complete profile | PAPER-006 | Confirmed standard | High | Provider capability must admit the whole profile |
| Remote revoke and local delete are different events | PAPER-007, DOC-033, DOC-036, DOC-038 | Fact plus engineering inference | High | Separate lifecycle states |
| Native stores are not semantically identical | DOC-032 through DOC-038 | Confirmed fact | High | Backend-specific typed behavior and smokes |
| Secret Service attributes are public metadata | DOC-038 | Confirmed fact | High | Secret bytes cannot enter selectors/catalog |
| Kraken observed authority can be verified | DOC-014 | Confirmed fact | High | Exact least-privilege activation gate |

## Limitations and Non-Findings

- No OS-store prompt, lock, headless, deletion, or crash behavior was tested.
- Secret Service 0.2 is a draft and requires implementation/version admission.
- External sources do not validate Market Squawk's existing encrypted-vault implementation.
- The existing vault remains an implementation candidate requiring exact-commit admission; this
  batch neither declares it missing nor proves it release-ready.

## Source List

PAPER-006, PAPER-007, DOC-032 through DOC-038, and DOC-014 are registered in
`source-inventory.json` and assigned to `docs-batch-004` with access and digest/reference metadata.
