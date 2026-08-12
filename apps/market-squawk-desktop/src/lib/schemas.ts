import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"

export const readinessSchema = z.object({
  state: z.enum(["ready", "available", "not_configured", "unverified"]),
  label: z.string(),
  detail: z.string(),
})

export const operationSummarySchema = z.object({
  name: z.string(),
  description: z.string(),
  domain: z.string(),
  authorization: z.enum([
    "read_only",
    "local_confirmation",
    "risk_mediated",
  ]),
  readOnly: z.boolean(),
  destructive: z.boolean(),
  inputSchema: z.record(z.string(), z.unknown()),
})

export const providerProfileSchema = z
  .object({
    id: z.string(),
    display_name: z.string(),
    official_handoff_url: z.string().url(),
    handoff_instruction: z.string(),
    zero_fee: z.string(),
    account_requirement: z.string(),
    credential_requirement: z.string(),
    release_state: z.string(),
    coverage: z.string(),
    quality_ceiling: z.string(),
  })
  .loose()

export const providerSessionSchema = z
  .object({
    session_id: z.string().uuid(),
    surface_id: z.string(),
    state: z.string(),
    next_action: z.string(),
    credential_stored: z.boolean(),
  })
  .loose()

export const providerActivationSchema = z
  .object({
    profile: z.string(),
    session_id: z.string().uuid(),
    capability_revision: z.number().int().nonnegative(),
  })
  .loose()

export const encryptedFileFallbackSchema = z.enum([
  "disabled",
  "locked",
  "ready",
])

export const setupGoalSchema = z.enum([
  "everything_recommended",
  "explore_public_markets",
  "research_investments",
  "manage_portfolio",
  "build_and_evaluate_models",
  "practice_paper_execution",
  "use_claude_code",
  "use_codex",
])

export const setupStarterPlanSchema = z.enum([
  "everything_recommended",
  "public_markets",
  "research",
  "portfolio",
  "models",
  "paper_practice",
  "ai_clients",
])

const durableSetupPlanSelectionSchema = z.object({
  goals: z.array(setupGoalSchema).min(1).max(8),
  starterPlan: setupStarterPlanSchema,
})

const setupStepIdSchema = z.enum([
  "goals_and_starter_plan",
  "storage_retention_time_and_disk",
  "public_and_zero_fee_providers",
  "file_and_portfolio_import",
  "model_runtime",
  "paper_and_risk",
  "claude_code",
  "codex",
  "backup",
  "review",
  "first_useful_result",
])

const setupExternalContactSchema = z.enum([
  "coinbase_public_api",
  "kraken_public_api",
  "securities_and_exchange_commission",
  "bureau_of_labor_statistics",
  "united_states_treasury",
  "federal_reserve_bank_of_st_louis",
  "claude_code_official_cli",
  "codex_official_cli",
])

const setupReversibleChangeSchema = z.enum([
  "accept_workspace_plan",
  "configure_workspace_retention_and_budget",
  "activate_or_remove_provider_sessions",
  "import_or_remove_derived_local_data",
  "configure_or_reset_model_runtime",
  "configure_stopped_paper_account_and_risk_defaults",
  "register_or_disconnect_claude_code",
  "register_or_disconnect_codex",
  "create_or_remove_backup_policy",
])

const setupDiskImpactSchema = z.enum([
  "no_additional_product_bytes",
  "variable_within_workspace_soft_limit",
  "variable_backup_destination",
])

const setupCapabilitySchema = z.enum([
  "managed_workspace",
  "retention_and_disk_budget",
  "public_market_data",
  "filing_research",
  "macro_research",
  "controlled_file_import",
  "portfolio_import",
  "managed_python_runtime",
  "native_model_inference",
  "onnx_model_inference",
  "paper_only_execution",
  "central_risk",
  "claude_code_mcp",
  "codex_mcp",
  "verified_backup",
  "capability_review",
  "first_useful_result",
])

const setupPlanChoiceSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("goals"),
    starter_plan: setupStarterPlanSchema,
    goals: z.array(setupGoalSchema).min(1).max(8),
  }),
  z.object({
    kind: z.literal("storage"),
    retention_days: z.number().int().nonnegative(),
    workspace_soft_limit_bytes: losslessIntegerSchema,
    time_policy: z.literal(
      "point_in_time_with_first_observed_locally_provenance",
    ),
  }),
  z.object({
    kind: z.literal("providers"),
    outcomes: z.array(z.enum([
      "coinbase_public_market_snapshot",
      "kraken_public_market_snapshot",
      "sec_filing_research",
      "bls_macro_research",
      "treasury_rates_research",
      "fred_alfred_authorized_research",
    ])).min(1).max(6),
  }),
  z.object({
    kind: z.literal("imports"),
    formats: z.array(z.enum(["csv", "json", "ndjson", "parquet", "portfolio_file"])).max(5),
    preserve_source_identity: z.boolean(),
    require_reconciliation_receipt: z.boolean(),
  }),
  z.object({
    kind: z.literal("model_runtime"),
    managed_python: z.boolean(),
    native_inference: z.boolean(),
    onnx_inference: z.boolean(),
  }),
  z.object({
    kind: z.literal("paper_risk"),
    starts_stopped: z.boolean(),
    paper_only: z.boolean(),
    central_risk_required: z.boolean(),
  }),
  z.object({
    kind: z.literal("claude_code"),
    separate_client_credential: z.boolean(),
    require_real_safe_read: z.boolean(),
  }),
  z.object({
    kind: z.literal("codex"),
    separate_client_credential: z.boolean(),
    require_real_safe_read: z.boolean(),
  }),
  z.object({
    kind: z.literal("backup"),
    retention_count: z.number().int().positive(),
    verify_after_create: z.boolean(),
  }),
  z.object({
    kind: z.literal("review"),
    show_gaps_and_reversible_changes: z.boolean(),
  }),
  z.object({
    kind: z.literal("first_useful_result"),
    result: z.enum([
      "verified_public_market_snapshot",
      "point_in_time_research_result",
      "reconciled_portfolio_summary",
      "admitted_model_forecast",
      "stopped_paper_and_risk_review",
      "verified_mcp_safe_read",
    ]),
    target_minutes: z.number().int().positive(),
  }),
])

const durableSetupPlanStepSchema = z.object({
  id: setupStepIdSchema,
  outcome: z.enum([
    "durable_resumable_plan",
    "governed_workspace_budget",
    "quality_labeled_provider_evidence",
    "receipt_bound_local_data",
    "verified_local_model_runtime",
    "stopped_paper_under_central_risk",
    "verified_claude_code_mcp",
    "verified_codex_mcp",
    "verified_recovery_point",
    "capability_gap_review",
    "first_useful_result",
  ]),
  disposition: z.enum(["included", "available_to_finish_later"]),
  requiredInput: z.enum([
    "none",
    "local_confirmation",
    "local_disk",
    "zero_fee_account_or_provider_key",
    "owned_file",
    "detected_local_client",
  ]),
  externalContacts: z.array(setupExternalContactSchema).max(8),
  reversibleLocalChange: setupReversibleChangeSchema.nullable(),
  expectedActiveMinutes: z.number().int().nonnegative(),
  diskImpact: setupDiskImpactSchema,
  safeSkip: z.enum([
    "not_skippable",
    "capability_remains_installed_and_available",
  ]),
  choice: setupPlanChoiceSchema,
})

export const durableSetupPlanSchema = z.object({
  formatVersion: z.number().int().positive(),
  revision: losslessIntegerSchema,
  selection: durableSetupPlanSelectionSchema,
  steps: z.array(durableSetupPlanStepSchema).length(11),
})

export const setupPlanStatusSchema = z.object({
  formatVersion: z.number().int().positive(),
  catalog: z.object({
    formatVersion: z.number().int().positive(),
    goals: z.array(setupGoalSchema).length(8),
    starterPlans: z.array(setupStarterPlanSchema).length(7),
    recommendedStarterPlan: setupStarterPlanSchema,
  }),
  currentRevision: losslessIntegerSchema,
  acceptedPlan: z
    .object({
      revision: losslessIntegerSchema,
      digest: z.string().regex(/^[0-9a-f]{64}$/),
      acceptedAtUnixSeconds: losslessIntegerSchema,
      plan: durableSetupPlanSchema,
    })
    .nullable(),
})

export const setupPlanPreviewSchema = z.object({
  formatVersion: z.number().int().positive(),
  previewId: z.string().uuid(),
  ownerWorkspace: z.string().uuid(),
  currentRevision: losslessIntegerSchema,
  planDigest: z.string().regex(/^[0-9a-f]{64}$/),
  plan: durableSetupPlanSchema,
  includedCapabilities: z.array(setupCapabilitySchema).max(17),
  externalContacts: z.array(setupExternalContactSchema).max(8),
  reversibleLocalChanges: z.array(setupReversibleChangeSchema).max(9),
  expectedTime: z.object({
    expectedActiveMinutes: z.number().int().nonnegative(),
    firstUseTargetMinutes: z.number().int().positive(),
    includesExternalWait: z.boolean(),
  }),
  expectedDisk: z.object({
    workspaceSoftLimitBytes: losslessIntegerSchema,
    includedImpacts: z.array(setupDiskImpactSchema).max(3),
  }),
  safeSkipSteps: z.array(setupStepIdSchema).max(7),
  issuedAtUnixSeconds: losslessIntegerSchema,
  expiresAtUnixSeconds: losslessIntegerSchema,
  previewSha256: z.string().regex(/^[0-9a-f]{64}$/),
})

export const setupPlanReceiptSchema = z.object({
  revision: losslessIntegerSchema,
  digest: z.string().regex(/^[0-9a-f]{64}$/),
  acceptedAtUnixSeconds: losslessIntegerSchema,
})

export type SetupPlanStatus = z.infer<typeof setupPlanStatusSchema>
export type SetupPlanPreview = z.infer<typeof setupPlanPreviewSchema>

export const desktopBootstrapSchema = z.object({
  contractVersion: z.literal("market-squawk-desktop-v1"),
  applicationVersion: z.string(),
  buildProfile: z.string(),
  platform: z.string(),
  dataRoot: z.string(),
  runtime: z.object({
    installationId: z.string().uuid(),
    workspaceId: z.string().uuid(),
    serviceGeneration: z.number().int().positive(),
  }),
  storage: readinessSchema,
  installation: readinessSchema,
  modelRuntime: readinessSchema,
  mcp: readinessSchema,
  telemetryEnabled: z.boolean(),
  encryptedFileFallback: encryptedFileFallbackSchema,
  providerProfiles: z.array(providerProfileSchema),
  providerSessions: z.array(providerSessionSchema),
  operations: z.array(operationSummarySchema),
})

export const installationStatusSchema = z.object({
  installed: z.boolean(),
  active_version: z.string().nullable(),
  previous_version: z.string().nullable(),
  target: z.string().nullable(),
  manifest_sha256: z.string().nullable(),
  channel_manifest_url: z.string().nullable(),
  healthy: z.boolean(),
})

const installationReceiptSchema = z.object({
  version: z.string(),
  previous_version: z.string().nullable(),
  manifest_sha256: z.string(),
  target: z.string(),
  repaired: z.boolean(),
})

const uninstallReceiptSchema = z.object({
  removed_program: z.boolean(),
  deleted_data_classes: z.array(z.string()),
})

export const installationControlResultSchema = z.object({
  action: z.enum(["status", "update", "repair", "rollback", "uninstall"]),
  status: installationStatusSchema,
  receipt: z
    .union([installationReceiptSchema, uninstallReceiptSchema])
    .nullable(),
  restartRequired: z.boolean(),
})

export const applicationResultSchema = z.object({
  data: z.unknown(),
  metadata: z.object({
    completeness: z.string(),
    returnedItems: z.number().int().nonnegative(),
    availableItems: z.number().int().nonnegative(),
    sourceCoverage: z.unknown(),
    dataQuality: z.unknown(),
  }),
})

export const governanceProvisioningStatusSchema = z.object({
  state: z.enum(["unprovisioned", "active"]),
  configured: z.boolean(),
  principals: z.array(
    z.object({
      principalId: z.string().min(1),
      displayName: z.string().min(1),
      roles: z.array(z.string().min(1)),
    }),
  ),
  missingRoles: z.array(z.string().min(1)),
})

export const providerBootstrapSchema = z.object({
  profiles: z.array(providerProfileSchema),
  sessions: z.array(providerSessionSchema),
  encryptedFileFallback: encryptedFileFallbackSchema,
})

export const inputTicketSchema = z.object({
  id: z.string().uuid(),
  installationId: z.string().uuid(),
  workspaceId: z.string().uuid(),
  generation: z.number().int().positive(),
  clientId: z.string().uuid(),
  mediaType: z.string(),
  byteLength: z.number().int().positive(),
  digest: z.object({
    algorithm: z.literal("sha256"),
    bytes: z.array(z.number().int().min(0).max(255)).length(32),
  }),
  expiresAt: losslessIntegerSchema,
})

const mcpServiceClientStatusSchema = z.object({
  client: z.enum(["claude_code", "codex"]),
  clientId: z.string().uuid(),
  credentialGeneration: z.number().int().positive(),
  credentialIdentity: z.string().min(1).max(128),
  maximumActiveRequests: z.number().int().positive(),
  activeRequests: z.number().int().nonnegative(),
  admittedRequests: z.number().int().nonnegative(),
  rateLimitedRequests: z.number().int().nonnegative(),
  observedRelayInitializations: z.number().int().nonnegative(),
  lastActivityUnixSeconds: z.number().int().nonnegative().nullable(),
  credentialRotationRecoveryPending: z.boolean(),
  priorCredentialCleanupPending: z.boolean(),
  accessRevoked: z.boolean(),
})

const mcpRuntimeStatusSchema = z.object({
  sessionModel: z.literal("stateless_request_scoped"),
  activeClients: z.number().int().nonnegative(),
  activeRequests: z.number().int().nonnegative(),
  admittedRequests: z.number().int().nonnegative().nullable(),
  rateLimitedRequests: z.number().int().nonnegative().nullable(),
  rejectedCredentials: z.number().int().nonnegative(),
  uptimeSeconds: z.number().int().nonnegative(),
  process: z.object({
    residentMemoryBytes: z.number().int().nonnegative().nullable(),
    virtualMemoryBytes: z.number().int().nonnegative().nullable(),
  }),
  limits: z.object({
    maximumFrameBytes: z.number().int().positive(),
    maximumBodyBytes: z.number().int().positive(),
    maximumActiveRequests: z.number().int().positive(),
    maximumInlineBytes: z.number().int().positive(),
    maximumInlineItems: z.number().int().positive(),
    maximumResultBytes: z.number().int().positive(),
    maximumResultItems: z.number().int().positive(),
    requestTimeoutMilliseconds: z.number().int().positive(),
  }),
  clients: z.array(mcpServiceClientStatusSchema).length(2),
})

export const mcpClientsStatusSchema = z.object({
  serviceReady: z.boolean(),
  sharedEndpointReady: z.boolean(),
  workspaceId: z.string().uuid(),
  serviceGeneration: z.number().int().positive(),
  protocolVersion: z.string().min(1).max(64),
  transport: z.literal("stdio_relay"),
  runtime: mcpRuntimeStatusSchema,
  clients: z.array(
    z.object({
      client: z.enum(["claude_code", "codex"]),
      label: z.string().min(1).max(64),
      state: z.enum([
        "absent",
        "unsupported",
        "ready",
        "owned",
        "repair_required",
        "access_revoked",
        "conflict",
      ]),
      clientVersion: z.string().min(1).max(128).nullable(),
      receipt: z
        .object({
          commandSha256: z.string().regex(/^[0-9a-f]{64}$/),
          observedAtUnixSeconds: z.number().int().nonnegative(),
        })
        .nullable(),
      verification: z
        .object({
          protocolVersion: z.string().min(1).max(64),
          clientInfoName: z.string().min(1).max(128),
          serverName: z.string().min(1).max(128),
          toolCount: z.number().int().nonnegative(),
          resourceCount: z.number().int().nonnegative(),
          toolDomains: z.array(z.string().min(1).max(64)).min(1).max(32),
          resourceNames: z.array(z.string().min(1).max(64)).min(1).max(32),
          safeReadTool: z.string().min(1).max(128),
          verifiedAtUnixSeconds: z.number().int().nonnegative(),
        })
        .nullable(),
      blocker: z.string().min(1).max(256).nullable(),
      service: mcpServiceClientStatusSchema,
    }),
  ).length(2),
})

const desktopEventSequenceSchema = z
  .string()
  .refine(
    (value) =>
      value.length <= 20 &&
      /^(?:0|[1-9]\d*)$/.test(value) &&
      BigInt(value) <= 18_446_744_073_709_551_615n,
    { message: "Expected a canonical unsigned 64-bit decimal" },
  )

export const desktopEventSchema = z.object({
  runtime: desktopBootstrapSchema.shape.runtime,
  sequence: desktopEventSequenceSchema,
  body: z.discriminatedUnion("type", [
    z.object({
      type: z.literal("authority_changed"),
      domain: z.string(),
      operation: z.string(),
      requestId: z.string(),
    }),
    z.object({
      type: z.literal("resync_required"),
      reason: z.string(),
    }),
    z.object({
      type: z.literal("stream_disconnected"),
      reason: z.string(),
    }),
  ]),
})

export const desktopEventSubscriptionReceiptSchema = z.object({
  subscriptionId: z.string().uuid(),
  runtime: desktopBootstrapSchema.shape.runtime,
  sequence: desktopEventSequenceSchema,
  resumed: z.boolean(),
})

export const desktopServiceBootstrapSchema = z.object({
  status: z.literal("bootstrap_required"),
  requirement: z.enum([
    "encrypted_fallback_locked",
    "foreground_keyring_retry",
  ]),
})

export const desktopStartupSchema = z.union([
  desktopBootstrapSchema,
  desktopServiceBootstrapSchema,
])

export type ApplicationResult = z.infer<typeof applicationResultSchema>
export type DesktopBootstrap = z.infer<typeof desktopBootstrapSchema>
export type DesktopServiceBootstrap = z.infer<
  typeof desktopServiceBootstrapSchema
>
export type DesktopStartup = z.infer<typeof desktopStartupSchema>
export type DesktopEvent = z.infer<typeof desktopEventSchema>
export type DesktopEventSubscriptionReceipt = z.infer<
  typeof desktopEventSubscriptionReceiptSchema
>
export type EncryptedFileFallback = z.infer<
  typeof encryptedFileFallbackSchema
>
export type InstallationControlResult = z.infer<
  typeof installationControlResultSchema
>
export type InstallationStatus = z.infer<typeof installationStatusSchema>
export type InputTicket = z.infer<typeof inputTicketSchema>
export type GovernanceProvisioningStatus = z.infer<
  typeof governanceProvisioningStatusSchema
>
export type McpClientsStatus = z.infer<typeof mcpClientsStatusSchema>
export type ProviderActivation = z.infer<typeof providerActivationSchema>
export type ProviderBootstrap = z.infer<typeof providerBootstrapSchema>
export type ProviderProfile = z.infer<typeof providerProfileSchema>
export type ProviderSession = z.infer<typeof providerSessionSchema>
