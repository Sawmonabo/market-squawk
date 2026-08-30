import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"

export const readinessSchema = z.object({
  state: z.enum(["ready", "available", "not_configured", "unverified"]),
  label: z.string(),
  detail: z.string(),
})

export const productCapabilitySchema = z.enum([
  "backtest_advanced_start",
  "backtest_activity",
  "backtest_artifact_read",
  "backtest_preparation",
  "backtest_prepared_start",
  "backtest_preview",
  "backtest_result",
  "bot_start",
  "bot_status",
  "bot_stop",
  "decision_analysis",
  "decision_analysis_list",
  "decision_recommendation_history",
  "decision_screen_list",
  "decision_target_review",
  "execution_cancel",
  "execution_fills",
  "execution_manual_draft",
  "execution_manual_targets",
  "execution_orders",
  "execution_reconcile",
  "fair_value_approvals",
  "fair_value_audit",
  "fair_value_classification",
  "fair_value_classify",
  "fair_value_evidence",
  "fair_value_explain",
  "fair_value_governance_commit",
  "fair_value_governance_preview",
  "fair_value_market_access",
  "fair_value_measurement",
  "fair_value_measurement_list",
  "fair_value_workspace",
  "feature_dataset_preparation",
  "feature_dataset_prepared_start",
  "feature_dataset_preview",
  "forecast_detail",
  "forecast_evaluate",
  "forecast_list",
  "forecast_metadata",
  "forecast_outcomes",
  "forecast_preparation",
  "forecast_prepare",
  "forecast_prepared_start",
  "fundamental_facts",
  "governance_authenticate",
  "governance_principals",
  "installation_status",
  "investment_lookup",
  "job_list",
  "job_watch",
  "macro_context",
  "macro_revisions",
  "market_instrument",
  "market_overview",
  "market_universe",
  "model_activity",
  "model_evidence",
  "model_training_start",
  "operations_backup_list",
  "operations_log_export",
  "operations_log_query",
  "operations_rollback_preview",
  "operations_rollback_start",
  "operations_runtime_status",
  "operations_settings",
  "operations_update_check",
  "operations_update_preview",
  "operations_update_start",
  "operations_update_status",
  "operations_workspace_list",
  "portfolio_account_list",
  "portfolio_attribution",
  "portfolio_candidate_impact",
  "portfolio_exposure",
  "portfolio_holdings",
  "portfolio_import_approve",
  "portfolio_import_commit",
  "portfolio_import_discard",
  "portfolio_import_preview",
  "portfolio_performance",
  "portfolio_rebalance",
  "portfolio_recommendation_setup",
  "portfolio_revision_list",
  "portfolio_risk",
  "portfolio_scenario",
  "portfolio_scenario_batch",
  "portfolio_transactions",
  "research_dataset_list",
  "research_export",
  "research_file_commit",
  "research_file_discard",
  "research_file_preview",
  "research_manifest",
  "risk_kill_switch",
])

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
  capabilities: z.array(productCapabilitySchema),
}).strict()

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

export const applicationResultSchema = z
  .object({
    data: z.unknown(),
    metadata: z
      .object({
        completeness: z.string(),
        returnedItems: z.number().int().nonnegative(),
        availableItems: z.number().int().nonnegative(),
        sourceCoverage: z.unknown(),
        dataQuality: z.unknown(),
      })
      .strict(),
  })
  .strict()

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
  capabilities: z.object({
    credentialImport: z.boolean(),
    health: z.boolean(),
    manifestEvidence: z.boolean(),
    researchIngestion: z.boolean(),
    status: z.boolean(),
    coverage: z.boolean(),
  }).strict(),
}).strict()

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
export type ProductCapability = z.infer<typeof productCapabilitySchema>
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
