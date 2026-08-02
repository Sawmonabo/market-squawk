import { z } from "zod"

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

export const setupStepSchema = z.object({
  id: z.enum([
    "system",
    "storage",
    "sources",
    "research",
    "portfolio",
    "paper",
    "mcp",
    "review",
  ]),
  label: z.string(),
  state: z.enum(["complete", "action_required", "blocked", "available"]),
  complete: z.boolean(),
  detail: z.string(),
  blockingReason: z.string().nullable(),
  recovery: z.string().nullable(),
  action: z
    .enum([
      "configure_sources",
      "configure_research",
      "configure_portfolio",
      "configure_paper",
      "review_mcp",
      "review_status",
    ])
    .nullable(),
})

export const mcpClientInstructionSchema = z.object({
  program: z.string().min(1),
  arguments: z.array(z.string()),
  environment: z.record(z.string(), z.string()),
  requiresDesktopExit: z.literal(true),
})

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
  mcpClient: mcpClientInstructionSchema.nullable(),
  telemetryEnabled: z.boolean(),
  encryptedFileFallback: encryptedFileFallbackSchema,
  providerProfiles: z.array(providerProfileSchema),
  providerSessions: z.array(providerSessionSchema),
  setupSteps: z.array(setupStepSchema).length(8),
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
  expiresAt: z.number().int(),
})

export const mcpStatusSchema = z.object({
  serviceReady: z.boolean(),
  sharedEndpointReady: z.boolean(),
  claudeCode: z.string(),
  codex: z.string(),
})

export const desktopEventSchema = z.object({
  runtime: desktopBootstrapSchema.shape.runtime,
  sequence: z.number().int().nonnegative(),
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
  ]),
})

export type ApplicationResult = z.infer<typeof applicationResultSchema>
export type DesktopBootstrap = z.infer<typeof desktopBootstrapSchema>
export type DesktopEvent = z.infer<typeof desktopEventSchema>
export type EncryptedFileFallback = z.infer<
  typeof encryptedFileFallbackSchema
>
export type InstallationControlResult = z.infer<
  typeof installationControlResultSchema
>
export type InstallationStatus = z.infer<typeof installationStatusSchema>
export type InputTicket = z.infer<typeof inputTicketSchema>
export type McpStatus = z.infer<typeof mcpStatusSchema>
export type ProviderActivation = z.infer<typeof providerActivationSchema>
export type ProviderBootstrap = z.infer<typeof providerBootstrapSchema>
export type ProviderProfile = z.infer<typeof providerProfileSchema>
export type ProviderSession = z.infer<typeof providerSessionSchema>
export type SetupStep = z.infer<typeof setupStepSchema>
