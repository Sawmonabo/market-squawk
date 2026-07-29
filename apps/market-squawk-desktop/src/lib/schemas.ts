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

export const desktopBootstrapSchema = z.object({
  contractVersion: z.literal("market-squawk-desktop-v1"),
  applicationVersion: z.string(),
  buildProfile: z.string(),
  platform: z.string(),
  dataRoot: z.string(),
  storage: readinessSchema,
  installation: readinessSchema,
  modelRuntime: readinessSchema,
  mcp: readinessSchema,
  paperModeEnabled: z.boolean(),
  telemetryEnabled: z.boolean(),
  encryptedFileFallback: z.enum(["disabled", "locked", "ready"]),
  providerProfiles: z.array(providerProfileSchema),
  providerSessions: z.array(providerSessionSchema),
  operations: z.array(operationSummarySchema),
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
  encryptedFileFallback: z.enum(["disabled", "locked", "ready"]),
})

export type ApplicationResult = z.infer<typeof applicationResultSchema>
export type DesktopBootstrap = z.infer<typeof desktopBootstrapSchema>
export type ProviderProfile = z.infer<typeof providerProfileSchema>
export type ProviderSession = z.infer<typeof providerSessionSchema>

