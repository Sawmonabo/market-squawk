export const dataQualities = [
  "direct_verified",
  "direct_unverified",
  "official_delayed",
  "aggregated",
  "indicative",
  "modeled",
  "estimated",
  "stale",
  "quarantined",
] as const

export type DataQuality = (typeof dataQualities)[number]

const labels: Record<DataQuality, string> = {
  direct_verified: "Direct verified",
  direct_unverified: "Direct unverified",
  official_delayed: "Official delayed",
  aggregated: "Aggregated",
  indicative: "Indicative",
  modeled: "Modeled",
  estimated: "Estimated",
  stale: "Stale",
  quarantined: "Quarantined",
}

export function qualityLabel(quality: DataQuality): string {
  return labels[quality]
}

export function isExecutionEligible(quality: DataQuality): boolean {
  return quality === "direct_verified"
}
