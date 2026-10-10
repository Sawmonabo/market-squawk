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
  direct_verified: "Verified",
  direct_unverified: "Unverified",
  official_delayed: "Delayed",
  aggregated: "Aggregated",
  indicative: "Indicative",
  modeled: "Modeled",
  estimated: "Estimated",
  stale: "Out of date",
  quarantined: "Needs review",
}

export function qualityLabel(quality: string | null): string {
  const known = dataQualities.find((candidate) => candidate === quality)
  return known === undefined ? "Not rated" : labels[known]
}

export function isExecutionEligible(quality: DataQuality): boolean {
  return quality === "direct_verified"
}
