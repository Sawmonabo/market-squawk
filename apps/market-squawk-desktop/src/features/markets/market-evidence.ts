import type { ApplicationResult } from "@/lib/schemas"

type RecordValue = Record<string, unknown>

export interface MarketEvidence {
  key: string
  sourceId: string
  venueId: string
  instrumentId: string
  providerProduct: string | null
  providerChannel: string | null
  phase: string | null
  asOf: string | null
  receivedAt: string | null
  sourceValidUntil: string | null
  recordedQuality: string | null
  currentQuality: string | null
  fresh: boolean | null
  snapshotInitialized: boolean | null
  generationCurrent: boolean | null
  crossedBook: boolean | null
  lastSequence: number | null
  bidDepth: number | null
  askDepth: number | null
  bestBid: BookLevel | null
  bestAsk: BookLevel | null
}

export interface BookLevel {
  priceTicks: number
  quantityLots: number
}

export function marketEvidence(
  snapshot: ApplicationResult | undefined,
  quality: ApplicationResult | undefined,
): MarketEvidence[] {
  const snapshots = rows(snapshot?.data)
  const qualities = rows(quality?.data)
  const byKey = new Map<string, RecordValue>()

  for (const row of [...snapshots, ...qualities]) {
    const key = identityKey(row)
    if (key) byKey.set(key, { ...byKey.get(key), ...row })
  }

  return [...byKey.entries()]
    .map(([key, row]) => toEvidence(key, row))
    .filter((value): value is MarketEvidence => value !== null)
    .sort((left, right) =>
      `${left.instrumentId}:${left.venueId}:${left.sourceId}`.localeCompare(
        `${right.instrumentId}:${right.venueId}:${right.sourceId}`,
      ),
    )
}

export function resultState(result: ApplicationResult | undefined) {
  if (!result) return null
  return {
    completeness: result.metadata.completeness,
    returned: result.metadata.returnedItems,
    available: result.metadata.availableItems,
  }
}

function toEvidence(key: string, row: RecordValue): MarketEvidence | null {
  const sourceId = text(row.sourceId)
  const venueId = text(row.venueId)
  const instrumentId = text(row.instrumentId)
  if (!sourceId || !venueId || !instrumentId) return null

  const book = record(row.book)
  const bids = array(book?.bids)
  const asks = array(book?.asks)

  return {
    key,
    sourceId,
    venueId,
    instrumentId,
    providerProduct: text(row.providerProduct),
    providerChannel: text(row.providerChannel),
    phase: evidenceName(row.phase),
    asOf: text(row.publishedAt) ?? text(row.evaluatedAt) ?? text(row.referenceAt),
    receivedAt: text(row.receivedAt),
    sourceValidUntil: text(row.sourceValidUntil),
    recordedQuality: evidenceName(row.recordedQuality),
    currentQuality: evidenceName(row.currentDisplayQuality),
    fresh: boolean(row.freshAtReference),
    snapshotInitialized: boolean(row.snapshotInitialized),
    generationCurrent: boolean(row.generationCurrent),
    crossedBook: boolean(row.crossedBook),
    lastSequence: integer(row.lastSequence),
    bidDepth: integer(row.stateBidDepth) ?? integer(book?.stateBidDepth),
    askDepth: integer(row.stateAskDepth) ?? integer(book?.stateAskDepth),
    bestBid: level(bids[0]),
    bestAsk: level(asks[0]),
  }
}

function identityKey(value: RecordValue) {
  const sourceId = text(value.sourceId)
  const venueId = text(value.venueId)
  const instrumentId = text(value.instrumentId)
  return sourceId && venueId && instrumentId
    ? `${sourceId}\u0000${venueId}\u0000${instrumentId}`
    : null
}

function rows(value: unknown): RecordValue[] {
  return array(value).map(record).filter((row): row is RecordValue => row !== null)
}

function level(value: unknown): BookLevel | null {
  const candidate = record(value)
  const priceTicks = integer(candidate?.priceTicks)
  const quantityLots = integer(candidate?.quantityLots)
  return priceTicks === null || quantityLots === null
    ? null
    : { priceTicks, quantityLots }
}

function record(value: unknown): RecordValue | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as RecordValue)
    : null
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null
}

function evidenceName(value: unknown): string | null {
  const direct = text(value)
  if (direct) return direct
  const candidate = record(value)
  if (!candidate) return null
  const keys = Object.keys(candidate)
  return keys.length === 1 ? keys[0] ?? null : null
}

function integer(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : null
}

function boolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null
}
