import type { ProductTransport } from "@/lib/transport"

import {
  changedPortfolio,
  parsePortfolioMeasurementAccounts,
  parsePortfolioMeasurementHoldings,
  parsePortfolioMeasurementPrincipals,
  samePortfolioAccount,
  samePortfolioHolding,
  type PortfolioMeasurementAccount,
  type PortfolioMeasurementHolding,
  type PortfolioMeasurementPrincipal,
} from "./portfolio-measurement-contracts"

export interface PortfolioMeasurementAccountSelection {
  account: PortfolioMeasurementAccount
  afterAccountId?: string
}

export interface PortfolioMeasurementPrincipalSelection {
  principal: PortfolioMeasurementPrincipal
  after?: string
}

export async function readFreshPortfolioMeasurementContext(
  transport: ProductTransport,
  selected: PortfolioMeasurementAccountSelection,
  expectedHolding: PortfolioMeasurementHolding,
  expectedPrincipal: PortfolioMeasurementPrincipalSelection,
) {
  const before = await readSelectedAccount(transport, selected)
  const holdingPage = parsePortfolioMeasurementHoldings(
    await transport.query({ query: "portfolioHoldings", accountId: before.accountId }),
    before,
  )
  const matchingHoldings = holdingPage.holdings.filter(
    (holding) => holding.instrument_id === expectedHolding.instrument_id,
  )
  const freshPrincipal = await readSelectedPrincipal(transport, expectedPrincipal)
  const after = await readSelectedAccount(transport, selected)
  if (
    !samePortfolioAccount(before, after) ||
    !samePortfolioAccount(before, selected.account) ||
    matchingHoldings.length !== 1 ||
    !samePortfolioHolding(matchingHoldings[0] as PortfolioMeasurementHolding, expectedHolding)
  ) {
    throw changedPortfolio()
  }
  if (JSON.stringify(freshPrincipal) !== JSON.stringify(expectedPrincipal.principal)) {
    throw changedPrincipal()
  }
  return {
    account: after,
    holding: matchingHoldings[0] as PortfolioMeasurementHolding,
    principal: freshPrincipal,
  }
}

async function readSelectedAccount(
  transport: ProductTransport,
  selected: PortfolioMeasurementAccountSelection,
) {
  const page = parsePortfolioMeasurementAccounts(
    await transport.query({
      query: "portfolioAccounts",
      ...(selected.afterAccountId ? { afterAccountId: selected.afterAccountId } : {}),
    }),
  )
  const matches = page.accounts.filter(
    (account) => account.accountId === selected.account.accountId,
  )
  if (matches.length !== 1) throw changedPortfolio()
  return matches[0] as PortfolioMeasurementAccount
}

async function readSelectedPrincipal(
  transport: ProductTransport,
  selected: PortfolioMeasurementPrincipalSelection,
) {
  const page = parsePortfolioMeasurementPrincipals(
    await transport.governanceQuery({
      query: "principals",
      limit: 64,
      ...(selected.after ? { after: selected.after } : {}),
    }),
  )
  const matches = page.principals.filter(
    (principal) => principal.principalId === selected.principal.principalId,
  )
  if (matches.length !== 1) throw changedPrincipal()
  return matches[0] as PortfolioMeasurementPrincipal
}

function changedPrincipal() {
  return new Error(
    "The selected governance principal changed while this measurement was being prepared. Refresh the inputs and review the current principal before trying again.",
  )
}
