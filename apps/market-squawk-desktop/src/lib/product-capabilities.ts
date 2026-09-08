import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"

export function hasProductCapability(
  bootstrap: DesktopBootstrap,
  capability: ProductCapability,
) {
  return bootstrap.capabilities.includes(capability)
}

export function productCapabilitySet(bootstrap: DesktopBootstrap) {
  return new Set<ProductCapability>(bootstrap.capabilities)
}
