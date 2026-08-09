const INTERSECTION_URN_PREFIX = 'urn:ngsi-ld:Intersection:'

/**
 * Convert the Orion entity identity used by Realtime into the business key
 * persisted by the Gold marts. Bare Gold keys are intentionally unchanged.
 */
export function toGoldIntersectionId(intersectionId: string): string {
  const value = intersectionId.trim()
  if (!value.startsWith(INTERSECTION_URN_PREFIX)) return value

  const suffix = value.slice(INTERSECTION_URN_PREFIX.length).trim()
  return suffix || value
}
