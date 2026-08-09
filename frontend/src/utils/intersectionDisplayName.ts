// intersectionDisplayName.ts — Single source of truth for intersection display names.
// Rule:
//   1. If IntersectionResponse.name is non-empty → use it directly (e.g. "Nguyen Hue - Le Loi" or "Ngã tư Cộng Hòa")
//   2. Else if id matches urn:ngsi-ld:Intersection:{suffix} → "Intersection {suffix}" (e.g. "Intersection C")
//   3. Else if id matches single short identifier (e.g. "A", "B", "C", "D") → "Intersection {id}"
//   4. If id already starts with "Intersection ", return as-is (prevent "Intersection Intersection D")
//   5. Fallback → return id or "Unknown Intersection"
//
// The actual API id (full URN, e.g. "urn:ngsi-ld:Intersection:D") MUST NEVER be replaced in API calls.
// Only presentation labels use this function.

/**
 * Get a human-friendly display name for an intersection.
 * @param id   The full URN id used by the API (e.g. "urn:ngsi-ld:Intersection:C")
 * @param name The optional .name field from IntersectionResponse (e.g. "Cong Hoa - Hoang Van Thu")
 * @returns    Friendly label (e.g. "Cong Hoa - Hoang Van Thu" or "Intersection C")
 */
export function getIntersectionDisplayName(
  id: string | null | undefined,
  name: string | null | undefined,
): string {
  // Priority 1: name from API if non-empty
  if (name && name.trim().length > 0) {
    return name.trim()
  }

  // Priority 2: derive friendly label from id
  if (id && id.trim().length > 0) {
    const rawId = id.trim()
    const urnPrefix = 'urn:ngsi-ld:Intersection:'

    if (rawId.startsWith(urnPrefix)) {
      const suffix = rawId.slice(urnPrefix.length).trim()
      if (suffix.length > 0) {
        return `Intersection ${suffix}`
      }
    }

    // If ID contains colon like "Intersection:C" or "int:C"
    const lastColon = rawId.lastIndexOf(':')
    if (lastColon !== -1 && lastColon < rawId.length - 1) {
      const suffix = rawId.slice(lastColon + 1).trim()
      if (suffix.length > 0) {
        return `Intersection ${suffix}`
      }
    }

    // If ID already starts with "Intersection ", return as-is to avoid duplicate prefix
    if (rawId.startsWith('Intersection ')) {
      return rawId
    }

    // Short IDs like "A", "B", "C", "D", "J1", "J2"
    if (rawId.length <= 3) {
      return `Intersection ${rawId}`
    }

    // Fallback: return raw ID
    return rawId
  }

  return 'Unknown Intersection'
}

/**
 * Short label — same as getIntersectionDisplayName but truncated for
 * compact UI contexts like select options and node labels.
 */
export function getIntersectionShortLabel(
  id: string | null | undefined,
  name: string | null | undefined,
  maxLength = 28,
): string {
  const full = getIntersectionDisplayName(id, name)
  if (full.length <= maxLength) return full
  return full.slice(0, maxLength - 1) + '…'
}
