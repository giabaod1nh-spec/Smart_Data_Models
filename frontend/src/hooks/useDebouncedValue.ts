// useDebouncedValue.ts — Debounce a changing value.
//
// Used to debounce polling inputs (e.g. intersectionId): when the user switches
// intersections rapidly, only the last value after `delayMs` of silence is
// emitted, so each intermediate value does not fire its own API request.

import { useEffect, useState } from 'react'

export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState<T>(value)

  useEffect(() => {
    if (debounced === value) return
    const timer = setTimeout(() => setDebounced(value), delayMs)
    return () => clearTimeout(timer)
  }, [value, delayMs, debounced])

  return debounced
}
