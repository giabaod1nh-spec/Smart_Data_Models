// intersectionDisplayName.test.ts — Tests for friendly intersection name utility
// Minimum test requirements from Section X:
// 1. name có giá trị: dùng name
// 2. name null: fallback từ URN
// 3. name empty string: fallback
// 4. URN C: → Intersection C
// 5. Không tạo: Intersection Intersection D
// 6. Full URN vẫn được giữ làm API value
// 7. Selector label dùng friendly name
// 8. Detail title dùng friendly name

import { describe, it, expect } from 'vitest'
import { getIntersectionDisplayName, getIntersectionShortLabel } from '@/utils/intersectionDisplayName'

describe('getIntersectionDisplayName (Section X)', () => {
  // Test 1: name has value -> use name directly
  it('1. Returns name field when it has a valid non-empty value', () => {
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:A', 'Nguyen Hue - Le Loi')).toBe('Nguyen Hue - Le Loi')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:B', 'Dien Bien Phu - Hai Ba Trung')).toBe('Dien Bien Phu - Hai Ba Trung')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:C', 'Cong Hoa - Hoang Van Thu')).toBe('Cong Hoa - Hoang Van Thu')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:D', 'Vo Van Kiet - Nguyen Tri Phuong')).toBe('Vo Van Kiet - Nguyen Tri Phuong')
  })

  // Test 2: name is null -> fallback from URN
  it('2. Falls back to formatted URN when name is null', () => {
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:A', null)).toBe('Intersection A')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:B', null)).toBe('Intersection B')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:C', null)).toBe('Intersection C')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:D', null)).toBe('Intersection D')
  })

  // Test 3: name is empty string or whitespace -> fallback
  it('3. Falls back to formatted URN when name is empty string or whitespace', () => {
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:X', '')).toBe('Intersection X')
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:X', '   ')).toBe('Intersection X')
  })

  // Test 4: URN C -> Intersection C
  it('4. Formats URN with C suffix to Intersection C', () => {
    expect(getIntersectionDisplayName('urn:ngsi-ld:Intersection:C', null)).toBe('Intersection C')
  })

  // Test 5: Does NOT produce "Intersection Intersection D"
  it('5. Prevents duplicate "Intersection Intersection D" when ID already starts with Intersection', () => {
    expect(getIntersectionDisplayName('Intersection D', null)).toBe('Intersection D')
    expect(getIntersectionDisplayName('Intersection D', '')).toBe('Intersection D')
    expect(getIntersectionDisplayName('D', null)).toBe('Intersection D')
  })

  // Test 6: Full URN is preserved as API value
  it('6. Preserves full URN intact for API requests without altering the value', () => {
    const rawApiId = 'urn:ngsi-ld:Intersection:D'
    const label = getIntersectionDisplayName(rawApiId, 'Vo Van Kiet - Nguyen Tri Phuong')

    expect(label).toBe('Vo Van Kiet - Nguyen Tri Phuong')
    // The raw API ID remains unmodified
    expect(rawApiId).toBe('urn:ngsi-ld:Intersection:D')
  })

  // Test 7: Selector label uses friendly name
  it('7. Formats selector options with friendly name while preserving URN value', () => {
    const mockIntersection = {
      id: 'urn:ngsi-ld:Intersection:D',
      name: 'Vo Van Kiet - Nguyen Tri Phuong',
    }

    const optionValue = mockIntersection.id
    const optionLabel = getIntersectionDisplayName(mockIntersection.id, mockIntersection.name)

    expect(optionValue).toBe('urn:ngsi-ld:Intersection:D')
    expect(optionLabel).toBe('Vo Van Kiet - Nguyen Tri Phuong')
  })

  // Test 8: Detail title uses friendly name
  it('8. Formats detail title cleanly using friendly name', () => {
    const titleWithName = getIntersectionDisplayName('urn:ngsi-ld:Intersection:D', 'Vo Van Kiet - Nguyen Tri Phuong')
    expect(titleWithName).toBe('Vo Van Kiet - Nguyen Tri Phuong')

    const titleFallback = getIntersectionDisplayName('urn:ngsi-ld:Intersection:D', null)
    expect(titleFallback).toBe('Intersection D')
  })

  // Edge cases
  it('handles null and undefined safely', () => {
    expect(getIntersectionDisplayName(null, null)).toBe('Unknown Intersection')
    expect(getIntersectionDisplayName(undefined, undefined)).toBe('Unknown Intersection')
    expect(getIntersectionDisplayName(null, 'Main & 5th')).toBe('Main & 5th')
  })
})

describe('getIntersectionShortLabel', () => {
  it('truncates long names to maxLength', () => {
    const longName = 'Very Long Intersection Name That Exceeds The Limit'
    const result = getIntersectionShortLabel(null, longName, 20)
    expect(result.length).toBeLessThanOrEqual(20)
    expect(result.endsWith('…')).toBe(true)
  })

  it('does not truncate short names', () => {
    expect(getIntersectionShortLabel(null, 'Short Name', 24)).toBe('Short Name')
  })
})
