/**
 * SUMO vehicle sprites — PNG textures matching SUMO GUI (intersection.rou.xml imgFile).
 */
import React, { useEffect, useState } from 'react'
import { Group, Image, Rect } from 'react-konva'
import type { LiveNetworkGeometry } from '@/types/liveTraffic'
import { mpx } from '@/utils/canvasWorldScale'

export interface VehicleRenderProps {
  type: string
  lengthM: number
  widthM: number
  speed: number
  scale: number
  /** SUMO angle → Konva rotation */
  rotation: number
  x: number
  y: number
  images: Map<string, HTMLImageElement>
}

/** Default vType dimensions (mirror intersection.rou.xml). */
export const DEFAULT_VTYPE_DIMS: Record<string, { length: number; width: number }> = {
  motorcycle: { length: 2.0, width: 0.9 },
  car: { length: 4.5, width: 2.2 },
  bus: { length: 12.0, width: 2.8 },
  truck: { length: 7.1, width: 2.4 },
  container: { length: 16.5, width: 2.5 },
  ambulance: { length: 5.5, width: 2.2 },
  police: { length: 5.0, width: 2.2 },
  firetruck: { length: 8.0, width: 2.5 },
}

/** PNG paths under /public/images — same names as SUMO vType imgFile. */
export const VEHICLE_SPRITE_URLS: Record<string, string> = {
  motorcycle: '/images/bike_bg.png',
  car: '/images/car_final.png',
  bus: '/images/bus_bg.png',
  truck: '/images/truck_bg.png',
  container: '/images/tractor_head.png',
  ambulance: '/images/ambulance_bg.png',
  police: '/images/police_bg.png',
  firetruck: '/images/firetruck_bg.png',
}

const TRAILER_URL = '/images/trailer_box.png'
const CONTAINER_LOCOMOTIVE_M = 4.0
const CONTAINER_TRAILER_M = 12.0
const CONTAINER_GAP_M = 0.5

const spriteLoadCache = new Map<string, Promise<HTMLImageElement>>()

function loadSprite(url: string): Promise<HTMLImageElement> {
  const cached = spriteLoadCache.get(url)
  if (cached) return cached
  const promise = new Promise<HTMLImageElement>((resolve, reject) => {
    const img = new window.Image()
    img.onload = () => resolve(img)
    img.onerror = () => reject(new Error(`failed to load vehicle sprite: ${url}`))
    img.src = url
  })
  spriteLoadCache.set(url, promise)
  return promise
}

/** Preload all vehicle PNGs once for Konva Image nodes. */
export async function loadVehicleSprites(): Promise<Map<string, HTMLImageElement>> {
  const urls = new Set([...Object.values(VEHICLE_SPRITE_URLS), TRAILER_URL])
  const entries = await Promise.all(
    [...urls].map(async (url) => [url, await loadSprite(url)] as const),
  )
  const byUrl = new Map(entries)
  const byType = new Map<string, HTMLImageElement>()
  for (const [type, url] of Object.entries(VEHICLE_SPRITE_URLS)) {
    const img = byUrl.get(url)
    if (img) byType.set(type, img)
  }
  byType.set('trailer_box', byUrl.get(TRAILER_URL)!)
  return byType
}

export function useVehicleSprites(): Map<string, HTMLImageElement> {
  const [images, setImages] = useState<Map<string, HTMLImageElement>>(new Map())
  useEffect(() => {
    let cancelled = false
    loadVehicleSprites()
      .then((map) => {
        if (!cancelled) setImages(map)
      })
      .catch(() => {
        /* keep empty map — fallback rects render */
      })
    return () => {
      cancelled = true
    }
  }, [])
  return images
}

export function resolveVehicleDims(
  type: string,
  lengthM: number,
  widthM: number,
  network: LiveNetworkGeometry | null,
): { length: number; width: number } {
  if (lengthM > 0.5 && widthM > 0.2) {
    return { length: lengthM, width: widthM }
  }
  const fromNet = network?.vTypes?.[type]
  if (fromNet) {
    return { length: fromNet.length, width: fromNet.width }
  }
  return DEFAULT_VTYPE_DIMS[type] ?? DEFAULT_VTYPE_DIMS.car
}

function spriteForType(type: string, images: Map<string, HTMLImageElement>): HTMLImageElement | undefined {
  const key = type.toLowerCase()
  return images.get(key) ?? images.get('car')
}

/** Flip PNG (front at bottom) → align with SUMO heading (rotation = -angle). */
function VehicleImage({
  img,
  widPx,
  lenPx,
  opacity = 1,
}: {
  img: HTMLImageElement
  widPx: number
  lenPx: number
  opacity?: number
}) {
  return (
    <Image
      image={img}
      x={-widPx / 2}
      y={-lenPx / 2}
      width={widPx}
      height={lenPx}
      offsetX={widPx / 2}
      offsetY={lenPx / 2}
      scaleY={-1}
      opacity={opacity}
      listening={false}
    />
  )
}

function FallbackVehicle({
  widPx,
  lenPx,
  stopped,
}: {
  widPx: number
  lenPx: number
  stopped: boolean
}) {
  return (
    <Rect
      x={-widPx / 2}
      y={-lenPx / 2}
      width={widPx}
      height={lenPx}
      fill={stopped ? '#F87171' : '#38BDF8'}
      cornerRadius={Math.min(widPx * 0.15, 2)}
      stroke="rgba(255,255,255,0.4)"
      strokeWidth={0.6}
    />
  )
}

/** Container = tractor head + trailer box (matches SUMO carriageImages). */
function ContainerSprite({
  images,
  widPx,
  lenPx,
}: {
  images: Map<string, HTMLImageElement>
  widPx: number
  lenPx: number
}) {
  const tractor = images.get('container') ?? images.get('truck')
  const trailer = images.get('trailer_box')
  const totalM = CONTAINER_LOCOMOTIVE_M + CONTAINER_GAP_M + CONTAINER_TRAILER_M
  const tractorLen = (CONTAINER_LOCOMOTIVE_M / totalM) * lenPx
  const gapLen = (CONTAINER_GAP_M / totalM) * lenPx
  const trailerLen = lenPx - tractorLen - gapLen

  if (!tractor || !trailer) {
    return <FallbackVehicle widPx={widPx} lenPx={lenPx} stopped={false} />
  }

  // Local +y = forward (after parent rotation). Tractor at front (negative y half).
  const tractorCenterY = -lenPx / 2 + tractorLen / 2
  const trailerCenterY = -lenPx / 2 + tractorLen + gapLen + trailerLen / 2

  return (
    <>
      <Group y={tractorCenterY}>
        <VehicleImage img={tractor} widPx={widPx} lenPx={tractorLen} />
      </Group>
      <Group y={trailerCenterY}>
        <VehicleImage img={trailer} widPx={widPx} lenPx={trailerLen} />
      </Group>
    </>
  )
}

/** Top-down vehicle PNG sized in meters × canvas scale. */
export function SumoVehicleSprite({
  type,
  lengthM,
  widthM,
  speed,
  scale,
  rotation,
  x,
  y,
  images,
}: VehicleRenderProps) {
  const stopped = speed < 0.1
  const lenPx = mpx(lengthM, scale, 1.5)
  const widPx = mpx(widthM, scale, 1)
  const t = type.toLowerCase()
  const img = spriteForType(t, images)

  return (
    <Group x={x} y={y} rotation={rotation} listening={false}>
      {t === 'container' ? (
        <ContainerSprite images={images} widPx={widPx} lenPx={lenPx} />
      ) : img ? (
        <VehicleImage img={img} widPx={widPx} lenPx={lenPx} opacity={stopped ? 0.88 : 1} />
      ) : (
        <FallbackVehicle widPx={widPx} lenPx={lenPx} stopped={stopped} />
      )}
    </Group>
  )
}
