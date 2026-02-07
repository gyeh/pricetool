"use client"

import { useEffect, useState, useRef } from "react"
import mapboxgl from "mapbox-gl"
import { X, MapPin } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

const MAPBOX_TOKEN =
  "pk.eyJ1Ijoia2RqZmthZHNqZjg4OGEiLCJhIjoiY21sYnN3ZTVnMHN1eDNocTRneXB5cHNtciJ9.tWRIMX9jTaDM-MgKVIlldw"

interface Hospital {
  hospital_id: number
  hospital_name: string
  hospital_address: string
  discounted_cash: number | null
  gross_charge: number | null
}

interface GeocodedHospital extends Hospital {
  lat: number
  lng: number
}

interface HospitalMapProps {
  hospitals: Hospital[]
  onClose?: () => void
  onHospitalClick?: (hospitalId: number) => void
  showCloseButton?: boolean
}

// Cache for geocoded addresses
const geocodeCache = new Map<string, { lat: number; lng: number } | null>()

async function geocodeAddress(
  address: string
): Promise<{ lat: number; lng: number } | null> {
  if (geocodeCache.has(address)) {
    return geocodeCache.get(address) || null
  }

  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(address)}&limit=1`,
      {
        headers: {
          "User-Agent": "PriceTool/1.0",
        },
      }
    )

    if (!response.ok) {
      geocodeCache.set(address, null)
      return null
    }

    const data = await response.json()
    if (data && data.length > 0) {
      const result = { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon) }
      geocodeCache.set(address, result)
      return result
    }

    geocodeCache.set(address, null)
    return null
  } catch (error) {
    console.error("Geocoding error:", error)
    geocodeCache.set(address, null)
    return null
  }
}

function formatPrice(price: number | null): string {
  if (price === null) return "N/A"
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(price)
}

function createPriceMarkerElement(hospital: Hospital): HTMLDivElement {
  const el = document.createElement("div")
  el.style.cursor = "pointer"

  const price = hospital.discounted_cash ?? hospital.gross_charge
  const isCash = hospital.discounted_cash !== null
  const label = price !== null ? formatPrice(price) : "N/A"
  const bg = price === null ? "#9ca3af" : isCash ? "#16a34a" : "#6b7280"

  el.innerHTML = `
    <div style="
      background: ${bg};
      color: white;
      padding: 2px 7px;
      border-radius: 9999px;
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
      box-shadow: 0 1px 4px rgba(0,0,0,0.3);
      border: 2px solid white;
      line-height: 1.4;
    ">${label}</div>
  `
  return el
}

export default function HospitalMap({
  hospitals,
  onClose,
  onHospitalClick,
  showCloseButton = true,
}: HospitalMapProps) {
  const mapContainerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<mapboxgl.Map | null>(null)
  const markersRef = useRef<mapboxgl.Marker[]>([])
  const [geocodedHospitals, setGeocodedHospitals] = useState<GeocodedHospital[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Geocode hospitals
  useEffect(() => {
    let cancelled = false

    async function geocodeHospitals() {
      setLoading(true)
      setError(null)

      const results: GeocodedHospital[] = []

      for (const hospital of hospitals) {
        if (cancelled) break
        if (!hospital.hospital_address) continue

        const coords = await geocodeAddress(hospital.hospital_address)
        if (coords) {
          results.push({
            ...hospital,
            lat: coords.lat,
            lng: coords.lng,
          })
        }

        // Rate limit: 1 request per second for Nominatim
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }

      if (!cancelled) {
        setGeocodedHospitals(results)
        setLoading(false)

        if (results.length === 0 && hospitals.length > 0) {
          setError("Could not locate any hospitals on the map")
        }
      }
    }

    geocodeHospitals()

    return () => {
      cancelled = true
    }
  }, [hospitals])

  // Initialize map
  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return

    mapboxgl.accessToken = MAPBOX_TOKEN

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: "mapbox://styles/mapbox/light-v11",
      center: [-74.006, 40.7128], // NYC default
      zoom: 10,
    })

    map.addControl(new mapboxgl.NavigationControl(), "top-right")
    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  // Stable callback ref for onHospitalClick
  const onHospitalClickRef = useRef(onHospitalClick)
  useEffect(() => {
    onHospitalClickRef.current = onHospitalClick
  }, [onHospitalClick])

  // Place markers when geocoded hospitals change
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    // Remove old markers
    markersRef.current.forEach((m) => m.remove())
    markersRef.current = []

    if (geocodedHospitals.length === 0) return

    const bounds = new mapboxgl.LngLatBounds()

    for (const hospital of geocodedHospitals) {
      const el = createPriceMarkerElement(hospital)

      const price = hospital.discounted_cash ?? hospital.gross_charge
      const isCash = hospital.discounted_cash !== null
      const priceLabel =
        price !== null ? formatPrice(price) : "Price not available"
      const priceType = price !== null ? (isCash ? "Cash Price" : "Gross Charge") : ""

      const popupHTML = `
        <div style="min-width:180px;font-family:system-ui,sans-serif;">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;">${hospital.hospital_name}</div>
          <div style="font-size:12px;color:#666;margin-bottom:8px;">${hospital.hospital_address}</div>
          <div style="border-top:1px solid #e5e7eb;padding-top:8px;">
            ${price !== null
              ? `<div style="font-size:12px;color:#666;">${priceType}</div>
                 <div style="font-size:16px;font-weight:700;color:${isCash ? "#16a34a" : "#374151"};">${priceLabel}</div>`
              : `<div style="font-size:13px;color:#9ca3af;">Price not available</div>`
            }
          </div>
        </div>
      `

      const popup = new mapboxgl.Popup({ offset: 15, maxWidth: "260px" }).setHTML(
        popupHTML
      )

      const marker = new mapboxgl.Marker({ element: el })
        .setLngLat([hospital.lng, hospital.lat])
        .setPopup(popup)
        .addTo(map)

      el.addEventListener("click", () => {
        onHospitalClickRef.current?.(hospital.hospital_id)
      })

      bounds.extend([hospital.lng, hospital.lat])
      markersRef.current.push(marker)
    }

    // Fit bounds once map is loaded
    const fit = () => map.fitBounds(bounds, { padding: 50, maxZoom: 14 })
    if (map.loaded()) {
      fit()
    } else {
      map.once("load", fit)
    }
  }, [geocodedHospitals])

  return (
    <Card className="h-full flex flex-col border-border/50 shadow-lg">
      <CardHeader className="flex-row items-center justify-between space-y-0 pb-3 border-b border-border/50">
        <CardTitle className="text-base font-semibold flex items-center gap-2">
          <MapPin className="w-4 h-4" />
          Hospital Locations
        </CardTitle>
        {showCloseButton && onClose && (
          <Button variant="ghost" size="sm" onClick={onClose} className="h-8 w-8 p-0">
            <X className="w-4 h-4" />
          </Button>
        )}
      </CardHeader>
      <CardContent className="flex-1 p-0 relative">
        {loading && (
          <div className="absolute inset-0 bg-background/80 z-10 flex items-center justify-center">
            <div className="text-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto mb-2" />
              <p className="text-sm text-muted-foreground">
                Locating hospitals...
              </p>
            </div>
          </div>
        )}

        {error && !loading && (
          <div className="absolute inset-0 bg-background z-10 flex items-center justify-center p-4">
            <p className="text-sm text-muted-foreground text-center">{error}</p>
          </div>
        )}

        <div
          ref={mapContainerRef}
          className="h-full w-full rounded-b-lg"
          style={{ minHeight: "400px" }}
        />
      </CardContent>
    </Card>
  )
}
