"use client"

import { useEffect, useState, useRef } from "react"
import { MapContainer, TileLayer, Marker, Popup, useMap } from "react-leaflet"
import L from "leaflet"
import { X, MapPin } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

// Fix for default marker icons in Leaflet with Next.js
const defaultIcon = L.icon({
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41],
})

L.Marker.prototype.options.icon = defaultIcon

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
  onClose: () => void
}

// Component to fit map bounds to markers
function FitBounds({ positions }: { positions: [number, number][] }) {
  const map = useMap()

  useEffect(() => {
    if (positions.length > 0) {
      const bounds = L.latLngBounds(positions.map(([lat, lng]) => [lat, lng]))
      map.fitBounds(bounds, { padding: [50, 50] })
    }
  }, [map, positions])

  return null
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
    maximumFractionDigits: 2,
  }).format(price)
}

export default function HospitalMap({ hospitals, onClose }: HospitalMapProps) {
  const [geocodedHospitals, setGeocodedHospitals] = useState<GeocodedHospital[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    async function geocodeHospitals() {
      setLoading(true)
      setError(null)

      const results: GeocodedHospital[] = []

      // Geocode addresses sequentially to respect Nominatim rate limits
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

  const positions: [number, number][] = geocodedHospitals.map((h) => [h.lat, h.lng])

  // Default center (NYC area) if no hospitals yet
  const defaultCenter: [number, number] = [40.7128, -74.006]
  const center =
    positions.length > 0
      ? ([
          positions.reduce((sum, p) => sum + p[0], 0) / positions.length,
          positions.reduce((sum, p) => sum + p[1], 0) / positions.length,
        ] as [number, number])
      : defaultCenter

  return (
    <Card className="h-full flex flex-col border-border/50 shadow-lg">
      <CardHeader className="flex-row items-center justify-between space-y-0 pb-3 border-b border-border/50">
        <CardTitle className="text-base font-semibold flex items-center gap-2">
          <MapPin className="w-4 h-4" />
          Hospital Locations
        </CardTitle>
        <Button variant="ghost" size="sm" onClick={onClose} className="h-8 w-8 p-0">
          <X className="w-4 h-4" />
        </Button>
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

        <MapContainer
          center={center}
          zoom={10}
          className="h-full w-full rounded-b-lg"
          style={{ minHeight: "400px" }}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {geocodedHospitals.map((hospital) => (
            <Marker key={hospital.hospital_id} position={[hospital.lat, hospital.lng]}>
              <Popup>
                <div className="min-w-[200px]">
                  <h3 className="font-semibold text-sm mb-1">
                    {hospital.hospital_name}
                  </h3>
                  <p className="text-xs text-gray-600 mb-2">
                    {hospital.hospital_address}
                  </p>
                  <div className="border-t pt-2">
                    {hospital.discounted_cash !== null ? (
                      <p className="text-sm">
                        <span className="text-gray-600">Cash Price: </span>
                        <span className="font-semibold text-green-600">
                          {formatPrice(hospital.discounted_cash)}
                        </span>
                      </p>
                    ) : hospital.gross_charge !== null ? (
                      <p className="text-sm">
                        <span className="text-gray-600">Gross Charge: </span>
                        <span className="font-semibold">
                          {formatPrice(hospital.gross_charge)}
                        </span>
                      </p>
                    ) : (
                      <p className="text-sm text-gray-500">Price not available</p>
                    )}
                  </div>
                </div>
              </Popup>
            </Marker>
          ))}

          {positions.length > 0 && <FitBounds positions={positions} />}
        </MapContainer>
      </CardContent>
    </Card>
  )
}
