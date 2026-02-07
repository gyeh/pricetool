"use client"

import { useEffect, useState, use } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import Link from "next/link"
import dynamic from "next/dynamic"
import {
  ArrowLeft,
  Building2,
  DollarSign,
  MapPin,
  TrendingDown,
  AlertCircle,
  Map as MapIcon,
  Search,
  ArrowUpDown,
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

const codeTypeDescriptions = new Map<string, string>([
  ["CPT", "Standard codes for medical procedures and services performed by clinicians."],
  ["HCPCS", "Codes for medical services supplies and equipment used for billing."],
  ["ICD", "Codes that describe diagnoses and medical conditions being treated."],
  ["DRG", "Hospital inpatient case groups used to determine payment for a stay."],
  ["MS-DRG", "Medicare severity-adjusted DRG groups for inpatient hospital payment."],
  ["R-DRG", "Refined DRG categories that add more clinical detail than basic DRGs."],
  ["S-DRG", "Severity-based DRG categories that account for how sick the patient is."],
  ["APS-DRG", "All-patient severity-adjusted DRG groups used across broader populations."],
  ["AP-DRG", "All-patient DRG categories for grouping inpatient hospital stays."],
  ["APR-DRG", "All-patient refined DRG groups with severity and mortality risk levels."],
  ["TRIS-DRG", "TRICARE DRG categories used for military health system payment."],
  ["APC", "Outpatient hospital payment categories used by Medicare."],
  ["NDC", "National Drug Code that identifies a specific medication and package."],
  ["HIPPS", "Codes used for payment in post-acute care settings like rehab and skilled nursing."],
  ["LOCAL", "Hospital-specific internal billing code used when no standard code fits."],
  ["EAPG", "Enhanced outpatient grouping codes used to price outpatient services."],
  ["CDT", "Standard codes for dental procedures and services."],
  ["RC", "Revenue center code that identifies the hospital department or service area billed."],
  ["CDM", "Hospital chargemaster code for a billable item service or supply."],
  ["CMG", "Case-mix group used to classify rehabilitation cases for payment."],
  ["MS-LTC-DRG", "Medicare severity DRG categories tailored to long-term care hospitals."],
])

function getCodeTypeDescription(codeType: string): string | null {
  return codeTypeDescriptions.get(codeType.trim().toUpperCase()) ?? null
}

function CodeTypeBadge({
  codeType,
  className,
}: {
  codeType: string
  className: string
}) {
  const description = getCodeTypeDescription(codeType)

  return (
    <span className="relative inline-flex group">
      <span className={className}>
        {codeType}
      </span>
      {description && (
        <span
          role="tooltip"
          className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 w-72 -translate-x-1/2 rounded-md border border-border/60 bg-card px-3 py-2 text-left text-xs font-normal text-card-foreground shadow-lg opacity-0 transition-opacity duration-150 group-hover:opacity-100"
        >
          {description}
        </span>
      )}
    </span>
  )
}

// Dynamic import to avoid SSR issues with Leaflet
const HospitalMap = dynamic(() => import("@/components/hospital-map"), {
  ssr: false,
  loading: () => (
    <div className="h-full flex items-center justify-center bg-muted/50 rounded-lg">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto mb-2" />
        <p className="text-sm text-muted-foreground">Loading map...</p>
      </div>
    </div>
  ),
})

interface ServiceInfo {
  code: string
  code_type: string
  description: string
}

interface HospitalPrice {
  hospital_id: number
  hospital_name: string
  hospital_address: string
  setting: string
  gross_charge: number | null
  discounted_cash: number | null
  minimum: number | null
  maximum: number | null
  payer_name: string | null
  plan_name: string | null
  negotiated_dollar: number | null
  methodology: string | null
  median_estimate: number | null
  plan_count: number
}

interface ServiceData {
  service: ServiceInfo
  prices: HospitalPrice[]
}

export default function ServicePage({
  params,
}: {
  params: Promise<{ code: string }>
}) {
  const { code } = use(params)
  const searchParams = useSearchParams()
  const router = useRouter()
  const codeType = searchParams.get("type")

  const [data, setData] = useState<ServiceData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showMap, setShowMap] = useState(true)

  // Header search state
  const [headerCodeType, setHeaderCodeType] = useState(codeType || "CPT")
  const [headerCodeValue, setHeaderCodeValue] = useState(code)
  const [showProcedureSearch, setShowProcedureSearch] = useState(false)
  const [procedureQuery, setProcedureQuery] = useState("")
  const [procedureResults, setProcedureResults] = useState<SearchResult[]>([])
  const [isSearchingProcedure, setIsSearchingProcedure] = useState(false)
  const [showProcedureResults, setShowProcedureResults] = useState(false)
  const [highlightedHospitalId, setHighlightedHospitalId] = useState<number | null>(null)
  const [sortBy, setSortBy] = useState<"cash" | "gross" | "median">("cash")

  interface SearchResult {
    code: string
    code_type: string
    description: string
    hospital_count: number
  }

  useEffect(() => {
    async function fetchData() {
      try {
        const url = codeType
          ? `/api/service/${code}?type=${codeType}`
          : `/api/service/${code}`
        const res = await fetch(url)

        if (!res.ok) {
          if (res.status === 404) {
            setError("Service not found")
          } else {
            setError("Failed to load service data")
          }
          return
        }

        const result = await res.json()
        setData(result)
      } catch (err) {
        console.error("Fetch error:", err)
        setError("Failed to load service data")
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [code, codeType])

  // Procedure search with debounce
  useEffect(() => {
    if (procedureQuery.length < 2) {
      setProcedureResults([])
      return
    }
    const timer = setTimeout(async () => {
      setIsSearchingProcedure(true)
      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(procedureQuery)}`)
        const data = await res.json()
        setProcedureResults(data.results || [])
      } catch {
        setProcedureResults([])
      } finally {
        setIsSearchingProcedure(false)
      }
    }, 300)
    return () => clearTimeout(timer)
  }, [procedureQuery])

  const handleHeaderLookup = () => {
    const trimmed = headerCodeValue.trim()
    if (trimmed) {
      router.push(`/service/${encodeURIComponent(trimmed)}?type=${encodeURIComponent(headerCodeType)}`)
    }
  }

  const handleHeaderKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleHeaderLookup()
  }

  const handleHospitalClick = (hospitalId: number) => {
    const el = document.querySelector(`[data-hospital-id="${hospitalId}"]`)
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" })
    }
    setHighlightedHospitalId(hospitalId)
    setTimeout(() => setHighlightedHospitalId(null), 2000)
  }

  const formatPrice = (price: number | null) => {
    if (price === null) return "N/A"
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    }).format(price)
  }

  const getLowestPrice = () => {
    if (!data?.prices?.length) return null
    const prices = data.prices
      .map((p) => p.discounted_cash ?? p.gross_charge)
      .filter((p): p is number => p !== null)
    return prices.length > 0 ? Math.min(...prices) : null
  }

  const getHighestPrice = () => {
    if (!data?.prices?.length) return null
    const prices = data.prices
      .map((p) => p.discounted_cash ?? p.gross_charge)
      .filter((p): p is number => p !== null)
    return prices.length > 0 ? Math.max(...prices) : null
  }

  const getSortedPrices = () => {
    if (!data?.prices?.length) return []
    return [...data.prices].sort((a, b) => {
      let valA: number | null
      let valB: number | null

      switch (sortBy) {
        case "cash":
          valA = a.discounted_cash
          valB = b.discounted_cash
          break
        case "gross":
          valA = a.gross_charge
          valB = b.gross_charge
          break
        case "median":
          valA = a.median_estimate
          valB = b.median_estimate
          break
        default:
          valA = a.discounted_cash
          valB = b.discounted_cash
      }

      // Null values go to the end
      if (valA === null && valB === null) return 0
      if (valA === null) return 1
      if (valB === null) return -1
      return valA - valB
    })
  }

  if (loading) {
    return (
      <main className="min-h-screen">
        <div className="container mx-auto px-6 py-12">
          <div className="animate-pulse space-y-8">
            <div className="h-8 w-32 bg-muted rounded" />
            <div className="h-12 w-2/3 bg-muted rounded" />
            <div className="grid md:grid-cols-3 gap-4">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-32 bg-muted rounded-xl" />
              ))}
            </div>
            <div className="space-y-4">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-24 bg-muted rounded-xl" />
              ))}
            </div>
          </div>
        </div>
      </main>
    )
  }

  if (error || !data) {
    return (
      <main className="min-h-screen">
        <div className="container mx-auto px-6 py-12">
          <Button
            variant="ghost"
            className="mb-8"
            onClick={() => router.push("/")}
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Search
          </Button>

          <Card className="max-w-md mx-auto">
            <CardContent className="pt-6 text-center">
              <AlertCircle className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <h2 className="text-xl font-semibold mb-2">
                {error || "Service Not Found"}
              </h2>
              <p className="text-muted-foreground mb-6">
                We couldn&apos;t find pricing data for code &quot;{code}&quot;.
              </p>
              <Button onClick={() => router.push("/")}>Search Again</Button>
            </CardContent>
          </Card>
        </div>
      </main>
    )
  }

  const lowestPrice = getLowestPrice()
  const highestPrice = getHighestPrice()

  return (
    <main className="min-h-screen">
      {/* Decorative background */}
      <div className="absolute top-0 left-0 right-0 h-80 bg-gradient-to-b from-warm-100/50 to-transparent -z-10" />

      {/* Header bar */}
      <div className="border-b border-border/50 bg-card/80 backdrop-blur-sm sticky top-0 z-30">
        <div className="container mx-auto px-6 py-3">
          <div className="flex items-center gap-3">
            <Link
              href="/"
              className="text-muted-foreground hover:text-foreground transition-colors shrink-0"
              title="Back to home"
            >
              <ArrowLeft className="w-5 h-5" />
            </Link>

            <select
              value={headerCodeType}
              onChange={(e) => setHeaderCodeType(e.target.value)}
              className="shrink-0 w-28 py-2 px-3 text-sm rounded-lg border border-border/60 bg-background focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
            >
              {["CPT", "HCPCS", "MS-DRG", "NDC", "RC", "CDM", "ICD", "DRG", "LOCAL", "APC"].map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>

            <input
              type="text"
              placeholder="Code value"
              className="w-40 py-2 px-3 text-sm rounded-lg border border-border/60 bg-background focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
              value={headerCodeValue}
              onChange={(e) => setHeaderCodeValue(e.target.value)}
              onKeyDown={handleHeaderKeyDown}
            />

            <button
              onClick={handleHeaderLookup}
              disabled={!headerCodeValue.trim()}
              className="shrink-0 px-4 py-2 text-sm rounded-lg bg-primary text-primary-foreground font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Look up
            </button>

            <div className="h-6 w-px bg-border/60 mx-1" />

            <div className="relative flex-1 min-w-0">
              <button
                onClick={() => {
                  setShowProcedureSearch(!showProcedureSearch)
                  setShowProcedureResults(false)
                }}
                className="flex items-center gap-2 py-2 px-3 text-sm text-muted-foreground hover:text-foreground transition-colors"
              >
                <Search className="w-4 h-4" />
                Search by procedure
              </button>

              {showProcedureSearch && (
                <div className="absolute top-full left-0 right-0 mt-1 z-50">
                  <input
                    type="text"
                    placeholder="Search by procedure name..."
                    autoFocus
                    className="w-full py-2 px-3 text-sm rounded-lg border border-border/60 bg-background shadow-lg focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
                    value={procedureQuery}
                    onChange={(e) => {
                      setProcedureQuery(e.target.value)
                      setShowProcedureResults(true)
                    }}
                    onFocus={() => setShowProcedureResults(true)}
                    onBlur={() => setTimeout(() => {
                      setShowProcedureResults(false)
                      if (!procedureQuery) setShowProcedureSearch(false)
                    }, 200)}
                  />

                  {showProcedureResults && procedureResults.length > 0 && (
                    <Card className="mt-1 overflow-hidden shadow-lg border-border/50">
                      <CardContent className="p-0">
                        <ul className="divide-y divide-border/50 max-h-80 overflow-y-auto">
                          {procedureResults.map((result, index) => (
                            <li
                              key={`${result.code}-${result.code_type}-${index}`}
                              className="px-4 py-3 hover:bg-accent/50 cursor-pointer transition-colors"
                              onMouseDown={() => {
                                router.push(`/service/${result.code}?type=${result.code_type}`)
                                setShowProcedureSearch(false)
                                setProcedureQuery("")
                              }}
                            >
                              <div className="flex items-center gap-2">
                                <span className="font-semibold text-sm">{result.code}</span>
                                <CodeTypeBadge
                                  codeType={result.code_type}
                                  className="px-1.5 py-0.5 text-xs font-medium rounded-full bg-warm-100 text-warm-700"
                                />
                                <span className="text-sm text-muted-foreground truncate">
                                  {result.description}
                                </span>
                              </div>
                            </li>
                          ))}
                        </ul>
                      </CardContent>
                    </Card>
                  )}

                  {showProcedureResults && procedureQuery.length >= 2 && procedureResults.length === 0 && !isSearchingProcedure && (
                    <Card className="mt-1 shadow-lg border-border/50">
                      <CardContent className="p-4 text-center text-sm text-muted-foreground">
                        No results found
                      </CardContent>
                    </Card>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8">
        <div className="flex gap-6">
          {/* Main content */}
          <div className={showMap ? "flex-1 min-w-0" : "w-full"}>

            {/* Service Header */}
            <div className="mb-10">
              <div className="flex items-center gap-3 mb-4">
                <CodeTypeBadge
                  codeType={data.service.code_type}
                  className="px-3 py-1 text-sm font-medium rounded-full bg-primary/10 text-primary"
                />
                <span className="px-3 py-1 text-sm font-medium rounded-full bg-primary/10 text-primary">
                  {data.service.code}
                </span>
              </div>
              <h1 className="text-xl md:text-2xl text-muted-foreground max-w-3xl leading-relaxed">
                {data.service.description}
              </h1>
            </div>

            {/* Stats Cards */}
            <div className="grid md:grid-cols-3 gap-4 mb-10">
              <Card className="warm-card border-0">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-3 mb-2">
                    <div className="w-10 h-10 rounded-lg bg-green-100 flex items-center justify-center">
                      <TrendingDown className="w-5 h-5 text-green-600" />
                    </div>
                    <span className="text-sm font-medium text-muted-foreground">
                      Lowest Price
                    </span>
                  </div>
                  <p className="text-2xl font-bold text-green-600">
                    {formatPrice(lowestPrice)}
                  </p>
                </CardContent>
              </Card>

              <Card className="warm-card border-0">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-3 mb-2">
                    <div className="w-10 h-10 rounded-lg bg-amber-100 flex items-center justify-center">
                      <DollarSign className="w-5 h-5 text-amber-600" />
                    </div>
                    <span className="text-sm font-medium text-muted-foreground">
                      Highest Price
                    </span>
                  </div>
                  <p className="text-2xl font-bold text-amber-600">
                    {formatPrice(highestPrice)}
                  </p>
                </CardContent>
              </Card>

              <Card className="warm-card border-0">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-3 mb-2">
                    <div className="w-10 h-10 rounded-lg bg-blue-100 flex items-center justify-center">
                      <Building2 className="w-5 h-5 text-blue-600" />
                    </div>
                    <span className="text-sm font-medium text-muted-foreground">
                      Hospitals
                    </span>
                  </div>
                  <p className="text-2xl font-bold text-blue-600">
                    {data.prices.length}
                  </p>
                </CardContent>
              </Card>
            </div>

            {/* Hospital List */}
            <Card className="border-border/50 shadow-sm">
              <CardHeader className="border-b border-border/50 flex-row items-center justify-between space-y-0">
                <CardTitle className="text-lg font-semibold">
                  Hospital Pricing Comparison
                </CardTitle>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2">
                    <ArrowUpDown className="w-4 h-4 text-muted-foreground" />
                    <select
                      value={sortBy}
                      onChange={(e) => setSortBy(e.target.value as "cash" | "gross" | "median")}
                      className="py-1.5 px-2 text-sm rounded-md border border-border/60 bg-background focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
                    >
                      <option value="cash">Cash Price</option>
                      <option value="gross">Gross Price</option>
                      <option value="median">Insurance Estimate</option>
                    </select>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setShowMap(!showMap)}
                    className="hidden md:flex items-center gap-2"
                  >
                    <MapIcon className="w-4 h-4" />
                    {showMap ? "Hide Map" : "Show Map"}
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                {data.prices.length === 0 ? (
                  <div className="p-8 text-center text-muted-foreground">
                    No pricing data available for this service.
                  </div>
                ) : (
                  <div className="divide-y divide-border/50">
                    {getSortedPrices().map((price, index) => (
                      <HospitalRow
                        key={`${price.hospital_id}-${index}`}
                        price={price}
                        isLowest={
                          lowestPrice !== null &&
                          (price.discounted_cash ?? price.gross_charge) ===
                            lowestPrice
                        }
                        isHighlighted={highlightedHospitalId === price.hospital_id}
                        formatPrice={formatPrice}
                        serviceCode={data.service.code}
                        serviceCodeType={data.service.code_type}
                      />
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Disclaimer */}
            <p className="mt-8 text-sm text-muted-foreground text-center max-w-2xl mx-auto">
              Prices shown are based on hospital price transparency data. Actual
              costs may vary based on insurance coverage, specific services
              rendered, and other factors. Contact the hospital directly for
              accurate estimates.
            </p>
          </div>

          {/* Map Panel */}
          {showMap && (
            <div className="hidden md:block w-[400px] shrink-0">
              <div className="sticky top-8 h-[calc(100vh-4rem)]">
                <HospitalMap
                  hospitals={data.prices}
                  onClose={() => setShowMap(false)}
                  onHospitalClick={handleHospitalClick}
                />
              </div>
            </div>
          )}
        </div>
      </div>
    </main>
  )
}

function HospitalRow({
  price,
  isLowest,
  isHighlighted,
  formatPrice,
  serviceCode,
  serviceCodeType,
}: {
  price: HospitalPrice
  isLowest: boolean
  isHighlighted: boolean
  formatPrice: (price: number | null) => string
  serviceCode: string
  serviceCodeType: string
}) {
  const displayPrice = price.discounted_cash ?? price.gross_charge
  const backHref = `/service/${encodeURIComponent(serviceCode)}?type=${encodeURIComponent(serviceCodeType)}`
  const hospitalHref = `/hospital/${price.hospital_id}?code=${encodeURIComponent(serviceCode)}&type=${encodeURIComponent(serviceCodeType)}&back=${encodeURIComponent(backHref)}`

  return (
    <div
      data-hospital-id={price.hospital_id}
      className={`p-5 hover:bg-accent/30 transition-all duration-300 ${
        isHighlighted
          ? "ring-2 ring-primary bg-primary/10"
          : isLowest
            ? "bg-green-50/50"
            : ""
      }`}
    >
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1">
            <Link href={hospitalHref} className="font-semibold text-foreground hover:text-primary transition-colors">
              {price.hospital_name}
            </Link>
            {isLowest && (
              <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-green-100 text-green-700">
                Lowest
              </span>
            )}
          </div>
          {price.hospital_address && (
            <a
              href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(price.hospital_address)}`}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1 text-sm text-muted-foreground hover:text-primary transition-colors"
            >
              <MapPin className="w-3.5 h-3.5" />
              <span className="truncate max-w-md hover:underline">{price.hospital_address}</span>
            </a>
          )}
          {price.setting && (
            <span className="inline-block mt-2 px-2 py-0.5 text-xs rounded-full bg-secondary text-secondary-foreground">
              {price.setting}
            </span>
          )}

          {/* Median insurance estimate */}
          {price.plan_count > 0 && price.median_estimate !== null && (
            <div className="mt-3 pt-3 border-t border-border/30">
              <p className="text-xs text-muted-foreground uppercase tracking-wide mb-2">
                Insurance Estimate
              </p>
              <div className="text-sm">
                <span className="text-blue-600 font-semibold">
                  {formatPrice(price.median_estimate)}
                </span>
                <span className="text-muted-foreground ml-1">
                  (median across {price.plan_count} plan{price.plan_count !== 1 ? "s" : ""})
                </span>
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-col items-end gap-1">
          <div className="flex items-center gap-3">
            {price.discounted_cash !== null && (
              <div className="text-right">
                <p className="text-xs text-muted-foreground uppercase tracking-wide">
                  Cash Price
                </p>
                <p className="text-xl font-bold text-green-600">
                  {formatPrice(price.discounted_cash)}
                </p>
              </div>
            )}
            {price.gross_charge !== null &&
              price.gross_charge !== price.discounted_cash && (
                <div className="text-right">
                  <p className="text-xs text-muted-foreground uppercase tracking-wide">
                    Gross
                  </p>
                  <p
                    className={`text-lg font-semibold ${
                      price.discounted_cash !== null
                        ? "text-muted-foreground line-through"
                        : "text-foreground"
                    }`}
                  >
                    {formatPrice(price.gross_charge)}
                  </p>
                </div>
              )}
          </div>
          {displayPrice === null && (
            <p className="text-muted-foreground">Price not available</p>
          )}
        </div>
      </div>
    </div>
  )
}
