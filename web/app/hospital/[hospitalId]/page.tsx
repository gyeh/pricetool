"use client"

import { use, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import dynamic from "next/dynamic"
import { useSearchParams } from "next/navigation"
import {
  AlertCircle,
  ArrowLeft,
  Building2,
  CheckCircle2,
  ExternalLink,
  MapPin,
  XCircle,
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

const HospitalMap = dynamic(() => import("@/components/hospital-map"), {
  ssr: false,
  loading: () => (
    <div className="h-[360px] flex items-center justify-center bg-muted/50 rounded-lg">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto mb-2" />
        <p className="text-sm text-muted-foreground">Loading map...</p>
      </div>
    </div>
  ),
})

interface MetricPoint {
  year: number
  value: number | null
}

interface HospitalInfo {
  hospital_id: number
  hospital_name: string
  hospital_address: string
  license_state: string | null
  last_updated_on: string | null
  has_inpatient: boolean
  has_outpatient: boolean
  year_founded: number | null
  metrics: {
    beds: MetricPoint[]
    physicians: MetricPoint[]
    employees: MetricPoint[]
    revenue: MetricPoint[]
  }
}

interface CodeInfo {
  code: string
  code_type: string
  description: string
}

interface StandardCharge {
  standard_charge_id: number
  setting: string
  gross_charge: number | null
  discounted_cash: number | null
  minimum: number | null
  maximum: number | null
  notes: string | null
}

interface PayerCharge {
  payer_charge_id: number
  standard_charge_id: number
  setting: string
  payer_name: string | null
  plan_name: string | null
  methodology: string | null
  negotiated_dollar: number | null
  estimated_amount: number | null
  median_amount: number | null
  percentile_10th: number | null
  percentile_90th: number | null
  count: string | null
  notes: string | null
}

interface PricingInfo {
  percentile_10th: number | null
  percentile_50th: number | null
  percentile_90th: number | null
  standard_charges: StandardCharge[]
  payer_charges: PayerCharge[]
}

interface HospitalPageData {
  hospital: HospitalInfo
  code: CodeInfo
  pricing: PricingInfo
}

function formatCurrency(value: number | null): string {
  if (value === null) return "N/A"
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(value)
}

function formatDate(value: string | null): string {
  if (!value) return "N/A"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "N/A"
  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(date)
}

function MetricHistoryChart({
  title,
  data,
  isCurrency,
}: {
  title: string
  data: MetricPoint[]
  isCurrency?: boolean
}) {
  const width = 320
  const height = 140
  const padding = 16

  const values = data
    .map((point) => point.value)
    .filter((value): value is number => value !== null)
  const hasData = values.length > 0

  const min = hasData ? Math.min(...values) : 0
  const max = hasData ? Math.max(...values) : 1
  const range = max - min || 1

  const points = data.map((point, index) => {
    const x = padding + (index / Math.max(data.length - 1, 1)) * (width - padding * 2)
    if (point.value === null || !hasData) {
      return { x, y: null as number | null }
    }
    const y = height - padding - ((point.value - min) / range) * (height - padding * 2)
    return { x, y }
  })

  let path = ""
  for (let i = 0; i < points.length; i += 1) {
    const point = points[i]
    if (point.y === null) continue
    path += path ? ` L ${point.x} ${point.y}` : `M ${point.x} ${point.y}`
  }

  return (
    <Card className="border-border/50 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-36">
          <line
            x1={padding}
            y1={height - padding}
            x2={width - padding}
            y2={height - padding}
            className="stroke-border"
            strokeWidth="1"
          />
          {hasData && path && (
            <>
              <path
                d={path}
                fill="none"
                className="stroke-primary"
                strokeWidth="2.5"
                strokeLinecap="round"
              />
              {points.map((point, index) =>
                point.y === null ? null : (
                  <circle
                    key={`${title}-point-${index}`}
                    cx={point.x}
                    cy={point.y}
                    r="3.5"
                    className="fill-primary"
                  />
                )
              )}
            </>
          )}
          {!hasData && (
            <text
              x={width / 2}
              y={height / 2}
              textAnchor="middle"
              className="fill-muted-foreground"
              style={{ fontSize: "12px" }}
            >
              No historical data available
            </text>
          )}
        </svg>
        <div className="mt-3 flex justify-between text-xs text-muted-foreground">
          {data.map((point) => (
            <span key={`${title}-${point.year}`}>{point.year}</span>
          ))}
        </div>
        {hasData && (
          <p className="mt-2 text-xs text-muted-foreground">
            Range: {isCurrency ? formatCurrency(min) : min.toLocaleString()} to{" "}
            {isCurrency ? formatCurrency(max) : max.toLocaleString()}
          </p>
        )}
      </CardContent>
    </Card>
  )
}

export default function HospitalPage({
  params,
}: {
  params: Promise<{ hospitalId: string }>
}) {
  const { hospitalId } = use(params)
  const searchParams = useSearchParams()
  const code = searchParams.get("code")
  const codeType = searchParams.get("type")
  const back = searchParams.get("back")

  const [data, setData] = useState<HospitalPageData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const backHref = useMemo(() => {
    if (back && back.startsWith("/")) return back
    if (code && codeType) {
      return `/service/${encodeURIComponent(code)}?type=${encodeURIComponent(codeType)}`
    }
    return "/"
  }, [back, code, codeType])

  useEffect(() => {
    if (!code || !codeType) {
      setError("Code context is missing. Open this page from a service result.")
      setLoading(false)
      return
    }

    const selectedCode = code
    const selectedCodeType = codeType

    async function fetchHospitalDetail() {
      try {
        const res = await fetch(
          `/api/hospital/${encodeURIComponent(hospitalId)}?code=${encodeURIComponent(selectedCode)}&type=${encodeURIComponent(selectedCodeType)}`
        )

        if (!res.ok) {
          if (res.status === 404) {
            setError("Hospital or code details were not found.")
          } else if (res.status === 400) {
            setError("Invalid request for hospital details.")
          } else {
            setError("Failed to load hospital details.")
          }
          return
        }

        const result = (await res.json()) as HospitalPageData
        setData(result)
      } catch (fetchError) {
        console.error("Hospital detail fetch failed:", fetchError)
        setError("Failed to load hospital details.")
      } finally {
        setLoading(false)
      }
    }

    fetchHospitalDetail()
  }, [hospitalId, code, codeType])

  if (loading) {
    return (
      <main className="min-h-screen">
        <div className="container mx-auto px-6 py-10">
          <div className="animate-pulse space-y-6">
            <div className="h-8 w-56 bg-muted rounded" />
            <div className="h-44 bg-muted rounded-xl" />
            <div className="grid md:grid-cols-2 gap-4">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-56 bg-muted rounded-xl" />
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
        <div className="container mx-auto px-6 py-10">
          <Link
            href={backHref}
            className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-8"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Search Results
          </Link>
          <Card className="max-w-lg">
            <CardContent className="pt-6">
              <AlertCircle className="w-10 h-10 text-muted-foreground mb-3" />
              <h1 className="text-xl font-semibold mb-2">Unable to load hospital page</h1>
              <p className="text-muted-foreground mb-6">{error ?? "Unknown error"}</p>
              <Button asChild>
                <Link href={backHref}>Return</Link>
              </Button>
            </CardContent>
          </Card>
        </div>
      </main>
    )
  }

  const mapPrice =
    data.pricing.standard_charges.find((charge) => charge.discounted_cash !== null)
      ?.discounted_cash ??
    data.pricing.standard_charges.find((charge) => charge.gross_charge !== null)?.gross_charge ??
    null

  return (
    <main className="min-h-screen pb-10">
      <div className="border-b border-border/50 bg-card/80 backdrop-blur-sm sticky top-0 z-30">
        <div className="container mx-auto px-6 py-3">
          <Link
            href={backHref}
            className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Search Results
          </Link>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8 space-y-8">
        <Card className="border-border/50 shadow-sm">
          <CardHeader>
            <CardTitle className="text-lg">Code Info</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-3 mb-3">
              <span className="px-3 py-1 text-sm font-medium rounded-full bg-primary/10 text-primary">
                {data.code.code_type}
              </span>
              <span className="px-3 py-1 text-sm font-medium rounded-full bg-primary/10 text-primary">
                {data.code.code}
              </span>
            </div>
            <p className="text-muted-foreground leading-relaxed">{data.code.description}</p>
          </CardContent>
        </Card>

        <Card className="border-border/50 shadow-sm">
          <CardHeader>
            <CardTitle className="text-xl flex items-center gap-2">
              <Building2 className="w-5 h-5" />
              {data.hospital.hospital_name}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid lg:grid-cols-5 gap-6">
              <div className="lg:col-span-3">
                <HospitalMap
                  hospitals={[
                    {
                      hospital_id: data.hospital.hospital_id,
                      hospital_name: data.hospital.hospital_name,
                      hospital_address: data.hospital.hospital_address,
                      discounted_cash: mapPrice,
                      gross_charge: mapPrice,
                    },
                  ]}
                  showCloseButton={false}
                />
              </div>
              <div className="lg:col-span-2 space-y-3">
                <div className="text-sm text-muted-foreground">Address</div>
                <a
                  href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(data.hospital.hospital_address)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-start gap-2 text-sm hover:text-primary"
                >
                  <MapPin className="w-4 h-4 mt-0.5 shrink-0" />
                  <span>{data.hospital.hospital_address || "N/A"}</span>
                  <ExternalLink className="w-3.5 h-3.5 mt-0.5 shrink-0" />
                </a>

                <div className="pt-2">
                  <div className="text-sm text-muted-foreground">Year founded</div>
                  <div className="font-medium">{data.hospital.year_founded ?? "N/A"}</div>
                </div>

                <div>
                  <div className="text-sm text-muted-foreground">Last updated</div>
                  <div className="font-medium">{formatDate(data.hospital.last_updated_on)}</div>
                </div>

                <div className="pt-2">
                  <div className="text-sm text-muted-foreground mb-2">Care settings</div>
                  <div className="flex flex-wrap gap-2">
                    <span
                      className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-medium ${
                        data.hospital.has_inpatient
                          ? "bg-green-100 text-green-700"
                          : "bg-muted text-muted-foreground"
                      }`}
                    >
                      {data.hospital.has_inpatient ? <CheckCircle2 className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
                      Inpatient
                    </span>
                    <span
                      className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-medium ${
                        data.hospital.has_outpatient
                          ? "bg-green-100 text-green-700"
                          : "bg-muted text-muted-foreground"
                      }`}
                    >
                      {data.hospital.has_outpatient ? <CheckCircle2 className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
                      Outpatient
                    </span>
                  </div>
                </div>
              </div>
            </div>

            <div className="grid md:grid-cols-2 gap-4">
              <MetricHistoryChart title="Beds (5 Years)" data={data.hospital.metrics.beds} />
              <MetricHistoryChart title="Physicians (5 Years)" data={data.hospital.metrics.physicians} />
              <MetricHistoryChart title="Employees (5 Years)" data={data.hospital.metrics.employees} />
              <MetricHistoryChart title="Revenue (5 Years)" data={data.hospital.metrics.revenue} isCurrency />
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/50 shadow-sm">
          <CardHeader>
            <CardTitle className="text-lg">Pricing</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid md:grid-cols-3 gap-4">
              <div className="rounded-lg border border-border/60 p-4">
                <div className="text-xs uppercase tracking-wide text-muted-foreground mb-1">10th Percentile</div>
                <div className="text-xl font-semibold">{formatCurrency(data.pricing.percentile_10th)}</div>
              </div>
              <div className="rounded-lg border border-border/60 p-4">
                <div className="text-xs uppercase tracking-wide text-muted-foreground mb-1">50th Percentile</div>
                <div className="text-xl font-semibold">{formatCurrency(data.pricing.percentile_50th)}</div>
              </div>
              <div className="rounded-lg border border-border/60 p-4">
                <div className="text-xs uppercase tracking-wide text-muted-foreground mb-1">90th Percentile</div>
                <div className="text-xl font-semibold">{formatCurrency(data.pricing.percentile_90th)}</div>
              </div>
            </div>

            <div>
              <h3 className="text-sm font-semibold mb-3">Standard Charges</h3>
              {data.pricing.standard_charges.length === 0 ? (
                <p className="text-sm text-muted-foreground">No standard charges found.</p>
              ) : (
                <div className="overflow-x-auto rounded-lg border border-border/60">
                  <table className="min-w-full text-sm">
                    <thead className="bg-muted/40">
                      <tr className="text-left">
                        <th className="px-3 py-2 font-medium">Setting</th>
                        <th className="px-3 py-2 font-medium">Cash</th>
                        <th className="px-3 py-2 font-medium">Gross</th>
                        <th className="px-3 py-2 font-medium">Minimum</th>
                        <th className="px-3 py-2 font-medium">Maximum</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.pricing.standard_charges.map((charge) => (
                        <tr key={charge.standard_charge_id} className="border-t border-border/40">
                          <td className="px-3 py-2">{charge.setting || "N/A"}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.discounted_cash)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.gross_charge)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.minimum)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.maximum)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div>
              <h3 className="text-sm font-semibold mb-3">Payer Negotiated Charges</h3>
              {data.pricing.payer_charges.length === 0 ? (
                <p className="text-sm text-muted-foreground">No payer-specific charges found.</p>
              ) : (
                <div className="overflow-x-auto rounded-lg border border-border/60">
                  <table className="min-w-full text-sm">
                    <thead className="bg-muted/40">
                      <tr className="text-left">
                        <th className="px-3 py-2 font-medium">Setting</th>
                        <th className="px-3 py-2 font-medium">Payer</th>
                        <th className="px-3 py-2 font-medium">Plan</th>
                        <th className="px-3 py-2 font-medium">Method</th>
                        <th className="px-3 py-2 font-medium">Negotiated</th>
                        <th className="px-3 py-2 font-medium">Estimated</th>
                        <th className="px-3 py-2 font-medium">Median</th>
                        <th className="px-3 py-2 font-medium">P10</th>
                        <th className="px-3 py-2 font-medium">P90</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.pricing.payer_charges.map((charge) => (
                        <tr key={charge.payer_charge_id} className="border-t border-border/40">
                          <td className="px-3 py-2">{charge.setting || "N/A"}</td>
                          <td className="px-3 py-2">{charge.payer_name || "N/A"}</td>
                          <td className="px-3 py-2">{charge.plan_name || "N/A"}</td>
                          <td className="px-3 py-2">{charge.methodology || "N/A"}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.negotiated_dollar)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.estimated_amount)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.median_amount)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.percentile_10th)}</td>
                          <td className="px-3 py-2">{formatCurrency(charge.percentile_90th)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </main>
  )
}
