import { NextRequest, NextResponse } from "next/server"
import { query } from "@/lib/db"

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

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ code: string }> }
) {
  const { code } = await params
  const searchParams = request.nextUrl.searchParams
  const codeType = searchParams.get("type") || null

  if (!code) {
    return NextResponse.json({ error: "Code is required" }, { status: 400 })
  }

  try {
    // Get service info
    const serviceQuery = codeType
      ? `SELECT DISTINCT c.code, c.code_type, sci.description
         FROM codes c
         JOIN item_codes ic ON ic.code_id = c.id
         JOIN standard_charge_items sci ON sci.id = ic.item_id
         WHERE c.code = $1 AND c.code_type = $2
         LIMIT 1`
      : `SELECT DISTINCT c.code, c.code_type, sci.description
         FROM codes c
         JOIN item_codes ic ON ic.code_id = c.id
         JOIN standard_charge_items sci ON sci.id = ic.item_id
         WHERE c.code = $1
         LIMIT 1`

    const serviceParams = codeType ? [code, codeType] : [code]
    const serviceResults = await query<ServiceInfo>(serviceQuery, serviceParams)

    if (serviceResults.length === 0) {
      return NextResponse.json({ error: "Service not found" }, { status: 404 })
    }

    const service = serviceResults[0]

    // Get hospital prices - prioritize discounted cash prices
    const priceQuery = `
      SELECT DISTINCT
        h.id as hospital_id,
        h.name as hospital_name,
        COALESCE(h.addresses[1], '') as hospital_address,
        sc.setting,
        sc.gross_charge,
        sc.discounted_cash,
        sc.minimum,
        sc.maximum,
        pc.payer_name,
        p.name as plan_name,
        pc.standard_charge_dollar as negotiated_dollar,
        pc.methodology,
        pc.estimated_amount
      FROM codes c
      JOIN item_codes ic ON ic.code_id = c.id
      JOIN standard_charge_items sci ON sci.id = ic.item_id
      JOIN hospitals h ON h.id = sci.hospital_id
      JOIN standard_charges sc ON sc.item_id = sci.id
      LEFT JOIN payer_charges pc ON pc.standard_charge_id = sc.id
      LEFT JOIN plans p ON p.id = pc.plan_id
      WHERE c.code = $1
        ${codeType ? "AND c.code_type = $2" : ""}
      ORDER BY
        sc.discounted_cash NULLS LAST,
        sc.gross_charge NULLS LAST,
        h.name
    `

    const priceParams = codeType ? [code, codeType] : [code]
    interface RawPrice extends Omit<HospitalPrice, 'median_estimate' | 'plan_count'> {
      estimated_amount: number | null
    }
    const prices = await query<RawPrice>(priceQuery, priceParams)

    // Group by hospital, taking best discounted cash price and collecting all estimates
    interface HospitalData {
      price: RawPrice
      estimates: number[]
    }
    const hospitalMap = new Map<number, HospitalData>()

    for (const price of prices) {
      const existing = hospitalMap.get(price.hospital_id)
      const estimate = price.estimated_amount !== null ? Number(price.estimated_amount) : null

      if (!existing) {
        hospitalMap.set(price.hospital_id, {
          price,
          estimates: estimate !== null ? [estimate] : [],
        })
      } else {
        // Update best discounted cash price
        if (
          price.discounted_cash !== null &&
          (existing.price.discounted_cash === null ||
            price.discounted_cash < existing.price.discounted_cash)
        ) {
          existing.price = price
        }

        // Collect all estimated amounts
        if (estimate !== null) {
          existing.estimates.push(estimate)
        }
      }
    }

    // Helper to calculate median
    function calculateMedian(values: number[]): number | null {
      if (values.length === 0) return null
      const sorted = [...values].sort((a, b) => a - b)
      const mid = Math.floor(sorted.length / 2)
      return sorted.length % 2 !== 0
        ? sorted[mid]
        : (sorted[mid - 1] + sorted[mid]) / 2
    }

    const hospitalPrices: HospitalPrice[] = Array.from(hospitalMap.values())
      .map(({ price, estimates }) => ({
        ...price,
        median_estimate: calculateMedian(estimates),
        plan_count: estimates.length,
      }))
      .sort((a, b) => {
        const priceA = a.discounted_cash ?? a.gross_charge ?? Infinity
        const priceB = b.discounted_cash ?? b.gross_charge ?? Infinity
        return priceA - priceB
      })

    return NextResponse.json({
      service,
      prices: hospitalPrices,
    })
  } catch (error) {
    console.error("Service error:", error)
    return NextResponse.json(
      { error: "Failed to get service information" },
      { status: 500 }
    )
  }
}
