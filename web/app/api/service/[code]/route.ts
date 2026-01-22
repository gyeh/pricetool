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
        pc.plan_name,
        pc.standard_charge_dollar as negotiated_dollar,
        pc.methodology
      FROM codes c
      JOIN item_codes ic ON ic.code_id = c.id
      JOIN standard_charge_items sci ON sci.id = ic.item_id
      JOIN hospitals h ON h.id = sci.hospital_id
      JOIN standard_charges sc ON sc.item_id = sci.id
      LEFT JOIN payer_charges pc ON pc.standard_charge_id = sc.id
      WHERE c.code = $1
        ${codeType ? "AND c.code_type = $2" : ""}
      ORDER BY
        sc.discounted_cash NULLS LAST,
        sc.gross_charge NULLS LAST,
        h.name
    `

    const priceParams = codeType ? [code, codeType] : [code]
    const prices = await query<HospitalPrice>(priceQuery, priceParams)

    // Group by hospital, taking best discounted cash price
    const hospitalMap = new Map<number, HospitalPrice>()
    for (const price of prices) {
      const existing = hospitalMap.get(price.hospital_id)
      if (!existing) {
        hospitalMap.set(price.hospital_id, price)
      } else if (
        price.discounted_cash !== null &&
        (existing.discounted_cash === null ||
          price.discounted_cash < existing.discounted_cash)
      ) {
        hospitalMap.set(price.hospital_id, price)
      }
    }

    const hospitalPrices = Array.from(hospitalMap.values()).sort((a, b) => {
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
