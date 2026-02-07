import { NextRequest, NextResponse } from "next/server"
import { query } from "@/lib/db"
import { getCodeDescription } from "@/lib/code-descriptions"

interface HospitalBase {
  hospital_id: number
  hospital_name: string
  hospital_address: string
  license_state: string | null
  last_updated_on: string | null
  has_inpatient: boolean
  has_outpatient: boolean
}

interface CodeInfoRow {
  code: string
  code_type: string
  description: string
}

interface PriceRow {
  standard_charge_id: number
  setting: string
  gross_charge: number | null
  discounted_cash: number | null
  minimum: number | null
  maximum: number | null
  standard_charge_notes: string | null
  payer_charge_id: number | null
  payer_name: string | null
  plan_name: string | null
  methodology: string | null
  negotiated_dollar: number | null
  estimated_amount: number | null
  median_amount: number | null
  percentile_10th: number | null
  percentile_90th: number | null
  count: string | null
  payer_notes: string | null
}

interface MetricPoint {
  year: number
  value: number | null
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

function getLastFiveYearSeries(): MetricPoint[] {
  const currentYear = new Date().getFullYear()
  const series: MetricPoint[] = []
  for (let year = currentYear - 4; year <= currentYear; year += 1) {
    series.push({ year, value: null })
  }
  return series
}

function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null
  if (values.length === 1) return values[0]

  const sorted = [...values].sort((a, b) => a - b)
  const index = (sorted.length - 1) * p
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) {
    return sorted[lower]
  }

  const weight = index - lower
  return sorted[lower] + (sorted[upper] - sorted[lower]) * weight
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ hospitalId: string }> }
) {
  const { hospitalId } = await params
  const searchParams = request.nextUrl.searchParams
  const code = searchParams.get("code")
  const codeType = searchParams.get("type")

  const parsedHospitalId = Number.parseInt(hospitalId, 10)
  if (!Number.isFinite(parsedHospitalId)) {
    return NextResponse.json({ error: "Invalid hospital id" }, { status: 400 })
  }
  if (!code || !codeType) {
    return NextResponse.json(
      { error: "Both code and type query params are required" },
      { status: 400 }
    )
  }

  try {
    const hospitalRows = await query<HospitalBase>(
      `
      SELECT
        h.id AS hospital_id,
        h.name AS hospital_name,
        COALESCE(h.addresses[1], '') AS hospital_address,
        h.license_state,
        h.last_updated_on::text AS last_updated_on,
        COALESCE(BOOL_OR(LOWER(sc.setting) IN ('inpatient', 'both')), false) AS has_inpatient,
        COALESCE(BOOL_OR(LOWER(sc.setting) IN ('outpatient', 'both')), false) AS has_outpatient
      FROM hospitals h
      LEFT JOIN standard_charge_items sci ON sci.hospital_id = h.id
      LEFT JOIN standard_charges sc ON sc.item_id = sci.id
      WHERE h.id = $1
      GROUP BY h.id, h.name, h.addresses, h.license_state, h.last_updated_on
      `,
      [parsedHospitalId]
    )

    if (hospitalRows.length === 0) {
      return NextResponse.json({ error: "Hospital not found" }, { status: 404 })
    }

    const codeRows = await query<CodeInfoRow>(
      `
      SELECT DISTINCT
        c.code,
        c.code_type,
        sci.description
      FROM standard_charge_items sci
      JOIN item_codes ic ON ic.item_id = sci.id
      JOIN codes c ON c.id = ic.code_id
      WHERE sci.hospital_id = $1
        AND c.code = $2
        AND c.code_type = $3
      LIMIT 1
      `,
      [parsedHospitalId, code, codeType]
    )

    if (codeRows.length === 0) {
      return NextResponse.json(
        { error: "Code not found for this hospital" },
        { status: 404 }
      )
    }

    const codeRow = codeRows[0]
    const codeDescription =
      getCodeDescription(codeRow.code_type, codeRow.code) ?? codeRow.description

    const priceRows = await query<PriceRow>(
      `
      SELECT
        sc.id AS standard_charge_id,
        sc.setting,
        sc.gross_charge,
        sc.discounted_cash,
        sc.minimum,
        sc.maximum,
        sc.additional_notes AS standard_charge_notes,
        pc.id AS payer_charge_id,
        pc.payer_name,
        p.name AS plan_name,
        pc.methodology,
        pc.standard_charge_dollar AS negotiated_dollar,
        pc.estimated_amount,
        pc.median_amount,
        pc.percentile_10th,
        pc.percentile_90th,
        pc.count,
        pc.additional_notes AS payer_notes
      FROM standard_charge_items sci
      JOIN item_codes ic ON ic.item_id = sci.id
      JOIN codes c ON c.id = ic.code_id
      JOIN standard_charges sc ON sc.item_id = sci.id
      LEFT JOIN payer_charges pc ON pc.standard_charge_id = sc.id
      LEFT JOIN plans p ON p.id = pc.plan_id
      WHERE sci.hospital_id = $1
        AND c.code = $2
        AND c.code_type = $3
      ORDER BY
        sc.setting,
        sc.discounted_cash NULLS LAST,
        sc.gross_charge NULLS LAST,
        pc.payer_name NULLS LAST,
        p.name NULLS LAST
      `,
      [parsedHospitalId, code, codeType]
    )

    const standardChargeMap = new Map<number, StandardCharge>()
    const payerCharges: PayerCharge[] = []
    const payerAmounts: number[] = []

    for (const row of priceRows) {
      if (!standardChargeMap.has(row.standard_charge_id)) {
        standardChargeMap.set(row.standard_charge_id, {
          standard_charge_id: row.standard_charge_id,
          setting: row.setting,
          gross_charge: row.gross_charge,
          discounted_cash: row.discounted_cash,
          minimum: row.minimum,
          maximum: row.maximum,
          notes: row.standard_charge_notes,
        })
      }

      if (row.payer_charge_id !== null) {
        payerCharges.push({
          payer_charge_id: row.payer_charge_id,
          standard_charge_id: row.standard_charge_id,
          setting: row.setting,
          payer_name: row.payer_name,
          plan_name: row.plan_name,
          methodology: row.methodology,
          negotiated_dollar: row.negotiated_dollar,
          estimated_amount: row.estimated_amount,
          median_amount: row.median_amount,
          percentile_10th: row.percentile_10th,
          percentile_90th: row.percentile_90th,
          count: row.count,
          notes: row.payer_notes,
        })

        const payerAmount =
          row.negotiated_dollar ?? row.estimated_amount ?? row.median_amount
        if (payerAmount !== null) {
          payerAmounts.push(Number(payerAmount))
        }
      }
    }

    return NextResponse.json({
      hospital: {
        ...hospitalRows[0],
        year_founded: null,
        metrics: {
          beds: getLastFiveYearSeries(),
          physicians: getLastFiveYearSeries(),
          employees: getLastFiveYearSeries(),
          revenue: getLastFiveYearSeries(),
        },
      },
      code: {
        code: codeRow.code,
        code_type: codeRow.code_type,
        description: codeDescription,
      },
      pricing: {
        percentile_10th: percentile(payerAmounts, 0.1),
        percentile_50th: percentile(payerAmounts, 0.5),
        percentile_90th: percentile(payerAmounts, 0.9),
        standard_charges: Array.from(standardChargeMap.values()),
        payer_charges: payerCharges,
      },
    })
  } catch (error) {
    console.error("Hospital detail error:", error)
    return NextResponse.json(
      { error: "Failed to load hospital detail" },
      { status: 500 }
    )
  }
}
