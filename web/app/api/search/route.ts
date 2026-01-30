import { NextRequest, NextResponse } from "next/server"
import { query } from "@/lib/db"

interface CodeResult {
  code: string
  code_type: string
  description: string
  hospital_count: number
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams
  const q = searchParams.get("q")

  if (!q || q.length < 2) {
    return NextResponse.json({ results: [] })
  }

  try {
    const results = await query<CodeResult>(
      `SELECT
        c.code,
        c.code_type,
        sci.description,
        COUNT(DISTINCT sci.hospital_id) as hospital_count
      FROM codes c
      JOIN item_codes ic ON ic.code_id = c.id
      JOIN standard_charge_items sci ON sci.id = ic.item_id
      WHERE c.code ILIKE $1 OR sci.description ILIKE $2
      GROUP BY c.code, c.code_type, sci.description
      ORDER BY
        CASE WHEN c.code ILIKE $1 THEN 0 ELSE 1 END,
        c.code
      LIMIT 20`,
      [`${q}%`, `%${q}%`]
    )

    return NextResponse.json({ results })
  } catch (error) {
    console.error("Search error:", error)
    return NextResponse.json(
      { error: "Failed to search codes" },
      { status: 500 }
    )
  }
}
