import fs from "node:fs"
import path from "node:path"

const CSV_HEADER = "code_type,code,consumer_description"

function parseCsvRow(line: string): string[] {
  const fields: string[] = []
  let current = ""
  let inQuotes = false

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i]

    if (char === "\"") {
      const nextChar = line[i + 1]
      if (inQuotes && nextChar === "\"") {
        current += "\""
        i += 1
        continue
      }

      inQuotes = !inQuotes
      continue
    }

    if (char === "," && !inQuotes) {
      fields.push(current)
      current = ""
      continue
    }

    current += char
  }

  fields.push(current)
  return fields
}

function normalize(value: string): string {
  return value.trim().toUpperCase()
}

function getKey(codeType: string, code: string): string {
  return `${normalize(codeType)}|${normalize(code)}`
}

function loadCodeDescriptionMap(): Map<string, string> {
  const candidates = [
    path.join(process.cwd(), "codes_with_descriptions.csv"),
    path.join(process.cwd(), "web", "codes_with_descriptions.csv"),
  ]
  const csvPath = candidates.find((candidate) => fs.existsSync(candidate))
  if (!csvPath) {
    throw new Error("codes_with_descriptions.csv not found")
  }
  const map = new Map<string, string>()

  const csv = fs.readFileSync(csvPath, "utf8")
  const lines = csv.split(/\r?\n/)

  if (!lines[0]?.startsWith(CSV_HEADER)) {
    throw new Error(`Unexpected CSV header in ${csvPath}`)
  }

  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i]?.trim()
    if (!line) {
      continue
    }

    const [codeType, code, description] = parseCsvRow(line)
    if (!codeType || !code || !description) {
      continue
    }

    map.set(getKey(codeType, code), description.trim())
  }

  return map
}

const codeDescriptionMap = loadCodeDescriptionMap()

export function getCodeDescription(codeType: string, code: string): string | null {
  return codeDescriptionMap.get(getKey(codeType, code)) ?? null
}
