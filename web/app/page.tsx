"use client"

import { useState, useEffect, useCallback } from "react"
import { useRouter } from "next/navigation"
import { Search, Heart, DollarSign, Building2 } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"

interface SearchResult {
  code: string
  code_type: string
  description: string
  hospital_count: number
}

export default function LandingPage() {
  const router = useRouter()
  const [searchQuery, setSearchQuery] = useState("")
  const [results, setResults] = useState<SearchResult[]>([])
  const [isSearching, setIsSearching] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const [codeType, setCodeType] = useState("CPT")
  const [codeValue, setCodeValue] = useState("")

  const search = useCallback(async (query: string) => {
    if (query.length < 2) {
      setResults([])
      return
    }

    setIsSearching(true)
    try {
      const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`)
      const data = await res.json()
      setResults(data.results || [])
    } catch (error) {
      console.error("Search failed:", error)
      setResults([])
    } finally {
      setIsSearching(false)
    }
  }, [])

  useEffect(() => {
    const timer = setTimeout(() => {
      search(searchQuery)
    }, 300)
    return () => clearTimeout(timer)
  }, [searchQuery, search])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && results.length > 0) {
      const first = results[0]
      router.push(`/service/${first.code}?type=${first.code_type}`)
    }
  }

  const handleResultClick = (result: SearchResult) => {
    router.push(`/service/${result.code}?type=${result.code_type}`)
  }

  const handleCodeLookup = () => {
    const trimmed = codeValue.trim()
    if (trimmed) {
      router.push(`/service/${encodeURIComponent(trimmed)}?type=${encodeURIComponent(codeType)}`)
    }
  }

  const handleCodeKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleCodeLookup()
    }
  }

  return (
    <main className="min-h-screen">
      {/* Hero Section */}
      <div className="relative overflow-hidden">
        {/* Decorative elements */}
        <div className="absolute top-20 left-10 w-72 h-72 bg-warm-200/30 rounded-full blur-3xl" />
        <div className="absolute bottom-20 right-10 w-96 h-96 bg-warm-300/20 rounded-full blur-3xl" />

        <div className="relative container mx-auto px-6 pt-20 pb-32">
          {/* Header */}
          <div className="text-center mb-16">
            <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-warm-100 text-warm-700 text-sm font-medium mb-6">
              <Heart className="w-4 h-4" />
              Hospital Price Transparency
            </div>
            <h1 className="text-5xl md:text-6xl font-bold text-foreground mb-6 tracking-tight">
              Compare Hospital
              <span className="text-primary block mt-2">Procedure Prices</span>
            </h1>
            <p className="text-xl text-muted-foreground max-w-2xl mx-auto leading-relaxed">
              Search for medical procedure codes (CPT, HCPCS, MS-DRG) and compare
              prices across hospitals. Make informed healthcare decisions.
            </p>
          </div>

          {/* Code Lookup */}
          <div className="max-w-2xl mx-auto mb-8">
            <label className="block text-sm font-medium text-muted-foreground mb-2">Look up by code</label>
            <div className="flex gap-2">
              <select
                value={codeType}
                onChange={(e) => setCodeType(e.target.value)}
                className="shrink-0 w-28 py-3 px-3 text-base rounded-xl border-2 border-border/60 bg-background/80 focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary transition-all"
              >
                {["CPT", "HCPCS", "MS-DRG", "NDC", "RC", "CDM", "ICD", "DRG", "LOCAL", "APC"].map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <input
                type="text"
                placeholder="Enter code (e.g., 99213)"
                className="search-input flex-1 px-4"
                value={codeValue}
                onChange={(e) => setCodeValue(e.target.value)}
                onKeyDown={handleCodeKeyDown}
              />
              <button
                onClick={handleCodeLookup}
                disabled={!codeValue.trim()}
                className="px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Look up
              </button>
            </div>
          </div>

          {/* Popular codes */}
          <div className="max-w-2xl mx-auto mt-4 mb-8">
            <p className="text-sm text-muted-foreground mb-2">Popular codes:</p>
            <div className="flex flex-wrap gap-2">
              {[
                { type: "CPT", code: "99213", desc: "Office visit, established patient" },
                { type: "CPT", code: "27447", desc: "Total knee replacement" },
                { type: "MS-DRG", code: "470", desc: "Major hip & knee joint replacement" },
                { type: "HCPCS", code: "J1450", desc: "Fluconazole injection" },
                { type: "CPT", code: "43239", desc: "Upper GI endoscopy with biopsy" },
                { type: "CPT", code: "29881", desc: "Knee arthroscopy/surgery" },
              ].map((item) => (
                <button
                  key={`${item.type}-${item.code}`}
                  className="px-3 py-1.5 text-sm rounded-full bg-secondary hover:bg-secondary/80 text-secondary-foreground transition-colors"
                  onClick={() => router.push(`/service/${encodeURIComponent(item.code)}?type=${encodeURIComponent(item.type)}`)}
                >
                  <span className="font-medium">{item.type} {item.code}</span>
                  <span className="text-muted-foreground"> — {item.desc}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="max-w-2xl mx-auto flex items-center gap-4 mb-8">
            <div className="flex-1 border-t border-border/50" />
            <span className="text-sm text-muted-foreground">or search by name</span>
            <div className="flex-1 border-t border-border/50" />
          </div>

          {/* Search Box */}
          <div className="max-w-2xl mx-auto relative">
            <div className="relative">
              <Search className="absolute left-5 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
              <input
                type="text"
                placeholder="Search by procedure name..."
                className="search-input pl-14 pr-6"
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value)
                  setShowResults(true)
                }}
                onKeyDown={handleKeyDown}
                onFocus={() => setShowResults(true)}
              />
            </div>

            {/* Search Results Dropdown */}
            {showResults && results.length > 0 && (
              <Card className="absolute top-full left-0 right-0 mt-2 z-50 overflow-hidden shadow-lg border-border/50">
                <CardContent className="p-0">
                  <ul className="divide-y divide-border/50">
                    {results.map((result, index) => (
                      <li
                        key={`${result.code}-${result.code_type}-${index}`}
                        className="px-5 py-4 hover:bg-accent/50 cursor-pointer transition-colors"
                        onClick={() => handleResultClick(result)}
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-semibold text-foreground">
                                {result.code}
                              </span>
                              <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-warm-100 text-warm-700">
                                {result.code_type}
                              </span>
                            </div>
                            <p className="text-sm text-muted-foreground truncate">
                              {result.description}
                            </p>
                          </div>
                          <div className="flex items-center gap-1 text-sm text-muted-foreground">
                            <Building2 className="w-4 h-4" />
                            {result.hospital_count}
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            )}

            {showResults && searchQuery.length >= 2 && results.length === 0 && !isSearching && (
              <Card className="absolute top-full left-0 right-0 mt-2 z-50 shadow-lg border-border/50">
                <CardContent className="p-6 text-center text-muted-foreground">
                  No results found for &quot;{searchQuery}&quot;
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>

      {/* Features Section */}
      <div className="bg-card/50 border-t border-border/50 py-20">
        <div className="container mx-auto px-6">
          <div className="grid md:grid-cols-3 gap-8">
            <FeatureCard
              icon={<Search className="w-6 h-6" />}
              title="Search by Code"
              description="Find procedures using CPT, HCPCS, MS-DRG, NDC, or Revenue codes"
            />
            <FeatureCard
              icon={<DollarSign className="w-6 h-6" />}
              title="Compare Prices"
              description="See discounted cash prices across multiple hospitals"
            />
            <FeatureCard
              icon={<Building2 className="w-6 h-6" />}
              title="Hospital Details"
              description="View hospital information and pricing methodology"
            />
          </div>
        </div>
      </div>

      {/* Footer */}
      <footer className="py-8 border-t border-border/50">
        <div className="container mx-auto px-6 text-center text-sm text-muted-foreground">
          <p>
            Data sourced from hospital price transparency files as required by CMS regulations.
          </p>
        </div>
      </footer>

      {/* Click outside to close results */}
      {showResults && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setShowResults(false)}
        />
      )}
    </main>
  )
}

function FeatureCard({
  icon,
  title,
  description,
}: {
  icon: React.ReactNode
  title: string
  description: string
}) {
  return (
    <Card className="warm-card border-0 hover:shadow-md transition-shadow">
      <CardContent className="pt-6">
        <div className="w-12 h-12 rounded-xl bg-primary/10 flex items-center justify-center text-primary mb-4">
          {icon}
        </div>
        <h3 className="text-lg font-semibold mb-2">{title}</h3>
        <p className="text-muted-foreground">{description}</p>
      </CardContent>
    </Card>
  )
}
