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

          {/* Search Box */}
          <div className="max-w-2xl mx-auto relative">
            <div className="relative">
              <Search className="absolute left-5 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
              <input
                type="text"
                placeholder="Search by code (e.g., 99213, J1450) or procedure name..."
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

          {/* Example codes */}
          <div className="max-w-2xl mx-auto mt-6 text-center">
            <p className="text-sm text-muted-foreground mb-3">Try searching for:</p>
            <div className="flex flex-wrap justify-center gap-2">
              {["99213", "J1450", "470", "92626", "H0017"].map((code) => (
                <button
                  key={code}
                  className="px-3 py-1.5 text-sm rounded-full bg-secondary hover:bg-secondary/80 text-secondary-foreground transition-colors"
                  onClick={() => {
                    setSearchQuery(code)
                    setShowResults(true)
                  }}
                >
                  {code}
                </button>
              ))}
            </div>
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
