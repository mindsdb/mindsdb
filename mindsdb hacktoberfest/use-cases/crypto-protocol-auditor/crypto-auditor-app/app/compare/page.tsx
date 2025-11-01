'use client';

import { useState } from 'react';

interface ComparisonProject {
  name: string;
  technicalInfo: {
    content: string;
    category: string;
  };
  priceData: {
    price_usd: number;
    market_cap_usd: number;
    volume_24h_usd: number;
    price_change_24h: number;
    price_change_7d: number;
  } | null;
  sentiment: {
    sentiment: 'bullish' | 'bearish' | 'neutral';
    score: number;
    confidence: number;
    summary: string;
    newsCount: number;
  } | null;
}

interface ComparisonData {
  projects: ComparisonProject[];
  comparisonSummary: string;
  generatedAt: string;
}

const AVAILABLE_PROJECTS = [
  'Bitcoin', 'Ethereum', 'Ripple', 'Avalanche', 'Cardano',
  'Solana', 'Polkadot', 'Polygon', 'Chainlink', 'Uniswap',
  'Aave', 'Curve', 'Yearn', 'Cosmos', 'NEAR', 'Tron'
];

export default function ComparePage() {
  const [selectedProjects, setSelectedProjects] = useState<string[]>(['Bitcoin', 'Ethereum']);
  const [comparisonData, setComparisonData] = useState<ComparisonData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggleProject = (project: string) => {
    if (selectedProjects.includes(project)) {
      setSelectedProjects(selectedProjects.filter(p => p !== project));
    } else if (selectedProjects.length < 5) {
      setSelectedProjects([...selectedProjects, project]);
    }
  };

  const handleCompare = async () => {
    if (selectedProjects.length < 2) {
      setError('Please select at least 2 projects to compare');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`/api/compare?projects=${selectedProjects.join(',')}`);
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch comparison');
      }

      setComparisonData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const getSentimentColor = (sentiment: string) => {
    switch (sentiment) {
      case 'bullish': return 'text-green-400';
      case 'bearish': return 'text-red-400';
      default: return 'text-gray-400';
    }
  };

  const getSentimentBg = (sentiment: string) => {
    switch (sentiment) {
      case 'bullish': return 'bg-green-500/20 border-green-500/30';
      case 'bearish': return 'bg-red-500/20 border-red-500/30';
      default: return 'bg-gray-500/20 border-gray-500/30';
    }
  };

  return (
    <div className="min-h-screen bg-linear-to-br from-gray-900 via-purple-900/20 to-gray-900 text-white">
      <div className="max-w-7xl mx-auto px-4 py-12">
        {/* Header */}
        <div className="mb-12">
          <h1 className="text-4xl font-bold mb-4 bg-linear-to-r from-blue-400 via-purple-400 to-pink-400 bg-clip-text text-transparent">
            Protocol Comparison
          </h1>
          <p className="text-gray-400">
            Compare technical specs, market data, and sentiment across multiple cryptocurrency protocols
          </p>
        </div>

        {/* Project Selector */}
        <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl p-6 border border-gray-700/50 mb-8">
          <h2 className="text-xl font-semibold mb-4">Select Projects (2-5)</h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3 mb-6">
            {AVAILABLE_PROJECTS.map((project) => (
              <button
                key={project}
                onClick={() => toggleProject(project)}
                disabled={!selectedProjects.includes(project) && selectedProjects.length >= 5}
                className={`px-4 py-2 rounded-lg font-medium transition-all ${
                  selectedProjects.includes(project)
                    ? 'bg-linear-to-r from-blue-500 to-purple-500 text-white'
                    : 'bg-gray-700/50 text-gray-300 hover:bg-gray-700'
                } disabled:opacity-50 disabled:cursor-not-allowed`}
              >
                {project}
              </button>
            ))}
          </div>

          <div className="flex items-center justify-between">
            <p className="text-sm text-gray-400">
              Selected: {selectedProjects.join(', ') || 'None'}
            </p>
            <button
              onClick={handleCompare}
              disabled={selectedProjects.length < 2 || loading}
              className="px-6 py-2 bg-linear-to-r from-purple-600 to-pink-600 rounded-lg font-semibold hover:from-purple-700 hover:to-pink-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Comparing...' : 'Compare Projects'}
            </button>
          </div>
        </div>

        {/* Error Message */}
        {error && (
          <div className="bg-red-500/20 border border-red-500/50 rounded-xl p-4 mb-8">
            <p className="text-red-300">{error}</p>
          </div>
        )}

        {/* Loading State */}
        {loading && (
          <div className="text-center py-12">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-purple-500"></div>
            <p className="text-gray-400 mt-4">Fetching comparison data...</p>
          </div>
        )}

        {/* Comparison Results */}
        {comparisonData && !loading && (
          <div className="space-y-8">
            {/* AI Summary */}
            <div className="bg-linear-to-r from-purple-500/10 to-pink-500/10 border border-purple-500/30 rounded-xl p-6">
              <h3 className="text-xl font-semibold mb-3 flex items-center gap-2">
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
                AI Comparison Summary
              </h3>
              <p className="text-gray-300 leading-relaxed">{comparisonData.comparisonSummary}</p>
            </div>

            {/* Comparison Table */}
            <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700/50 overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-gray-700">
                      <th className="px-6 py-4 text-left text-gray-400 font-semibold">Metric</th>
                      {comparisonData.projects.map((project) => (
                        <th key={project.name} className="px-6 py-4 text-left font-semibold">
                          {project.name}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {/* Category */}
                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/20">
                      <td className="px-6 py-4 text-gray-400">Category</td>
                      {comparisonData.projects.map((project) => (
                        <td key={project.name} className="px-6 py-4">
                          <span className="px-3 py-1 bg-blue-500/20 border border-blue-500/30 rounded-full text-sm">
                            {project.technicalInfo.category}
                          </span>
                        </td>
                      ))}
                    </tr>

                    {/* Price */}
                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/20">
                      <td className="px-6 py-4 text-gray-400">Price (USD)</td>
                      {comparisonData.projects.map((project) => (
                        <td key={project.name} className="px-6 py-4 font-semibold text-lg">
                          {project.priceData ? `$${project.priceData.price_usd.toLocaleString()}` : 'N/A'}
                        </td>
                      ))}
                    </tr>

                    {/* Market Cap */}
                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/20">
                      <td className="px-6 py-4 text-gray-400">Market Cap</td>
                      {comparisonData.projects.map((project) => (
                        <td key={project.name} className="px-6 py-4">
                          {project.priceData 
                            ? `$${(project.priceData.market_cap_usd / 1e9).toFixed(2)}B`
                            : 'N/A'}
                        </td>
                      ))}
                    </tr>

                    {/* 24h Change */}
                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/20">
                      <td className="px-6 py-4 text-gray-400">24h Change</td>
                      {comparisonData.projects.map((project) => (
                        <td key={project.name} className="px-6 py-4">
                          {project.priceData ? (
                            <span className={project.priceData.price_change_24h >= 0 ? 'text-green-400' : 'text-red-400'}>
                              {project.priceData.price_change_24h >= 0 ? '↑' : '↓'} {Math.abs(project.priceData.price_change_24h).toFixed(2)}%
                            </span>
                          ) : 'N/A'}
                        </td>
                      ))}
                    </tr>

                    {/* Sentiment */}
                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/20">
                      <td className="px-6 py-4 text-gray-400">Market Sentiment</td>
                      {comparisonData.projects.map((project) => (
                        <td key={project.name} className="px-6 py-4">
                          {project.sentiment ? (
                            <div>
                              <span className={`px-3 py-1 rounded-full text-sm border ${getSentimentBg(project.sentiment.sentiment)} ${getSentimentColor(project.sentiment.sentiment)}`}>
                                {project.sentiment.sentiment.toUpperCase()}
                              </span>
                              <p className="text-xs text-gray-500 mt-1">
                                {project.sentiment.newsCount} articles
                              </p>
                            </div>
                          ) : 'N/A'}
                        </td>
                      ))}
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Technical Details */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {comparisonData.projects.map((project) => (
                <div key={project.name} className="bg-gray-800/50 backdrop-blur-sm rounded-xl p-6 border border-gray-700/50">
                  <h3 className="text-xl font-semibold mb-3">{project.name}</h3>
                  
                  {/* Sentiment Summary */}
                  {project.sentiment && (
                    <div className="mb-4 p-4 bg-gray-700/30 rounded-lg">
                      <p className="text-sm text-gray-300 italic">{project.sentiment.summary}</p>
                    </div>
                  )}
                  
                  {/* Technical Overview */}
                  <div className="text-sm text-gray-400">
                    <p className="line-clamp-6">{project.technicalInfo.content}</p>
                  </div>
                </div>
              ))}
            </div>

            {/* Timestamp */}
            <p className="text-center text-gray-500 text-sm">
              Generated at {new Date(comparisonData.generatedAt).toLocaleString()}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
