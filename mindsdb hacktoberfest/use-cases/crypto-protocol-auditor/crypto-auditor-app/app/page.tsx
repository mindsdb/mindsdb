'use client';

import { useState } from 'react';
import Image from 'next/image';

// Agent API Response types
type SearchResult = {
  content: string;
  relevance: number;
  metadata: {
    _source?: string;
    category?: string;
    [key: string]: any;
  };
  source: string;
  searchMode?: string;
};

type PriceResult = {
  project: string;
  price_usd: number;
  market_cap_usd: number;
  volume_24h_usd: number;
  price_change_24h: number;
  price_change_7d: number;
  last_updated: string;
};

type SentimentData = {
  sentiment: 'bullish' | 'bearish' | 'neutral';
  score: number;
  confidence: number;
  summary: string;
  newsCount: number;
  recentNews?: Array<{
    title: string;
    description: string;
    source_name?: string;
    url: string;
    publishedAt: string;
  }>;
};

type AgentResponse = {
  queryId: string;
  originalQuery: string;
  classifiedAs: 'kb_only' | 'price_only' | 'combined' | 'auto';
  results: {
    kb_results?: SearchResult[];
    price_results?: PriceResult[];
    kbSearchComplete: boolean;
    priceSearchComplete: boolean;
  };
  executedAt: {
    kb_search_ms: number;
    price_fetch_ms: number;
    total_ms: number;
  };
  agentReasoning: string;
};

/**
 * Extract project name from KB result content
 */
function extractProjectName(content: string): string | null {
  const projectKeywords: { [key: string]: string } = {
    bitcoin: 'bitcoin',
    ethereum: 'ethereum',
    ripple: 'ripple',
    xrp: 'ripple',
    avalanche: 'avalanche',
    cardano: 'cardano',
    polkadot: 'polkadot',
    solana: 'solana',
    polygon: 'polygon',
    arbitrum: 'arbitrum',
  };

  for (const [keyword, projectName] of Object.entries(projectKeywords)) {
    if (content.toLowerCase().includes(keyword)) {
      return projectName;
    }
  }
  return null;
}

/**
 * Sentiment Badge Component
 */
function SentimentBadge({ sentiment, score, confidence }: { sentiment: string; score: number; confidence: number }) {
  const getColors = () => {
    switch (sentiment.toLowerCase()) {
      case 'bullish':
        return { bg: 'rgba(0, 208, 132, 0.1)', text: 'var(--color-success)', border: 'rgba(0, 208, 132, 0.3)', icon: '🟢' };
      case 'bearish':
        return { bg: 'rgba(255, 59, 48, 0.1)', text: 'var(--color-error)', border: 'rgba(255, 59, 48, 0.3)', icon: '🔴' };
      default:
        return { bg: 'rgba(255, 149, 0, 0.1)', text: 'var(--color-warning)', border: 'rgba(255, 149, 0, 0.3)', icon: '🟡' };
    }
  };

  const colors = getColors();

  return (
    <div 
      className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border"
      style={{ backgroundColor: colors.bg, borderColor: colors.border }}
    >
      <span className="text-sm">{colors.icon}</span>
      <span className="text-sm font-semibold capitalize" style={{ color: colors.text }}>
        {sentiment}
      </span>
      <span className="text-xs font-medium" style={{ color: colors.text }}>
        ({(score * 100).toFixed(0)}%)
      </span>
    </div>
  );
}

/**
 * Recent News Item Component
 */
function NewsItem({ article }: { article: any }) {
  const formatDate = (dateString: string) => {
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    } catch {
      return 'Recently';
    }
  };

  return (
    <a
      href={article.url || '#'}
      target="_blank"
      rel="noopener noreferrer"
      className="block group p-4 rounded-xl transition-fast border"
      style={{ 
        backgroundColor: 'var(--card-bg)',
        borderColor: 'var(--card-border)'
      }}
    >
      <div className="flex items-start gap-3">
        <span className="text-base shrink-0" style={{ color: 'var(--text-tertiary)' }}>📰</span>
        <div className="flex-1 min-w-0">
          <h4 className="text-sm font-medium line-clamp-2 transition-fast" style={{ color: 'var(--text-primary)' }}>
            {article.title}
          </h4>
          <div className="flex items-center gap-2 mt-2">
            <span className="text-xs font-medium" style={{ color: 'var(--text-tertiary)' }}>
              {article.source_name || 'News'}
            </span>
            <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>•</span>
            <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>
              {formatDate(article.publishedAt)}
            </span>
          </div>
        </div>
        <span className="text-sm transition-fast shrink-0" style={{ color: 'var(--text-tertiary)' }}>→</span>
      </div>
    </a>
  );
}

export default function Home() {
  const [question, setQuestion] = useState('');
  const [agentResponse, setAgentResponse] = useState<AgentResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sentimentCache, setSentimentCache] = useState<Record<string, SentimentData>>({});

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);
    setAgentResponse(null);

    try {
      const response = await fetch('/api/agent/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          query: question,
          context: {
            searchMode: 'auto', // Let agent decide
            maxResults: 5,
          }
        }),
      });

      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.error || 'Failed to fetch results');
      }

      const data: AgentResponse = await response.json();
      console.log('Agent Response:', data);
      setAgentResponse(data);
      
      // Fetch sentiment data for each KB result's project
      if (data.results.kb_results && data.results.kb_results.length > 0) {
        const projectNames = data.results.kb_results
          .map(r => extractProjectName(r.content))
          .filter((p): p is string => p !== null);
        
        // Deduplicate and fetch sentiment for each project
        const uniqueProjects = Array.from(new Set(projectNames));
        for (const project of uniqueProjects) {
          try {
            const sentimentRes = await fetch(`/api/sentiment?project=${project}&days=7`);
            if (sentimentRes.ok) {
              const sentimentData = await sentimentRes.json();
              setSentimentCache(prev => ({
                ...prev,
                [project]: sentimentData
              }));
            }
          } catch (err) {
            console.error(`Failed to fetch sentiment for ${project}:`, err);
          }
        }
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <main className="min-h-screen" style={{ backgroundColor: 'var(--bg-default)' }}>
  {/* Navigation Header */}
  <header className="sticky top-0 z-50 backdrop-blur-[10px] border-b" style={{ backgroundColor: 'var(--bg-surface)', borderColor: 'var(--card-border)' }}>
        <div className="max-w-[1200px] mx-auto px-12 md:px-6">
          <div className="flex items-center justify-between h-[72px]">
            <div className="flex items-center gap-3">
              <Image 
                src="/logo.png" 
                alt="Crypto Protocol Auditor Logo" 
                width={80} 
                height={80}
                className="rounded"
              />
              <h1 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
                Crypto Protocol Auditor
              </h1>
            </div>
            <nav className="flex gap-2">
              <a 
                href="/" 
                className="px-4 py-2 text-base font-medium transition-fast rounded-lg"
                style={{ color: 'var(--color-primary)' }}
              >
                Home
              </a>
              <a 
                href="/compare" 
                className="px-4 py-2 text-base font-medium transition-fast rounded-lg hover:bg-[var(--bg-purple-light)]"
                style={{ color: 'var(--text-primary)' }}
              >
                Compare
              </a>
            </nav>
          </div>
        </div>
      </header>

      {/* Hero Section removed per user request to keep the UI focused on search */}

      {/* Main Content */}
      <div className="max-w-[1200px] mx-auto px-12 md:px-6 py-16">
        {/* Search Section */}
        <div className="mb-16">
          <form onSubmit={handleSubmit} className="max-w-[800px] mx-auto">
            <div className="rounded-2xl p-8 shadow-elevated border" style={{ backgroundColor: 'var(--card-bg)', borderColor: 'var(--card-border)' }}>
              <label className="block mb-3 text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
                Ask about any crypto protocol
              </label>
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="e.g., What is Bitcoin's consensus mechanism? What is Ethereum's price?"
                className="w-full bg-transparent border border-[var(--input-border)] rounded-lg px-4 py-3 text-base transition-fast focus:outline-none focus:border-[var(--color-primary)]"
                style={{ 
                  boxShadow: question ? '0 0 0 3px rgba(108, 92, 231, 0.1)' : 'none',
                  color: 'var(--text-primary)'
                }}
              />
              <button
                type="submit"
                disabled={isLoading}
                className="mt-4 w-full rounded-lg px-7 py-3.5 text-base font-semibold text-white transition-fast disabled:opacity-50 disabled:cursor-not-allowed"
                style={{ 
                  backgroundColor: isLoading ? 'var(--text-tertiary)' : 'var(--color-primary)',
                  transform: isLoading ? 'none' : 'translateY(0)',
                  boxShadow: isLoading ? 'none' : 'var(--shadow-button-hover)'
                }}
                onMouseEnter={(e) => {
                  if (!isLoading) {
                    e.currentTarget.style.backgroundColor = 'var(--color-primary-hover)';
                    e.currentTarget.style.transform = 'translateY(-1px)';
                  }
                }}
                onMouseLeave={(e) => {
                  if (!isLoading) {
                    e.currentTarget.style.backgroundColor = 'var(--color-primary)';
                    e.currentTarget.style.transform = 'translateY(0)';
                  }
                }}
              >
                {isLoading ? (
                  <span className="flex items-center justify-center gap-2">
                    <span className="inline-block animate-spin">⚙️</span>
                    Analyzing...
                  </span>
                ) : (
                  <span className="flex items-center justify-center gap-2">
                    <span>🔍</span>
                    Search Protocol
                  </span>
                )}
              </button>
            </div>
          </form>
        </div>

        {/* Error Message */}
        {error && (
          <div className="mb-8 max-w-[800px] mx-auto rounded-xl p-6 border-2 flex items-start gap-4" style={{ backgroundColor: '#FFF5F5', borderColor: 'var(--color-error)', color: 'var(--color-error)' }}>
            <span className="text-2xl shrink-0">⚠️</span>
            <div>
              <h4 className="font-semibold mb-1">Error</h4>
              <p className="text-sm">{error}</p>
            </div>
          </div>
        )}

        {/* Agent Info Banner */}
        {agentResponse && (
          <div className="mb-12 max-w-[800px] mx-auto rounded-2xl p-6 border" style={{ background: 'linear-gradient(135deg, var(--bg-purple-light) 0%, var(--bg-blue-light) 100%)', borderColor: 'rgba(108, 92, 231, 0.2)' }}>
            <div className="flex items-start gap-4 flex-wrap">
              <span className="text-3xl">🤖</span>
              <div className="flex-1 min-w-[200px]">
                <div className="flex items-center gap-3 mb-2 flex-wrap">
                  <span className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>Query Type:</span>
                  <span className={`px-3 py-1 rounded-full text-xs font-semibold text-white`} style={{ 
                    backgroundColor: agentResponse.classifiedAs === 'kb_only' ? 'var(--color-primary)' :
                                    agentResponse.classifiedAs === 'price_only' ? 'var(--color-success)' :
                                    agentResponse.classifiedAs === 'combined' ? 'var(--color-info)' :
                                    'var(--text-tertiary)'
                  }}>
                    {agentResponse.classifiedAs === 'kb_only' && '📚 Knowledge Base'}
                    {agentResponse.classifiedAs === 'price_only' && '💰 Live Prices'}
                    {agentResponse.classifiedAs === 'combined' && '🔀 Combined'}
                    {agentResponse.classifiedAs === 'auto' && '🔍 Auto'}
                  </span>
                </div>
                {agentResponse.agentReasoning && (
                  <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>{agentResponse.agentReasoning}</p>
                )}
              </div>
              {agentResponse.executedAt && (
                <div className="text-xs space-y-1" style={{ color: 'var(--text-tertiary)' }}>
                  <div>KB: {agentResponse.executedAt.kb_search_ms}ms</div>
                  <div>Prices: {agentResponse.executedAt.price_fetch_ms}ms</div>
                  <div className="font-semibold">Total: {agentResponse.executedAt.total_ms}ms</div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Knowledge Base Results Section */}
        {agentResponse?.results.kb_results && agentResponse.results.kb_results.length > 0 && (
          <div className="mb-16">
            <div className="mb-8 flex items-center gap-3">
              <span className="text-3xl">📚</span>
              <h3 className="text-4xl font-bold" style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
                Knowledge Base Results
                <span className="ml-3 text-2xl font-normal" style={{ color: 'var(--color-primary)' }}>
                  ({agentResponse.results.kb_results.length})
                </span>
              </h3>
            </div>

            <div className="grid gap-6">
              {agentResponse.results.kb_results.map((result, index) => {
                const projectName = extractProjectName(result.content);
                const sentiment = projectName ? sentimentCache[projectName] : null;
                
                return (
                  <div
                    key={index}
                    className="group rounded-2xl p-8 transition-medium border hover:shadow-floating"
                    style={{ 
                      backgroundColor: 'var(--card-bg)',
                      borderColor: 'var(--card-border)',
                      boxShadow: 'var(--shadow-subtle)'
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.transform = 'translateY(-4px)';
                      e.currentTarget.style.boxShadow = 'var(--shadow-floating)';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.transform = 'translateY(0)';
                      e.currentTarget.style.boxShadow = 'var(--shadow-subtle)';
                    }}
                  >
                    <div className="flex items-start gap-4">
                      <span className="text-3xl shrink-0">🤖</span>
                      <div className="flex-1">
                        <div className="flex items-center gap-3 mb-4 flex-wrap">
                          <h4 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
                            AI Response
                          </h4>
                          {sentiment && (
                            <SentimentBadge
                              sentiment={sentiment.sentiment}
                              score={sentiment.score}
                              confidence={sentiment.confidence}
                            />
                          )}
                        </div>
                        <p className="text-base leading-7 whitespace-pre-wrap" style={{ color: 'var(--text-secondary)' }}>
                          {result.content}
                        </p>
                      </div>
                    </div>
                    
                    {/* Sentiment Summary */}
                    {sentiment && (
                      <div className="mt-8 pt-8 border-t" style={{ borderColor: 'rgba(0, 0, 0, 0.06)' }}>
                        <div className="mb-4">
                          <p className="text-sm font-medium mb-2" style={{ color: 'var(--text-secondary)' }}>
                            💬 Market Sentiment Summary
                          </p>
                          <p className="text-base leading-7" style={{ color: 'var(--text-primary)' }}>
                            {sentiment.summary}
                          </p>
                          <div className="mt-3 text-sm" style={{ color: 'var(--text-tertiary)' }}>
                            Confidence: {(sentiment.confidence * 100).toFixed(0)}% | Based on {sentiment.newsCount} articles
                          </div>
                        </div>
                      </div>
                    )}
                    
                    {/* Recent News Section */}
                    {sentiment && sentiment.recentNews && sentiment.recentNews.length > 0 && (
                      <div className="mt-8 pt-8 border-t" style={{ borderColor: 'rgba(0, 0, 0, 0.06)' }}>
                        <p className="text-sm font-semibold mb-4" style={{ color: 'var(--text-primary)' }}>
                          📰 Recent News
                        </p>
                        <div className="space-y-3 max-h-[300px] overflow-y-auto">
                          {sentiment.recentNews.slice(0, 4).map((article, idx) => (
                            <NewsItem key={idx} article={article} />
                          ))}
                        </div>
                      </div>
                    )}
                  
                    {result.relevance && (
                      <div className="mt-8 pt-8 border-t" style={{ borderColor: 'rgba(0, 0, 0, 0.06)' }}>
                        <div className="flex items-center gap-3">
                          <span className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>
                            Relevance:
                          </span>
                          <div className="flex-1 bg-[var(--bg-light-gray)] rounded-full h-2 overflow-hidden">
                            <div 
                              className="h-full transition-all"
                              style={{ 
                                width: `${result.relevance * 100}%`,
                                backgroundColor: result.relevance > 0.8 ? 'var(--color-success)' :
                                               result.relevance > 0.6 ? 'var(--color-info)' :
                                               'var(--color-warning)'
                              }}
                            ></div>
                          </div>
                          <span className="text-sm font-bold" style={{ color: 'var(--color-primary)' }}>
                            {(result.relevance * 100).toFixed(1)}%
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Price Results Section */}
        {agentResponse?.results.price_results && agentResponse.results.price_results.length > 0 && (
          <div className="mb-16">
            <div className="mb-8 flex items-center gap-3">
              <span className="text-3xl">💰</span>
              <h3 className="text-4xl font-bold" style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
                Live Prices
                <span className="ml-3 text-2xl font-normal" style={{ color: 'var(--color-success)' }}>
                  ({agentResponse.results.price_results.length})
                </span>
              </h3>
              <span className="text-sm font-medium" style={{ color: 'var(--text-tertiary)' }}>
                via CoinGecko
              </span>
            </div>

            <div className="grid gap-6 md:grid-cols-2">
              {agentResponse.results.price_results.map((price, index) => (
                <div
                  key={index}
                  className="rounded-2xl p-6 transition-medium border"
                  style={{ 
                    backgroundColor: 'var(--card-bg)',
                    borderColor: 'var(--card-border)',
                    boxShadow: 'var(--shadow-subtle)'
                  }}
                >
                  <div className="flex items-center justify-between mb-6">
                    <h4 className="text-2xl font-bold capitalize" style={{ color: 'var(--text-primary)' }}>
                      {price.project}
                    </h4>
                    {price.price_change_24h !== undefined && (
                      <span 
                        className="px-3 py-1.5 rounded-full text-sm font-semibold text-white"
                        style={{ backgroundColor: price.price_change_24h >= 0 ? 'var(--color-success)' : 'var(--color-error)' }}
                      >
                        {price.price_change_24h >= 0 ? '↑' : '↓'} {Math.abs(price.price_change_24h).toFixed(2)}%
                      </span>
                    )}
                  </div>

                  <div className="space-y-4">
                    {price.price_usd !== undefined && (
                      <div className="rounded-xl p-4" style={{ backgroundColor: 'rgba(0, 208, 132, 0.05)' }}>
                        <p className="text-xs font-semibold uppercase mb-2" style={{ color: 'var(--text-secondary)', letterSpacing: '0.05em' }}>
                          💵 Current Price
                        </p>
                        <p className="text-3xl font-bold" style={{ color: 'var(--color-success)' }}>
                          ${price.price_usd.toLocaleString()}
                        </p>
                      </div>
                    )}

                    {price.market_cap_usd !== undefined && (
                      <div className="rounded-xl p-4" style={{ backgroundColor: 'var(--bg-blue-light)' }}>
                        <p className="text-xs font-semibold uppercase mb-2" style={{ color: 'var(--text-secondary)', letterSpacing: '0.05em' }}>
                          📊 Market Cap
                        </p>
                        <p className="text-lg font-semibold" style={{ color: 'var(--color-info)' }}>
                          ${(price.market_cap_usd / 1e9).toFixed(2)}B
                        </p>
                      </div>
                    )}
                  </div>

                  {price.last_updated && (
                    <div className="mt-4 pt-4 border-t text-xs" style={{ borderColor: 'rgba(0, 0, 0, 0.06)', color: 'var(--text-tertiary)' }}>
                      Updated: {new Date(price.last_updated).toLocaleString()}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Empty State */}
        {!isLoading && !agentResponse && !error && (
          <div className="text-center py-24">
            <span className="text-7xl mb-6 block">🔍</span>
            <h3 className="text-3xl font-bold mb-3" style={{ color: 'var(--text-primary)' }}>
              Start Your Search
            </h3>
            <p className="text-lg mb-12" style={{ color: 'var(--text-secondary)' }}>
              Ask about crypto protocols, request live prices, or combine both!
            </p>
            <div className="grid md:grid-cols-3 gap-6 max-w-4xl mx-auto">
              <div className="p-6 rounded-2xl border text-left transition-medium hover:shadow-elevated" style={{ backgroundColor: 'var(--card-bg)', borderColor: 'var(--card-border)' }}>
                <p className="text-sm font-semibold mb-3" style={{ color: 'var(--color-primary)' }}>
                  📊 Sentiment Badges
                </p>
                <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
                  See protocol-level sentiment summaries (bullish / bearish / neutral) aggregated from recent news.
                </p>
              </div>
              <div className="p-6 rounded-2xl border text-left transition-medium hover:shadow-elevated" style={{ backgroundColor: 'var(--card-bg)', borderColor: 'var(--card-border)' }}>
                <p className="text-sm font-semibold mb-3" style={{ color: 'var(--color-info)' }}>
                  📰 Recent News
                </p>
                <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
                  Quick access to the latest headlines and sources used to compute sentiment.
                </p>
              </div>
              <div className="p-6 rounded-2xl border text-left transition-medium hover:shadow-elevated" style={{ backgroundColor: 'var(--card-bg)', borderColor: 'var(--card-border)' }}>
                <p className="text-sm font-semibold mb-3" style={{ color: 'var(--color-success)' }}>
                  🔀 Protocol Comparison
                </p>
                <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
                  Compare protocols side-by-side with live prices, market cap and sentiment overlays.
                </p>
              </div>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
