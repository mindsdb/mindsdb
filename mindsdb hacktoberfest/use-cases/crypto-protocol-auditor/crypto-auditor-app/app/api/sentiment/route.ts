import { NextRequest, NextResponse } from 'next/server';

// In-memory cache for sentiment results (1 hour TTL)
const sentimentCache = new Map<string, { data: any; timestamp: number }>();
const CACHE_TTL = 60 * 60 * 1000; // 1 hour in milliseconds

interface NewsArticle {
  title: string;
  description: string;
  source: string;
  publishedAt: string;
  url: string;
}

interface SentimentResult {
  project: string;
  sentiment: 'bullish' | 'bearish' | 'neutral';
  score: number; // -1 to 1 scale
  confidence: number; // 0 to 1
  summary: string;
  recentNews: NewsArticle[];
  newsCount: number;
  lastUpdated: string;
}

// Map crypto projects to News API search terms
const PROJECT_SEARCH_TERMS: Record<string, string[]> = {
  bitcoin: ['Bitcoin', 'BTC cryptocurrency'],
  ethereum: ['Ethereum', 'ETH blockchain'],
  ripple: ['Ripple', 'XRP'],
  avalanche: ['Avalanche', 'AVAX'],
  cardano: ['Cardano', 'ADA'],
  solana: ['Solana', 'SOL'],
  polkadot: ['Polkadot', 'DOT'],
  polygon: ['Polygon', 'MATIC'],
  chainlink: ['Chainlink', 'LINK'],
  uniswap: ['Uniswap', 'UNI'],
};

/**
 * Fetch recent news articles for a project from MindsDB News API
 */
async function fetchProjectNews(project: string, days: number = 7): Promise<NewsArticle[]> {
  const searchTerms = PROJECT_SEARCH_TERMS[project.toLowerCase()] || [project];
  const articles: NewsArticle[] = [];

  try {
    // Query News API through MindsDB
    const query = `
      SELECT title, description, source_name, publishedAt, url
      FROM crypto_news.article
      WHERE query = '${searchTerms[0]} cryptocurrency'
      LIMIT 20;
    `;

    const response = await fetch('http://127.0.0.1:47334/api/sql/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });

    const data = await response.json();

    if (data.data && Array.isArray(data.data)) {
      // Parse response (array of arrays format)
      const columnNames = data.column_names || ['title', 'description', 'source_name', 'publishedAt', 'url'];
      
      data.data.forEach((row: any[]) => {
        const article: any = {};
        columnNames.forEach((col: string, idx: number) => {
          article[col] = row[idx];
        });
        
        articles.push({
          title: article.title || '',
          description: article.description || '',
          source: article.source_name || 'Unknown',
          publishedAt: article.publishedAt || new Date().toISOString(),
          url: article.url || '',
        });
      });
    }

    return articles;
  } catch (error) {
    console.error(`Error fetching news for ${project}:`, error);
    return [];
  }
}

/**
 * Analyze sentiment using MindsDB Gemini agent
 */
async function analyzeSentiment(project: string, articles: NewsArticle[]): Promise<Omit<SentimentResult, 'recentNews' | 'newsCount'>> {
  try {
    // Prepare news summary for analysis
    const newsSummary = articles
      .slice(0, 10) // Analyze top 10 articles
      .map((a, i) => `${i + 1}. ${a.title} - ${a.description}`)
      .join('\n\n');

    const analysisQuery = `
Analyze the market sentiment for ${project} based on these recent news headlines and descriptions:

${newsSummary}

Provide a JSON response with:
1. sentiment: "bullish", "bearish", or "neutral"
2. score: a number from -1 (very bearish) to 1 (very bullish)
3. confidence: a number from 0 to 1 indicating confidence in the analysis
4. summary: a 2-3 sentence explanation of the sentiment

Respond ONLY with valid JSON, no markdown formatting.
    `.trim();

    const query = `SELECT answer FROM crypto_auditor_agent WHERE question = '${analysisQuery.replace(/'/g, "''")}';`;

    const response = await fetch('http://127.0.0.1:47334/api/sql/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });

    const data = await response.json();

    if (data.data && data.data.length > 0) {
      const answer = data.data[0]?.[0] || '{}';
      
      // Try to parse JSON from the answer
      let sentimentData;
      try {
        // Extract JSON if it's wrapped in markdown code blocks
        const jsonMatch = answer.match(/\{[\s\S]*\}/);
        sentimentData = JSON.parse(jsonMatch ? jsonMatch[0] : answer);
      } catch {
        // Fallback: parse sentiment from text
        const sentiment = answer.toLowerCase().includes('bullish') ? 'bullish' :
                         answer.toLowerCase().includes('bearish') ? 'bearish' : 'neutral';
        sentimentData = {
          sentiment,
          score: sentiment === 'bullish' ? 0.5 : sentiment === 'bearish' ? -0.5 : 0,
          confidence: 0.6,
          summary: answer.substring(0, 200),
        };
      }

      return {
        project,
        sentiment: sentimentData.sentiment || 'neutral',
        score: sentimentData.score || 0,
        confidence: sentimentData.confidence || 0.5,
        summary: sentimentData.summary || 'No sentiment analysis available.',
        lastUpdated: new Date().toISOString(),
      };
    }

    // Fallback: basic sentiment based on article count and keywords
    return basicSentimentAnalysis(project, articles);
  } catch (error) {
    console.error(`Error analyzing sentiment for ${project}:`, error);
    return basicSentimentAnalysis(project, articles);
  }
}

/**
 * Fallback: Basic keyword-based sentiment analysis
 */
function basicSentimentAnalysis(project: string, articles: NewsArticle[]): Omit<SentimentResult, 'recentNews' | 'newsCount'> {
  const positiveKeywords = ['surge', 'rally', 'bullish', 'gain', 'rise', 'up', 'growth', 'adoption', 'partnership', 'upgrade'];
  const negativeKeywords = ['crash', 'drop', 'bearish', 'down', 'fall', 'concern', 'risk', 'hack', 'issue', 'problem'];

  let positiveCount = 0;
  let negativeCount = 0;

  articles.forEach(article => {
    const text = `${article.title} ${article.description}`.toLowerCase();
    positiveKeywords.forEach(keyword => {
      if (text.includes(keyword)) positiveCount++;
    });
    negativeKeywords.forEach(keyword => {
      if (text.includes(keyword)) negativeCount++;
    });
  });

  const totalSignals = positiveCount + negativeCount;
  const score = totalSignals > 0 ? (positiveCount - negativeCount) / totalSignals : 0;
  const sentiment = score > 0.2 ? 'bullish' : score < -0.2 ? 'bearish' : 'neutral';

  return {
    project,
    sentiment,
    score,
    confidence: totalSignals > 5 ? 0.7 : 0.4,
    summary: `Based on ${articles.length} recent articles, sentiment appears ${sentiment} with ${positiveCount} positive and ${negativeCount} negative signals.`,
    lastUpdated: new Date().toISOString(),
  };
}

/**
 * GET /api/sentiment?project=bitcoin&days=7
 */
export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const project = searchParams.get('project')?.toLowerCase();
  const days = parseInt(searchParams.get('days') || '7');

  if (!project) {
    return NextResponse.json(
      { error: 'Missing required parameter: project' },
      { status: 400 }
    );
  }

  // Check cache
  const cacheKey = `${project}-${days}`;
  const cached = sentimentCache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return NextResponse.json({
      ...cached.data,
      cached: true,
      cacheAge: Math.floor((Date.now() - cached.timestamp) / 1000),
    });
  }

  try {
    // Fetch recent news
    const articles = await fetchProjectNews(project, days);

    if (articles.length === 0) {
      return NextResponse.json({
        project,
        sentiment: 'neutral',
        score: 0,
        confidence: 0,
        summary: 'No recent news articles found for sentiment analysis.',
        recentNews: [],
        newsCount: 0,
        lastUpdated: new Date().toISOString(),
      });
    }

    // Analyze sentiment
    const sentimentAnalysis = await analyzeSentiment(project, articles);

    const result: SentimentResult = {
      ...sentimentAnalysis,
      recentNews: articles.slice(0, 5), // Return top 5 articles
      newsCount: articles.length,
    };

    // Cache the result
    sentimentCache.set(cacheKey, {
      data: result,
      timestamp: Date.now(),
    });

    return NextResponse.json(result);
  } catch (error) {
    console.error('Sentiment analysis error:', error);
    return NextResponse.json(
      { error: 'Failed to analyze sentiment', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}

/**
 * POST /api/sentiment - Batch sentiment analysis for multiple projects
 */
export async function POST(request: NextRequest) {
  try {
    const { projects, days = 7 } = await request.json();

    if (!Array.isArray(projects) || projects.length === 0) {
      return NextResponse.json(
        { error: 'Invalid request: projects array required' },
        { status: 400 }
      );
    }

    // Fetch sentiment for all projects in parallel
    const results = await Promise.all(
      projects.map(async (project: string) => {
        const cacheKey = `${project.toLowerCase()}-${days}`;
        const cached = sentimentCache.get(cacheKey);
        
        if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
          return { ...cached.data, cached: true };
        }

        const articles = await fetchProjectNews(project, days);
        if (articles.length === 0) {
          return {
            project,
            sentiment: 'neutral' as const,
            score: 0,
            confidence: 0,
            summary: 'No recent news found.',
            recentNews: [],
            newsCount: 0,
            lastUpdated: new Date().toISOString(),
          };
        }

        const sentimentAnalysis = await analyzeSentiment(project, articles);
        const result = {
          ...sentimentAnalysis,
          recentNews: articles.slice(0, 3),
          newsCount: articles.length,
        };

        sentimentCache.set(cacheKey, { data: result, timestamp: Date.now() });
        return result;
      })
    );

    return NextResponse.json({ results });
  } catch (error) {
    console.error('Batch sentiment analysis error:', error);
    return NextResponse.json(
      { error: 'Failed to analyze sentiment', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
