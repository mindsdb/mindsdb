import { NextResponse } from 'next/server';

// In-memory cache for prices
const priceCache = new Map<string, { data: any; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Mapping of project names to CoinGecko IDs
const PROJECT_TO_COINGECKO_MAP: Record<string, string> = {
  'bitcoin': 'bitcoin',
  'btc': 'bitcoin',
  'ethereum': 'ethereum',
  'eth': 'ethereum',
  'ripple': 'ripple',
  'xrp': 'ripple',
  'avalanche': 'avalanche-2',
  'avax': 'avalanche-2',
  'cardano': 'cardano',
  'ada': 'cardano',
  'solana': 'solana',
  'sol': 'solana',
  'polkadot': 'polkadot',
  'dot': 'polkadot',
  'litecoin': 'litecoin',
  'ltc': 'litecoin',
  'dogecoin': 'dogecoin',
  'doge': 'dogecoin',
  'polygon': 'matic-network',
  'matic': 'matic-network',
  'arbitrum': 'arbitrum',
  'arb': 'arbitrum',
  'optimism': 'optimism',
  'op': 'optimism',
  'cosmos': 'cosmos',
  'atom': 'cosmos',
  'near': 'near',
  'tron': 'tron',
  'trx': 'tron',
  'chainlink': 'chainlink',
  'link': 'chainlink',
  'uniswap': 'uniswap',
  'uni': 'uniswap',
  'aave': 'aave',
  'curve': 'curve-dao-token',
  'crv': 'curve-dao-token',
  'yearn': 'yearn-finance',
  'yfi': 'yearn-finance',
  'monero': 'monero',
  'xmr': 'monero',
  'zcash': 'zcash',
  'zec': 'zcash',
};

/**
 * Get cached prices if available and not expired
 */
const getCachedPrices = (projects: string[]): any | null => {
  const now = Date.now();
  const cacheKey = projects.map(p => p.toLowerCase()).sort().join(',');
  
  if (priceCache.has(cacheKey)) {
    const { data, timestamp } = priceCache.get(cacheKey)!;
    if (now - timestamp < CACHE_TTL) {
      console.log(`✅ Cache hit for: ${cacheKey}`);
      return data;
    }
  }
  
  return null;
};

/**
 * Store prices in cache
 */
const setCachedPrices = (projects: string[], data: any): void => {
  const cacheKey = projects.map(p => p.toLowerCase()).sort().join(',');
  priceCache.set(cacheKey, { data, timestamp: Date.now() });
  console.log(`💾 Cached prices for: ${cacheKey}`);
};

/**
 * Fetch prices from CoinGecko API
 */
const fetchFromCoinGecko = async (coinIds: string[]): Promise<any> => {
  const ids = coinIds.join(',');
  const url = `https://api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true`;
  
  console.log(`📡 Fetching from CoinGecko: ${url.substring(0, 80)}...`);
  
  const response = await fetch(url, {
    headers: { 'Accept': 'application/json' },
  });
  
  if (!response.ok) {
    throw new Error(`CoinGecko API error: ${response.status} ${response.statusText}`);
  }
  
  return await response.json();
};

/**
 * Format CoinGecko response into standardized format
 */
const formatPriceData = (coinGeckoData: any, projects: string[]): Record<string, any> => {
  const formatted: Record<string, any> = {};
  
  for (const project of projects) {
    const geckoId = PROJECT_TO_COINGECKO_MAP[project.toLowerCase()];
    if (!geckoId) continue;
    
    const data = coinGeckoData[geckoId];
    if (!data) continue;
    
    formatted[project.toLowerCase()] = {
      name: project,
      symbol: geckoId.slice(0, 3).toUpperCase(), // Simple symbol generation
      price_usd: data.usd,
      market_cap_usd: data.usd_market_cap,
      volume_24h_usd: data.usd_24h_vol,
      price_change_24h: data.usd_24h_change,
      price_change_7d: data.usd_7d_change || null,
      last_updated: new Date().toISOString(),
      status: 'success',
    };
  }
  
  return formatted;
};

/**
 * POST /api/prices
 * 
 * Request body:
 * {
 *   "projects": ["bitcoin", "ethereum"],
 *   "forceRefresh": false
 * }
 * 
 * Response:
 * {
 *   "bitcoin": {
 *     "name": "Bitcoin",
 *     "symbol": "BTC",
 *     "price_usd": 45123.45,
 *     "market_cap_usd": 890000000000,
 *     "volume_24h_usd": 23000000000,
 *     "price_change_24h": 5.2,
 *     "price_change_7d": 12.5,
 *     "last_updated": "2025-10-31T12:34:56Z",
 *     "status": "success"
 *   }
 * }
 */
export async function POST(request: Request) {
  try {
    const { projects, forceRefresh = false } = await request.json();
    
    // Validate input
    if (!Array.isArray(projects) || projects.length === 0) {
      return NextResponse.json(
        { error: 'projects must be a non-empty array' },
        { status: 400 }
      );
    }
    
    console.log(`🔍 Price lookup requested for: ${projects.join(', ')}`);
    
    // Check cache first (unless forceRefresh)
    if (!forceRefresh) {
      const cached = getCachedPrices(projects);
      if (cached) {
        return NextResponse.json({
          data: cached,
          source: 'cache',
          timestamp: new Date().toISOString(),
        });
      }
    }
    
    // Map project names to CoinGecko IDs
    const geckoIds = projects
      .map(p => PROJECT_TO_COINGECKO_MAP[p.toLowerCase()])
      .filter(Boolean);
    
    if (geckoIds.length === 0) {
      return NextResponse.json(
        { 
          error: 'No valid projects found',
          validProjects: Object.keys(PROJECT_TO_COINGECKO_MAP),
        },
        { status: 400 }
      );
    }
    
    console.log(`✔️ Mapped to CoinGecko IDs: ${geckoIds.join(', ')}`);
    
    // Fetch from CoinGecko
    const geckoData = await fetchFromCoinGecko(geckoIds);
    
    // Format response
    const formattedData = formatPriceData(geckoData, projects);
    
    // Cache the results
    setCachedPrices(projects, formattedData);
    
    console.log(`✅ Successfully fetched prices for ${Object.keys(formattedData).length} projects`);
    
    return NextResponse.json({
      data: formattedData,
      source: 'live',
      timestamp: new Date().toISOString(),
    });
    
  } catch (error: any) {
    console.error('❌ Price lookup error:', error);
    
    return NextResponse.json(
      { 
        error: 'Failed to fetch prices',
        message: error.message,
      },
      { status: 500 }
    );
  }
}

/**
 * GET /api/prices?projects=bitcoin,ethereum&forceRefresh=false
 * 
 * Alternative GET endpoint for simple queries
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const projectsParam = searchParams.get('projects');
    const forceRefresh = searchParams.get('forceRefresh') === 'true';
    
    if (!projectsParam) {
      return NextResponse.json(
        { error: 'projects query parameter is required' },
        { status: 400 }
      );
    }
    
    const projects = projectsParam.split(',').map(p => p.trim());
    
    // Delegate to POST handler
    return await POST(
      new Request(request, {
        method: 'POST',
        body: JSON.stringify({ projects, forceRefresh }),
      })
    );
    
  } catch (error: any) {
    console.error('❌ Price lookup error:', error);
    
    return NextResponse.json(
      { error: 'Failed to fetch prices' },
      { status: 500 }
    );
  }
}
