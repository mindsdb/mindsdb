import { NextRequest, NextResponse } from 'next/server';

// Types
interface QueryContext {
  searchMode?: 'auto' | 'kb_only' | 'price_only' | 'combined';
  maxResults?: number;
  timeout?: number;
  filters?: {
    category?: string; // Filter by document category (whitepaper, technical-doc, etc.)
    projects?: string[]; // Filter to specific projects
    dateRange?: {
      start?: string;
      end?: string;
    };
  };
}

interface KBResult {
  content: string;
  relevance: number;
  metadata: {
    _source?: string;
    category?: string;
    chunk_index?: number;
    row_index?: number;
  };
  source: string;
  searchMode: string;
}

interface PriceResult {
  project: string;
  price_usd: number;
  market_cap_usd: number;
  volume_24h_usd: number;
  price_change_24h: number;
  price_change_7d: number;
  last_updated: string;
}

interface AgentResponse {
  queryId: string;
  originalQuery: string;
  classifiedAs: 'kb_only' | 'price_only' | 'combined' | 'auto';
  results: {
    kb_results?: KBResult[];
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
}

// ============================================================
// AGENT CLASSIFICATION LOGIC
// ============================================================

/**
 * Classify query into: kb_only, price_only, combined, or auto
 */
function classifyQuery(query: string): {
  type: 'kb_only' | 'price_only' | 'combined' | 'auto';
  reasoning: string;
  detectedProjects: string[];
} {
  const lowerQuery = query.toLowerCase();

  // Price-related keywords
  const priceKeywords = [
    'price',
    'cost',
    'worth',
    'market',
    'trading',
    'bullish',
    'bearish',
    'chart',
    'volume',
    'marketcap',
    'market cap',
    'usd',
    'expensive',
  ];

  // Technical/Protocol keywords
  const technicalKeywords = [
    'consensus',
    'whitepaper',
    'protocol',
    'algorithm',
    'mechanism',
    'network',
    'validation',
    'mining',
    'stake',
    'hash',
    'block',
    'transaction',
    'security',
    'cryptography',
    'smart contract',
    'proof of work',
    'proof of stake',
    'Byzantine',
    'technology',
    'technical',
    'how does',
    'how it works',
    'explain',
    'works',
    'function',
    'implementation',
    'design',
    'architecture',
    'development',
    'infrastructure',
  ];

  // Crypto project names and symbols
  const projectPatterns: { [key: string]: string } = {
    bitcoin: 'bitcoin',
    btc: 'bitcoin',
    ethereum: 'ethereum',
    eth: 'ethereum',
    solana: 'solana',
    sol: 'solana',
    cardano: 'cardano',
    ada: 'cardano',
    polkadot: 'polkadot',
    dot: 'polkadot',
    ripple: 'ripple',
    xrp: 'ripple',
    litecoin: 'litecoin',
    ltc: 'litecoin',
    dogecoin: 'dogecoin',
    doge: 'dogecoin',
    polygon: 'polygon',
    matic: 'polygon',
    arbitrum: 'arbitrum',
    arb: 'arbitrum',
    optimism: 'optimism',
    op: 'optimism',
    avalanche: 'avalanche',
    avax: 'avalanche',
    cosmos: 'cosmos',
    atom: 'cosmos',
    near: 'near',
    tron: 'tron',
    trx: 'tron',
    chainlink: 'chainlink',
    link: 'chainlink',
    uniswap: 'uniswap',
    uni: 'uniswap',
    aave: 'aave',
    curve: 'curve',
    crv: 'curve',
    yearn: 'yearn-finance',
    yfi: 'yearn-finance',
    monero: 'monero',
    xmr: 'monero',
    zcash: 'zcash',
    zec: 'zcash',
  };

  // Detect projects mentioned in query
  const detectedProjects: string[] = [];
  for (const [key, project] of Object.entries(projectPatterns)) {
    if (lowerQuery.includes(key)) {
      if (!detectedProjects.includes(project)) {
        detectedProjects.push(project);
      }
    }
  }

  // Count keyword occurrences
  const priceKeywordCount = priceKeywords.filter((kw) =>
    lowerQuery.includes(kw)
  ).length;
  const technicalKeywordCount = technicalKeywords.filter((kw) =>
    lowerQuery.includes(kw)
  ).length;

  // Classification logic
  if (priceKeywordCount > 0 && technicalKeywordCount > 0) {
    return {
      type: 'combined',
      reasoning: `Query contains both price terms (${priceKeywordCount}) and technical terms (${technicalKeywordCount}). Executing both KB search and price fetch.`,
      detectedProjects,
    };
  }

  if (priceKeywordCount > 0) {
    return {
      type: 'price_only',
      reasoning: `Query contains ${priceKeywordCount} price-related keywords. Fetching live price data.`,
      detectedProjects,
    };
  }

  if (technicalKeywordCount > 0) {
    return {
      type: 'kb_only',
      reasoning: `Query contains ${technicalKeywordCount} technical keywords. Searching knowledge base.`,
      detectedProjects,
    };
  }

  if (detectedProjects.length > 0) {
    return {
      type: 'auto',
      reasoning: `Detected crypto project mentions (${detectedProjects.join(', ')}). Using adaptive detection.`,
      detectedProjects,
    };
  }

  return {
    type: 'auto',
    reasoning: 'No clear classification. Using adaptive mode.',
    detectedProjects: [],
  };
}

// ============================================================
// KB SEARCH EXECUTION
// ============================================================

async function executeKBSearch(
  query: string,
  detectedProjects: string[] = [],
  maxResults: number = 5,
  filters?: QueryContext['filters']
): Promise<{ results: KBResult[]; duration: number }> {
  const startTime = Date.now();

  try {
    console.log(`[KB Search] Querying for: "${query}"`);
    console.log(`[KB Search] Detected projects: ${detectedProjects.join(', ') || 'none'}`);
    console.log(`[KB Search] Filters:`, filters);
    
    // Build WHERE clause based on filters
    const whereConditions: string[] = [];
    
    // Project filter (either from detection or explicit filter)
    const projectsToFilter = filters?.projects || detectedProjects;
    if (projectsToFilter.length > 0) {
      const project = projectsToFilter[0].toLowerCase();
      whereConditions.push(`project_id = '${project}'`);
    }
    
    // Category filter
    if (filters?.category) {
      whereConditions.push(`category = '${filters.category}'`);
    }
    
    // If we have filters or detected projects, try direct KB query first
    if (whereConditions.length > 0) {
      const whereClause = whereConditions.join(' AND ');
      console.log(`[KB Search] Attempting direct KB query with filters: ${whereClause}`);
      
      try {
        const directResponse = await fetch('http://127.0.0.1:47334/api/sql/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            query: `SELECT chunk_content, project_id, category, source_file FROM mindsdb.web3_kb WHERE ${whereClause} LIMIT ${maxResults};`
          }),
        });

        if (directResponse.ok) {
          const directData = await directResponse.json();
          console.log(`[KB Search] Direct KB query returned ${directData.data?.length || 0} chunks`);
          
          if (directData.data && directData.data.length > 0) {
            // Combine chunks into a single comprehensive answer
            const chunks = directData.data.map((row: any) => row[0]).filter((c: string) => c && c.length > 50);
            
            if (chunks.length > 0) {
              // Combine first few chunks to create a comprehensive answer
              const combinedContent = chunks.slice(0, 3).join('\n\n');
              const duration = Date.now() - startTime;
              
              console.log(`[KB Search] Returning ${chunks.length} KB chunks (${combinedContent.length} chars) in ${duration}ms`);
              
              return {
                results: [{
                  content: combinedContent,
                  relevance: 1.0,
                  metadata: {
                    _source: directData.data[0][3] || 'Knowledge Base',
                    category: directData.data[0][2] || 'document',
                  },
                  source: 'Knowledge Base',
                  searchMode: 'direct',
                }],
                duration
              };
            }
          }
        }
      } catch (directError) {
        console.warn(`[KB Search] Direct KB query failed:`, directError);
        // Fall through to agent query
      }
    }
    
    // Fall back to MindsDB Agent query
    console.log(`[KB Search] Using MindsDB agent`);
    
    // Remove price-related keywords
    const technicalQuery = query
      .replace(/current\s+market\s+price/gi, 'technical specifications')
      .replace(/market\s+price/gi, 'technical specifications')
      .replace(/price\s+(of|for)/gi, 'technical specifications of')
      .replace(/what['^]s\s+the\s+price/gi, 'describe the technical architecture');
    
    // Extract project name
    let project = 'the project';
    if (detectedProjects.length > 0) {
      project = detectedProjects[0].charAt(0).toUpperCase() + detectedProjects[0].slice(1);
    } else {
      const projectMatch = technicalQuery.match(/(?:about|tell me|explain|what is|describe)\s+([a-z\s]+?)(?:\?|and|'s|technology|technical|$)/i);
      project = projectMatch ? projectMatch[1].trim() : 'the project';
    }
    
    const enhancedQuery = `Provide comprehensive and detailed technical information about ${project} specifically.

IMPORTANT: Search the knowledge base for information about ${project} (not other projects).

Include ALL available information for ${project}:
1. Complete technical description and purpose
2. Technical architecture and system design
3. Consensus mechanism and validation process
4. Fee structure, tokenomics, and economic model
5. Key features and innovations
6. Security model and considerations

Be comprehensive:`;
    
    const response = await fetch('http://127.0.0.1:47334/api/sql/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `SELECT answer FROM crypto_auditor_agent WHERE question = '${enhancedQuery.replace(/'/g, "''")}'`
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[KB Search] MindsDB agent query failed: ${response.statusText}`);
      throw new Error(`MindsDB agent query failed: ${response.statusText}`);
    }

    const data = await response.json();
    
    if (!data.data || data.data.length === 0) {
      console.warn(`[KB Search] No data from agent`);
      return { results: [], duration: Date.now() - startTime };
    }
    
    // Handle response format
    let answer = '';
    if (Array.isArray(data.data[0])) {
      answer = data.data[0][0] || '';
    } else {
      answer = String(data.data[0]) || '';
    }
    
    console.log(`[KB Search] Agent answer length: ${answer.length} chars`);
    console.log(`[KB Search] First 200 chars: ${answer.substring(0, 200)}`);
    
    // Filter out tool_code responses or very short answers
    if (answer.includes('tool_code') || answer.includes('kb_query_tool')) {
      console.warn(`[KB Search] Agent returned tool_code response`);
      return { results: [], duration: Date.now() - startTime };
    }
    
    if (answer.length < 50) {
      console.warn(`[KB Search] Agent returned too-short response (${answer.length} chars)`);
      return { results: [], duration: Date.now() - startTime };
    }
    
    const results: KBResult[] = [{
      content: answer,
      relevance: 1.0,
      metadata: {
        _source: 'MindsDB Agent',
        category: 'agent_response'
      },
      source: 'MindsDB Agent',
      searchMode: 'agent',
    }];

    const duration = Date.now() - startTime;
    console.log(`[KB Search] Successfully returned answer (${answer.length} chars) in ${duration}ms`);
    return { results, duration };
  } catch (error) {
    console.error('[KB Search Error]:', error);
    return { results: [], duration: Date.now() - startTime };
  }
}

// ============================================================
// PRICE FETCH EXECUTION
// ============================================================

async function executePriceFetch(
  projects: string[]
): Promise<{ results: PriceResult[]; duration: number }> {
  const startTime = Date.now();

  try {
    if (!projects || projects.length === 0) {
      return { results: [], duration: 0 };
    }

    const response = await fetch('http://localhost:3000/api/prices', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        projects,
        forceRefresh: false,
      }),
    });

    if (!response.ok) {
      throw new Error(`Price API failed: ${response.statusText}`);
    }

    const data = await response.json();
    const duration = Date.now() - startTime;
    
    console.log(`[Price Fetch] API response:`, JSON.stringify(data, null, 2));

    // Transform to PriceResult format
    // API returns: { data: { bitcoin: {...}, ethereum: {...} }, source, timestamp }
    const results: PriceResult[] = [];
    
    if (data.data) {
      for (const [projectKey, projectData] of Object.entries(data.data)) {
        const priceData = projectData as any;
        if (priceData.status === 'success') {
          results.push({
            project: priceData.name || projectKey,
            price_usd: priceData.price_usd,
            market_cap_usd: priceData.market_cap_usd,
            volume_24h_usd: priceData.volume_24h_usd,
            price_change_24h: priceData.price_change_24h,
            price_change_7d: priceData.price_change_7d || 0,
            last_updated: priceData.last_updated,
          });
        }
      }
    }
    
    console.log(`[Price Fetch] Transformed ${results.length} price results`);
    return { results, duration };
  } catch (error) {
    console.error('Price Fetch Error:', error);
    return { results: [], duration: Date.now() - startTime };
  }
}

// ============================================================
// MAIN AGENT HANDLER
// ============================================================

export async function POST(request: NextRequest) {
  const startTime = Date.now();
  const queryId = `q_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  try {
    const body = await request.json();
    const { query, context = {} as QueryContext } = body;

    if (!query) {
      return NextResponse.json(
        { error: 'Query is required' },
        { status: 400 }
      );
    }

    // Classify the query
    const classification = classifyQuery(query);
    const searchMode = context.searchMode || classification.type;
    const maxResults = context.maxResults || 5;

    console.log(`[Agent ${queryId}] Query: "${query}"`);
    console.log(`[Agent ${queryId}] Classification: ${classification.type}`);
    console.log(`[Agent ${queryId}] Detected Projects: ${classification.detectedProjects.join(', ') || 'none'}`);

    // Execute based on classification
    let kbResults: KBResult[] = [];
    let kbDuration = 0;
    let priceResults: PriceResult[] = [];
    let priceDuration = 0;

    // Determine what to execute
    const executeKB =
      searchMode === 'kb_only' ||
      searchMode === 'combined' ||
      (searchMode === 'auto' && classification.type !== 'price_only');
    const executePrice =
      searchMode === 'price_only' ||
      searchMode === 'combined' ||
      (searchMode === 'auto' && classification.detectedProjects.length > 0);

    // Execute in parallel for speed
    const [kbResult, priceResult] = await Promise.all([
      executeKB ? executeKBSearch(query, classification.detectedProjects, maxResults, context.filters) : Promise.resolve({ results: [], duration: 0 }),
      executePrice ? executePriceFetch(classification.detectedProjects) : Promise.resolve({ results: [], duration: 0 }),
    ]);

    kbResults = kbResult.results;
    kbDuration = kbResult.duration;
    priceResults = priceResult.results;
    priceDuration = priceResult.duration;

    const totalDuration = Date.now() - startTime;

    // Build response
    const response: AgentResponse = {
      queryId,
      originalQuery: query,
      classifiedAs: classification.type,
      results: {
        kb_results: kbResults.length > 0 ? kbResults : undefined,
        price_results: priceResults.length > 0 ? priceResults : undefined,
        kbSearchComplete: executeKB,
        priceSearchComplete: executePrice,
      },
      executedAt: {
        kb_search_ms: kbDuration,
        price_fetch_ms: priceDuration,
        total_ms: totalDuration,
      },
      agentReasoning: classification.reasoning,
    };

    console.log(`[Agent ${queryId}] Response ready in ${totalDuration}ms`);
    console.log(`[Agent ${queryId}] KB Results: ${kbResults.length}, Price Results: ${priceResults.length}`);

    return NextResponse.json(response);
  } catch (error) {
    console.error(`[Agent Error] ${error}`);
    return NextResponse.json(
      { 
        error: 'Agent processing failed',
        queryId,
        details: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// ============================================================
// HEALTH CHECK ENDPOINT
// ============================================================

export async function GET() {
  return NextResponse.json({
    status: 'healthy',
    agent: 'crypto-auditor-agent-v1',
    capabilities: ['kb_search', 'price_fetch', 'query_classification', 'parallel_execution'],
    endpoints: {
      query_endpoint: 'POST /api/agent/query',
      kb_only: 'POST /api/agent/query with context.searchMode="kb_only"',
      price_only: 'POST /api/agent/query with context.searchMode="price_only"',
      combined: 'POST /api/agent/query with context.searchMode="combined"',
      auto: 'POST /api/agent/query with context.searchMode="auto" (default)',
    },
  });
}
