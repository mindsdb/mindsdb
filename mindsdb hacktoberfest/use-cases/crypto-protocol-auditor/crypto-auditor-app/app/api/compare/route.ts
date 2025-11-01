import { NextRequest, NextResponse } from 'next/server';

interface ComparisonProject {
  name: string;
  technicalInfo: {
    content: string;
    category: string;
    consensusMechanism?: string;
    layer?: string;
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
  lastUpdated: string;
}

interface ComparisonResponse {
  projects: ComparisonProject[];
  comparisonSummary: string;
  generatedAt: string;
}

/**
 * Fetch KB data for a project
 */
async function fetchProjectKBData(project: string): Promise<ComparisonProject['technicalInfo']> {
  try {
    const query = `
      SELECT chunk_content, project_id, category
      FROM mindsdb.web3_kb
      WHERE project_id = '${project.toLowerCase()}'
      LIMIT 3;
    `;

    const response = await fetch('http://127.0.0.1:47334/api/sql/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });

    const data = await response.json();

    if (data.data && data.data.length > 0) {
      // Combine first 3 chunks
      const chunks = data.data.map((row: any[]) => row[0]);
      const combinedContent = chunks.join('\n\n');
      const category = data.data[0]?.[2] || 'Unknown';

      return {
        content: combinedContent.substring(0, 800), // Limit for comparison view
        category,
      };
    }

    return {
      content: 'No technical documentation found.',
      category: 'Unknown',
    };
  } catch (error) {
    console.error(`Error fetching KB data for ${project}:`, error);
    return {
      content: 'Error fetching technical data.',
      category: 'Error',
    };
  }
}

/**
 * Fetch price data for a project
 */
async function fetchProjectPrice(project: string): Promise<ComparisonProject['priceData']> {
  try {
    const response = await fetch(`http://localhost:3000/api/prices`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ projects: [project] }),
    });
    const data = await response.json();

    if (data.data && data.data[project.toLowerCase()]) {
      return data.data[project.toLowerCase()];
    }

    return null;
  } catch (error) {
    console.error(`Error fetching price for ${project}:`, error);
    return null;
  }
}

/**
 * Fetch sentiment data for a project
 */
async function fetchProjectSentiment(project: string): Promise<ComparisonProject['sentiment']> {
  try {
    const response = await fetch(`http://localhost:3000/api/sentiment?project=${project}&days=7`);
    const data = await response.json();

    if (data.sentiment) {
      return {
        sentiment: data.sentiment,
        score: data.score,
        confidence: data.confidence,
        summary: data.summary,
        newsCount: data.newsCount,
      };
    }

    return null;
  } catch (error) {
    console.error(`Error fetching sentiment for ${project}:`, error);
    return null;
  }
}

/**
 * Generate AI comparison summary using MindsDB agent
 */
async function generateComparisonSummary(projects: ComparisonProject[]): Promise<string> {
  try {
    const projectSummaries = projects.map(p => `
**${p.name.toUpperCase()}**:
- Category: ${p.technicalInfo.category}
- Price: ${p.priceData ? `$${p.priceData.price_usd}` : 'N/A'}
- Market Cap: ${p.priceData ? `$${(p.priceData.market_cap_usd / 1e9).toFixed(2)}B` : 'N/A'}
- Sentiment: ${p.sentiment?.sentiment || 'N/A'} (${p.sentiment?.newsCount || 0} articles)
- 24h Change: ${p.priceData ? `${p.priceData.price_change_24h.toFixed(2)}%` : 'N/A'}
    `).join('\n\n');

    const comparisonQuery = `
Compare these cryptocurrency protocols and provide a brief analysis (3-4 sentences) highlighting key differences in technology, market performance, and sentiment:

${projectSummaries}

Focus on: technical architecture differences, market position, and recent sentiment trends.
    `.trim();

    const query = `SELECT answer FROM crypto_auditor_agent WHERE question = '${comparisonQuery.replace(/'/g, "''")}';`;

    const response = await fetch('http://127.0.0.1:47334/api/sql/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });

    const data = await response.json();

    if (data.data && data.data.length > 0) {
      return data.data[0]?.[0] || 'Comparison summary not available.';
    }

    return 'Comparison summary not available.';
  } catch (error) {
    console.error('Error generating comparison summary:', error);
    return 'Unable to generate comparison summary.';
  }
}

/**
 * GET /api/compare?projects=bitcoin,ethereum,ripple
 */
export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const projectsParam = searchParams.get('projects');

  if (!projectsParam) {
    return NextResponse.json(
      { error: 'Missing required parameter: projects (comma-separated list)' },
      { status: 400 }
    );
  }

  const projectNames = projectsParam.split(',').map(p => p.trim()).filter(p => p);

  if (projectNames.length < 2) {
    return NextResponse.json(
      { error: 'Please provide at least 2 projects to compare' },
      { status: 400 }
    );
  }

  if (projectNames.length > 5) {
    return NextResponse.json(
      { error: 'Maximum 5 projects can be compared at once' },
      { status: 400 }
    );
  }

  try {
    // Fetch all data for each project in parallel
    const projectsData = await Promise.all(
      projectNames.map(async (projectName) => {
        const [technicalInfo, priceData, sentiment] = await Promise.all([
          fetchProjectKBData(projectName),
          fetchProjectPrice(projectName),
          fetchProjectSentiment(projectName),
        ]);

        const project: ComparisonProject = {
          name: projectName,
          technicalInfo,
          priceData,
          sentiment,
          lastUpdated: new Date().toISOString(),
        };

        return project;
      })
    );

    // Generate AI comparison summary
    const comparisonSummary = await generateComparisonSummary(projectsData);

    const response: ComparisonResponse = {
      projects: projectsData,
      comparisonSummary,
      generatedAt: new Date().toISOString(),
    };

    return NextResponse.json(response);
  } catch (error) {
    console.error('Comparison error:', error);
    return NextResponse.json(
      { error: 'Failed to generate comparison', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}

/**
 * POST /api/compare - Compare projects with custom filters
 */
export async function POST(request: NextRequest) {
  try {
    const { projects, includeTechnical = true, includePrices = true, includeSentiment = true } = await request.json();

    if (!Array.isArray(projects) || projects.length < 2) {
      return NextResponse.json(
        { error: 'Please provide at least 2 projects in the projects array' },
        { status: 400 }
      );
    }

    if (projects.length > 5) {
      return NextResponse.json(
        { error: 'Maximum 5 projects can be compared at once' },
        { status: 400 }
      );
    }

    // Fetch data based on filters
    const projectsData = await Promise.all(
      projects.map(async (projectName: string) => {
        const [technicalInfo, priceData, sentiment] = await Promise.all([
          includeTechnical ? fetchProjectKBData(projectName) : Promise.resolve({ content: '', category: '' }),
          includePrices ? fetchProjectPrice(projectName) : Promise.resolve(null),
          includeSentiment ? fetchProjectSentiment(projectName) : Promise.resolve(null),
        ]);

        const project: ComparisonProject = {
          name: projectName,
          technicalInfo,
          priceData,
          sentiment,
          lastUpdated: new Date().toISOString(),
        };

        return project;
      })
    );

    const comparisonSummary = await generateComparisonSummary(projectsData);

    const response: ComparisonResponse = {
      projects: projectsData,
      comparisonSummary,
      generatedAt: new Date().toISOString(),
    };

    return NextResponse.json(response);
  } catch (error) {
    console.error('Comparison error:', error);
    return NextResponse.json(
      { error: 'Failed to generate comparison', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
