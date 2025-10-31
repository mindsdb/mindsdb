# AIRLYTICS - Airline Review Intelligence Platform

**Author:** [Rajesh Adhikari](https://github.com/rajesh-adk-137)  
**Hackathon:** MindsDB Hacktoberfest 2025 — Track 2 (Advanced Capabilities)  
**Category:** Hybrid Semantic + Analytical AI App  
**Status:** ✅ Completed  
**Demo:** [Watch on YouTube](https://youtu.be/KkKMPIlNErU?si=fk1KI5NXdW6ItY4l)


## Summary   

This PR introduces **AIRLYTICS**, a production-grade conversational analytics engine that transforms 20,000+ unstructured airline reviews into actionable business intelligence. Built for MindsDB Hacktoberfest 2025 Track 2, it showcases advanced RAG capabilities through dual-agent architecture, intelligent query routing, and AI-powered strategic interpretation—all powered by MindsDB Knowledge Bases.

## Why It Matters

**Solves a real business problem:** Airlines drown in feedback but starve for insight. Traditional BI tools can't understand natural language, require rigid schemas, and provide numbers without strategy. AIRLYTICS enables operations teams to ask questions like *"Among users complaining about baggage delays, what % rated ground service above 4?"* and get instant statistical breakdowns with AI-generated recommendations.

**Demonstrates end-to-end MindsDB KB workflow:** Zero-ETL ingestion (Google Sheets → KB) → semantic search with metadata filtering → dual-agent interpretation (query routing + strategic insights) → interactive visualizations → management-ready recommendations.

**Production-ready architecture:** Full-stack web app (React + FastAPI) with 5 specialized analytical engines, smart sampling (user-controlled Top-N), and extensible design applicable to any feedback-driven industry.

## What's Included

**Use case folder:** `use-cases/airlytics-airline-review-intelligence/`

**Data pipeline:** Google Sheets (20K+ reviews) → MindsDB Knowledge Base with 14 metadata columns

**Dual-agent system:**
- `analytics_query_agent`: Interprets natural language queries, routes to appropriate analytical functions
- `insight_interpreter_agent`: Transforms raw statistics into strategic business recommendations

**Five analytical engines:** General percentage distribution, conditional distribution, category-to-category, rating-to-rating, conditional rating analysis

**Full-stack app:**
- Backend: FastAPI with `/base_case`, `/smart_case`, `/interpret_agent` endpoints
- Frontend: React + TailwindCSS with dual-mode interface (semantic search + advanced analytics)
- Visualizations: Recharts, Plotly.js for interactive charts, heatmaps, distributions

**Comprehensive docs:** Architecture, setup guide, SQL examples, API reference, sample queries

## Architecture

<img width="400" alt="System Architecture" src="https://github.com/user-attachments/assets/15c29516-87e3-433b-b6b0-b64c384af2b0" />

**Data Flow:**
1. Reviews ingested from Google Sheets into MindsDB KB with vector embeddings
2. User query → Analytics Agent (interprets intent, routes to function)
3. KB semantic search + metadata filtering (Top-N sampling)
4. Statistical computation via specialized engines
5. Insight Agent generates strategic recommendations
6. Frontend renders visualizations + AI insights

## Quick Start

**Prerequisites:** Docker, Python 3.8+, Node.js 16+, OpenAI API key

**Backend setup:**
```bash
cd backend
docker compose up -d  # Start MindsDB
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
# Follow mindsdb/mindsdb_readme.md to create KB + agents
uvicorn app:app --reload  # http://localhost:8000
```

**Frontend setup:**
```bash
cd frontend
yarn install && yarn run dev  # http://localhost:5173
```

**MindsDB Studio:** http://localhost:47334

## MindsDB SQL Examples

**Create Knowledge Base with metadata:**
```sql
CREATE KNOWLEDGE_BASE airline_kb_10000
USING
  embedding_model = {
    "provider": "openai",
    "model_name": "text-embedding-3-large",
    "api_key": "YOUR_KEY"
  },
  metadata_columns = [
    'airline_name', 'overall_rating', 'verified', 'seat_type',
    'type_of_traveller', 'seat_comfort', 'cabin_staff_service',
    'food_beverages', 'ground_service', 'wifi_connectivity', 
    'value_for_money', 'recommended'
  ],
  content_columns = ['review'],
  id_column = 'unique_id';
```

**Semantic search with metadata filtering:**
```sql
SELECT *
FROM airline_kb_10000
WHERE content = 'poor wifi and helpful staff'
  AND airline_name = 'Emirates'
  AND seat_type = 'Business Class'
  AND overall_rating >= 7
LIMIT 50;
```

**Agent-powered analytical query:**
```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Among users who mentioned poor inflight meals, what percent of those who rated food and beverages above 4 also recommended the airline?';
```

**AI insight generation:**
```sql
SELECT answer
FROM insight_interpreter_agent
WHERE question = '{json_context_with_statistics_and_top_reviews}';
```

**Automated data freshness:**
```sql
CREATE JOB airline_kb_job AS (
  INSERT INTO airline_kb_10000
  SELECT unique_id, review, airline_name, overall_rating, [...]
  FROM airline_sheet_10000.airline_review_10000
  WHERE unique_id > 10000
) EVERY 1 minute;
```

## Demo & Documentation

**📝 Blog post:** [Talk to Your Data Like a Human: How I Built an AI Airline Analyst](https://dev.to/rajesh-adk-137/talk-to-your-data-like-a-human-how-i-built-an-ai-airline-analyst-4pl9)

**🐦 Twitter/X:** [Project announcement](https://x.com/BoTie137/status/1984042973580161353)

**📹 Demo video:** [Youtube](https://www.youtube.com/watch?v=KkKMPIlNErU)

**📂 Original repo:** [github.com/rajesh-adk-137/AIRLYTICS](https://github.com/rajesh-adk-137/AIRLYTICS)

**📚 Full documentation:** Available in main repository with setup guides, architecture diagrams, API reference

## Knowledge Base Schema

**Source data (Google Sheets):**
- `unique_id` (primary key)
- `review` (unstructured text)
- `airline_name` (50+ airlines, auto-grouped as "Others")
- `overall_rating` (1-10)
- `seat_type` (Economy/Premium Economy/Business/First Class)
- `type_of_traveller` (Solo/Couple/Family Leisure, Business)
- 7 service ratings: `seat_comfort`, `cabin_staff_service`, `food_beverages`, `ground_service`, `inflight_entertainment`, `wifi_connectivity`, `value_for_money` (1-5 scale)
- `recommended` (yes/no)
- `verified` (true/false)

**KB query-time fields:** `content`, `chunk_content`, `relevance`, `distance`, all metadata columns

## Sample Queries

**Natural language (semantic search):**
- *"excellent legroom and comfortable seats"*
- *"lost baggage, delayed luggage"*
- *"value for money, affordable tickets"*

**Natural language (advanced analytics):**
- *"For passengers complaining about check-in delays, show seat type distribution among those who rated ground service above 3"*
- *"Out of all reviews mentioning slow boarding, what percentage of passengers rated value for money above 4?"*
- *"Users who complained about baggage claim delays — what percentage of those who rated ground service above 4 rated overall experience below 5?"*

**SQL equivalents:** See `mindsdb/mindsdb_readme.md` for complete agent prompts and query examples

## Advanced Capabilities (Track 2)

✅ **Knowledge Base Integration:** 20K+ reviews with semantic search  
✅ **Agent Integration:** Dual-agent architecture (query interpreter + insight generator)  
✅ **Metadata Filtering:** 14 structured fields with complex conditional logic  
✅ **Hybrid Search:** Semantic similarity + SQL filtering (alpha-tunable)  
✅ **Jobs Integration:** Automated data refresh with scheduled ingestion  
✅ **Zero-ETL Pipeline:** Direct Google Sheets → MindsDB ingestion  

**Unique technical achievements:**
- **Smart sampling:** User-controlled Top-N (10/20/50/75/100) for context-aware statistics
- **Five analytical engines:** Automatic query routing to specialized statistical functions
- **InsightInterpreter:** AI agent trained to generate strategic recommendations, not summaries
- **Production-ready:** Full authentication flow, error handling, responsive UI

## Extensibility Beyond Airlines

This architecture is a **template for any feedback analytics domain:**

🏨 Hotels → Guest reviews, amenity ratings  
🍽️ Restaurants → Menu feedback, delivery experiences  
🛒 E-commerce → Product reviews, return issues  
🏥 Healthcare → Patient satisfaction, facility feedback  
📞 Support → Ticket descriptions, call transcripts  

**Swap the schema, retrain agents on domain language, deploy.**

## Tech Stack

**Frontend:** React, Vite, TailwindCSS, Lucide Icons, Recharts, Plotly.js  
**Backend:** FastAPI (Python), MindsDB SDK  
**AI Layer:** MindsDB Knowledge Bases + Agents, OpenAI (text-embedding-3-large, gpt-4-mini)  
**Data:** Google Sheets (zero-ETL)  
**Container:** Docker Compose  
**Visualization:** Interactive heatmaps, distributions, conditional charts

## Reviewer Notes

- **OpenAI API key required** for embeddings and agents (see setup guide)
- **Sample data provided** via Google Sheets integration (20K+ reviews)
- **Endpoints:** `/base_case` (semantic search), `/smart_case` (agent analytics), `/interpret_agent` (insights)
- **Dual-mode UI:** Standard search with stats + Advanced analytics with visualizations
- **Smart sampling:** All metrics computed on Top-N semantically matched reviews (user-configurable)

## Checklist (Hacktoberfest Deliverables)

✅ Public full-stack app with MindsDB KB + dual agents  
✅ Comprehensive README with problem statement, architecture, KB schema, SQL examples  
✅ Blog post and social media posts (LinkedIn, Twitter/X)  
✅ Sample queries (natural language + SQL)  
✅ Production-ready code with error handling, responsive UI  
✅ Extensible template for other industries  
✅ Complete setup documentation  

---

**AIRLYTICS: Where natural language meets strategic intelligence. Ask questions in English. Get insights that drive decisions.**
