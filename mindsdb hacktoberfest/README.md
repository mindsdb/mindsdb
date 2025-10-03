# 🎃 MindsDB Hacktoberfest 2025 

## Supercharging AI analytical Apps with Knowledge Bases ⚡

This Hacktoberfest, MindsDB challenges you to build RAG apps using Knowledge Bases.


### 🌟 Why Join?
MindsDB's Hacktoberfest is your chance to turn code into impact:
- Build tools that answer real business questions.
- Help teams move beyond rigid dashboards and siloed data.
- Level up your open-source contributions with AI-native analytics apps.
- Compete for prizes: GitHub sponsorships, swag, and a Prize Draw for a [MacBook Pro 16" M4 Chip](https://www.apple.com/shop/buy-mac/macbook-pro/16-inch-space-black-standard-display-apple-m4-pro-with-14-core-cpu-and-20-core-gpu-48gb-memory-512gb).
- Get your project featured on the MindsDB blog + community.

**Your mission:** Create AI apps powered by MindsDB's Knowledge Bases that query enterprise-like data in place—delivering accurate, explainable answers.


------

## 🛠️ Core Task

- Pick a use case where there is unstructured data and can benefit from making it searchable via natural language: (For example analyzing CRM Unstructured data: Notes, Emails, Calls, Meetings, Tasks, Conversations → transcripts, attachments, Tickets → Descriptions, associated notes/emails)
- Pick the datasources that you will need for these use cases
- Write a blog post (Medium, Hashnode, dev.to, LinkedIn) explaining your use case.
- Write a pull request with your use-case implementation in the use-cases folder (create a folder for your use case with a descriptive name)
- Your use case implementation can be either a notebook or an app, that uses MindsDB + Knowledge bases
- Promote your use case on Linkedin, and X/Twitter with a post mentioning @mindsdb.

-----

## 🏆 Prize Categories

Stand a chance to win a [MacBook Pro 16" M4 Chip](https://www.apple.com/shop/buy-mac/macbook-pro/16-inch-space-black-standard-display-apple-m4-pro-with-14-core-cpu-and-20-core-gpu-48gb-memory-512gb) in our Prize Draw!

### 🔥 Most Popular Pull Requests
- Top 3 Pull Requests with the most thumbs up (👍) or heart (❤️) reaction wins win GitHub sponsorship prizes.
- Every 10 positive reaction = 1 entry into the Apple MacBook Pro prize draw.
  
**Prizes:**
- 🥇 $1500 + MindsDB T-shirt
- 🥈 $1000 + MindsDB T-shirt
- 🥉 $500 + MindsDB T-shirt


(Note: GitHub sponsorship must be available in your country in order to receive the prize, participants to check before they contribute. Automated voting is not allowed—violations will be disqualified.)

### 📣 Social Media Awareness
Top 3 posts (LinkedIn/X) with the most engagement win:
- MindsDB T-shirt
- 1 entry into the Apple MacBook Pro prize draw
- $100 Github Sponsorship

(Github Sponsorship may change depending on the amount of engagement a social media post received).

### ✍️ Best Blog Content
Top 3 blog posts (as judged by the MindsDB team) win:
- MindsDB T-shirt
- Blog feature on the official MindsDB website
- 1 entry into the Apple MacBook Pro prize draw
- $100 Github Sponsorship.

----

## 🎯 Goals
- Showcase zero-ETL, data-in-place AI analytics with MindsDB KBs.
- Demonstrate hybrid semantic + SQL logic and use Evaluate KB for quality.
- Encourage integrations (Salesforce, BigQuery, Confluence, Gong, Postgres, etc.).
- Create repeatable app templates for use cases in accordance to our industries listed on our webpage, i.e [Finance Services](https://mindsdb.com/solutions/industry/ai-data-solution-financial-services), [Energy & Utilities](https://mindsdb.com/solutions/industry/ai-data-solution-energy-utilities), [Retail & E-commerce](https://mindsdb.com/solutions/industry/ai-data-solution-retail-ecommerce), [Enterprise Software Vendors](https://mindsdb.com/solutions/industry/ai-data-solution-b2b-tech), or for another Enterprise industry.

## 👩‍💻 Who Should Join?
- AI/ML Enthusiasts (especially RAG & semantic search fans)
- SQL-savvy developers (data engineers, full-stack devs, data scientists)
- Existing MindsDB users & open-source contributors

## 🔑 Example Use Cases
- Decision BI Re-imagined → NLQ → KPIs/charts (with auditability).
- Operations Copilot → Root cause & SOP search across tickets/wikis.
- Customer Intelligence → 360° CRM + docs with explainable recs.
- Compliance & Controls → Policy/filing QA with citations + risk flags.
- Wildcard → Any creative KB-powered analytics app.

## 🛤️ Tracks

### Track 1: Build an application with MindsDB Knowledge Bases

Create a functional application (CLI, Web App, API, Bot Interface etc.) where the primary interaction or feature relies on the semantic query results from the KB. This includes:
  - A functional, empty Knowledge Base exists within their MindsDB instance (Cloud or local)
  - Participant connects a data source (Salesforce, Gong, Hubspot, Postgres or files) and successfully ingests text data into the KB using INSERT INTO. The KB is populated with text data suitable for semantic querying.
  - Demonstrate retrieving meaningful results based on semantic similarity and metadata filtering using [Hybrid Search](https://docs.mindsdb.com/mindsdb_sql/knowledge_bases/hybrid_search). Successfully retrieve relevant data chunks/rows based on semantic queries. 
  - Provide a public GitHub repo with clear setup instructions and documentation, along with a working application that demonstrates a practical use case for Knowledge Bases, supported by a short, shareable demo video showcasing the app in action.

### Track 2: Advanced Capabilities
- Jobs Integration: Auto-update KBs with [CREATE JOB](https://docs.mindsdb.com/mindsdb_sql/sql/create/jobs).
- [Agent Integration](https://docs.mindsdb.com/mindsdb_sql/agents/agent)
- Metadata Filtering: Hybrid search with semantic + structured filters for eg. LIKE and BETWEEN operators.
- [Evaluate Knowledge Bases](https://docs.mindsdb.com/mindsdb_sql/knowledge_bases/evaluate): Produce an evaluation report (MRR, Hit@k, relevancy, etc.).
- [Hybrid Search](https://docs.mindsdb.com/mindsdb_sql/knowledge_bases/hybrid_search): Perform semantic and metadata filtering queries on your data.

-----

## 📦 Deliverables/ Minimum Requirements
- Public GitHub repo with code + infra (Docker optional).
- README: problem statement(what use case this solves), architecture, Knowledge Base schema, SQL examples, metrics.
- Demo UI (CLI or Web) + 5-min demo video
- Sample queries (Natural language + SQL).
- Evaluation report: metrics (MRR, Hit@k, avg relevancy, etc.).
- Blog post explaining how you built the application and what use case it solves.
- Social media posts on LinkedIn and Twitter about your use case, mention @mindsdb.

----

## 🚀 Get Started

- [MindsDB Documentation](https://docs.mindsdb.com/mindsdb)
- [MindsDB Knowledge Bases Documentation](https://docs.mindsdb.com/mindsdb_sql/knowledge_bases/overview)
- [SDK's and API documentation](https://docs.mindsdb.com/overview_sdks_apis)

As the main category is based on the amount of likes/upvotes your Pull Request receives, you can request to have it merged so that you can claim the merged PR by the official [Hacktoberfest organizers](https://hacktoberfest.com/participation/). Pull Requests will be merged 2 hours before the deadline.

**Deadline:**
The competion ends on 31st October 2025 00:00 PST. It is advised to make Pull Requests well in advanced. We wish everyone goodluck!

**Hack smarter. Query faster. Build the Next Generation of AI Analytics Apps with MindsDB.**




