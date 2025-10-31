# ✈️ AIRLYTICS

<p align="center">
  <a href="https://github.com/rajesh-adk-137/AIRLYTICS/watchers" target="_blank">
    <img src="https://img.shields.io/github/watchers/rajesh-adk-137/AIRLYTICS?style=for-the-badge&logo=appveyor" alt="Watchers"/>
  </a>
  <a href="https://github.com/rajesh-adk-137/AIRLYTICS/fork" target="_blank">
    <img src="https://img.shields.io/github/forks/rajesh-adk-137/AIRLYTICS?style=for-the-badge&logo=appveyor" alt="Forks"/>
  </a>
  <a href="https://github.com/rajesh-adk-137/AIRLYTICS/stargazers" target="_blank">
    <img src="https://img.shields.io/github/stars/rajesh-adk-137/AIRLYTICS?style=for-the-badge&logo=appveyor" alt="Stars"/>
  </a>
</p>

<p align="center">
  <strong>🎃 MindsDB Hacktoberfest 2025 | Track 2: Advanced Capabilities</strong>
</p>

---

## 💡 About the Project

**AIRLYTICS** is an AI-native airline review intelligence platform that transforms 20,000+ unstructured passenger reviews into actionable business insights using **MindsDB Knowledge Bases** and **Agents**.

This isn't just another dashboard. It's a **conversational analytics engine** where business users ask questions in plain English and receive instant statistical breakdowns, beautiful visualizations, and AI-powered strategic recommendations.

### The Problem It Solves

Airlines collect thousands of customer reviews across multiple touchpoints, but extracting actionable intelligence from this unstructured feedback is time-consuming and requires technical expertise. Traditional BI tools fall short because:

- They can't understand natural language queries
- They require pre-defined metrics and rigid schemas
- They don't connect semantic context with statistical analysis
- They provide numbers without explaining *what to do about them*

**AIRLYTICS solves this** by combining:
- **Semantic + keyword search** for context-aware review retrieval
- **Metadata filtering** with structured fields (airlines, seat types, traveler categories, ratings)
- **Statistical engines** for deep analytical breakdowns
- **AI interpretation** that converts raw metrics into management-ready insights

### 🌐 Beyond Airlines: A Template for Experience Analytics

While AIRLYTICS focuses on airline reviews, **this architecture is a blueprint for any domain with unstructured feedback data**:

- **🏨 Hotel & Hospitality:** Guest reviews, service feedback, amenity ratings
- **🍽️ Food Services & Restaurants:** Delivery experiences, menu feedback, ambiance comments
- **🛒 E-commerce & Retail:** Product reviews, shopping experiences, return feedback
- **🏥 Healthcare:** Patient satisfaction, appointment experiences, facility reviews
- **📞 Customer Support:** Ticket descriptions, call transcripts, chat logs

With minor schema adjustments, this same semantic + SQL approach can power intelligent analytics for **any business collecting textual feedback**.

---

## ⚡ Key Features

### 🧠 **Dual-Mode Intelligence**

#### **1. Semantic+Keyword Search Mode (Standard)** 
Find reviews using natural language descriptions — powered by MindsDB Knowledge Bases with vector embeddings.

**Perfect for:**
- *"excellent legroom and comfortable seats"*
- *"best crew and inflight service"*
- *"very long delay and bad food"*
- *"lost baggage, delayed luggage"*

**You Get:**
- Top 50 most relevant reviews with full metadata
- Summary statistics (avg rating, recommendation rate, verified users)
- Detailed rating breakdowns (seat comfort, staff, food, wifi, value)
- Distribution analysis (seat types, traveler types)
- Correlation matrices and field completeness
- Per-category averages (by airline, seat class, etc.)

---

#### **2. Advanced Analytics Mode (Agent-Powered)**
Ask complex analytical questions — the AI agent automatically interprets your intent, maps it to structured fields, and executes the appropriate statistical function.

**Perfect for:**
- *"For passengers complaining about check-in delays, show seat type distribution among those who rated ground service above 3."*
- *"Among reviews mentioning crowded flights, how many Economy Class passengers were Solo Leisure travelers compared to Business travelers?"*
- *"Users who complained about baggage claim delays — what percentage of those who rated ground service below 4 rated overall experience below 5?"*

**You Get Everything from Standard Mode PLUS:**
- Multi-dimensional statistical breakdowns
- Advanced visualizations (bar charts, pie charts, stacked comparisons)
- Targeted metrics based on your specific question
- Agent-interpreted semantic queries optimized for your analytical goal

---

### 📊 **Five Specialized Analytical Engines**

The agent automatically routes queries to the right function:

| Function | What It Solves |
|----------|----------------|
| `general_percentage_distribution` | Single-field percentage checks (e.g., "What % rated value above 4?") |
| `conditional_distribution_analysis` | Filtered category breakdowns (e.g., "Show seat types for delayed passengers") |
| `conditional_category_to_category_analysis` | Cross-category comparisons (e.g., "Economy vs Business traveler mix") |
| `conditional_rating_to_rating_analysis` | Numeric correlations with thresholds (e.g., "Low wifi + low satisfaction overlap") |
| `conditional_rating_analysis` | Rating-to-outcome analysis (e.g., "High food scores who also recommended") |

---

### 🎯 **Smart Sampling with Top-N Control**

AIRLYTICS doesn't analyze all 20,000+ reviews for every query — it focuses on **what matters most**.

**How it works:**
- Every query retrieves the **top N semantically matched reviews** based on MindsDB's similarity scoring
- Users control sample size via dropdown: **10, 20, 50, 75, or 100 rows** (100 = MindsDB Knowledge Base limit)
- All statistics, distributions, and correlations are computed from this focused, context-relevant subset

**Why this matters:**
- ✅ Faster query execution
- ✅ Context-aware metrics (not diluted by unrelated reviews)
- ✅ Representative samples that directly answer your question
- ✅ Statistically grounded insights from real, matched data

---

### 🔍 **Rich Metadata Filtering**

AIRLYTICS supports multi-dimensional filtering across structured fields:

**Available Filters:**
- **Airlines:** British Airways, Emirates, Qatar Airways, Singapore Airlines, + others (unknowns auto-grouped as "Others")
- **Aircraft Types:** Boeing 777, Airbus A380, 787 Dreamliner, etc.
- **Seat Types:** Economy, Premium Economy, Business, First Class
- **Traveler Types:** Solo Leisure, Family Leisure, Couple Leisure, Business
- **Route Types:** Short-haul, Medium-haul, Long-haul
- **Verification Status:** Verified vs Trip Verified users
- **Date Ranges:** Filter by review publication dates
- **Recommendation Status:** Recommended vs Not Recommended
- **Rating Thresholds:** Any numeric field (overall_rating, seat_comfort, wifi_connectivity, food_beverages, etc.)

**Example Semantic + Metadata Query:**
```sql
SELECT content, airline, seat_type, overall_rating
FROM airline_reviews_kb
WHERE search_query = 'poor Wi-Fi and helpful staff'
  AND airline = 'Emirates'
  AND seat_type = 'Business Class'
  AND overall_rating >= 7
LIMIT 50;
```

---

### 💡 **AI InsightInterpreter**

Raw statistics tell you *what happened*. **InsightInterpreter tells you what to do about it.**

After any query (semantic or analytical), click **"Get AI Insights"** to activate your AI data strategist.

**What It Does:**
- ✅ Identifies hidden patterns and contradictions
- ✅ Explains root causes behind metrics
- ✅ Provides strategic + operational recommendations
- ✅ Writes in executive-friendly, decision-ready language

**Example Transformation:**

| ❌ Generic Output | ✅ InsightInterpreter Output |
|------------------|------------------------------|
| "Wi-Fi ratings average 2.8/5" | "Wi-Fi below 3 stars drops overall satisfaction by 68%. Upgrade routers on long-haul routes — it impacts loyalty more than catering." |
| "48% of users recommended the airline" | "High ratings but low recommendations suggest good service, weak loyalty. Review pricing strategy and frequent flyer perks." |
| "Business travelers rate cabin cleanliness 6.2/10" | "Business travelers forgive delays but hate dirty cabins. Prioritize cleaning staff over gate efficiency." |

**What It Doesn't Do:**
- ❌ Repeat your query or list basic stats
- ❌ Use filler phrases like "The data shows..."
- ❌ Give shallow or generic takeaways

**What It Does:**
- ✅ Uncover cause-effect relationships
- ✅ Speak like a data-driven strategist
- ✅ Deliver precise, actionable next steps

---


## 🏗️ Architecture Overview
<img width="512" height="768" alt="System Architecture" src="https://github.com/user-attachments/assets/15c29516-87e3-433b-b6b0-b64c384af2b0" />

---

## ⚙️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | React (Vite), TailwindCSS, Lucide Icons | User interface, query input, visualizations |
| **Backend** | FastAPI (Python) | API endpoints, query orchestration |
| **AI/Search** | MindsDB Knowledge Bases | Semantic,keyword and metadata filtering |
| **Agents** | MindsDB Agents (OpenAI) | Query interpretation, function selection |
| **Database** | Google Sheets → MindsDB KB | Zero-ETL data ingestion |
| **Containerization** | Docker Compose | MindsDB instance management |
| **Visualization** | Recharts, Plotly.js | Interactive charts and plots |

---

## 🚀 Installation Guide

### Prerequisites

Ensure your system has:
- **Docker Desktop** & **Docker Compose**
- **Python 3.8+**
- **Node.js 16+** & **Yarn**

---

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/rajesh-adk-137/AIRLYTICS.git
cd AIRLYTICS
```

---

### 2️⃣ Frontend Setup

#### Navigate to frontend directory
```bash
cd frontend
```

#### Install dependencies
```bash
yarn install
```

#### Start development server
```bash
yarn run dev
```

#### Access the app
Open [http://localhost:5173](http://localhost:5173) in your browser

---

### 3️⃣ Backend Setup

#### Navigate to backend directory
```bash
cd ../backend
```

#### Create virtual environment

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

#### Install Python dependencies
```bash
pip install -r requirements.txt
```

#### Start MindsDB container
```bash
docker compose up -d
```

**To stop MindsDB:**
```bash
docker compose down
```

#### Start FastAPI backend
```bash
uvicorn app:app --reload
```

#### Access services
- **MindsDB Studio:** [http://localhost:47334](http://localhost:47334)
- **FastAPI Backend:** [http://localhost:8000](http://localhost:8000)

---

### 4️⃣ Initialize MindsDB

Follow the detailed setup instructions in [`mindsdb_readme.md`](mindsdb/mindsdb_readme.md) to:
- Create the Knowledge Base
- Configure AI Tables (OpenAI)
- Set up Agents for query routing
- Ingest sample data

---

## 🎃 MindsDB Hacktoberfest 2025 Submission

**Category:** Track 2 – Advanced Capabilities

### 🏆 Advanced Features Demonstrated

✅ **Knowledge Base Integration:** Complete RAG pipeline with semantic search  
✅ **Agent Integration:** AI-driven query interpretation and function routing  
✅ **Hybrid Search:** Semantic, keyword and metadata filtering capabilities  
✅ **Metadata Filtering:** Multi-dimensional structured field filtering  
✅ **Jobs Integration:** Automated KB updates 
✅ **Zero-ETL Architecture:** Direct Google Sheets → MindsDB ingestion  

### 🎯 Why This Submission Stands Out

1. **Complete RAG-to-BI Pipeline:** Not just search — full analytical capabilities with visualizations
2. **Dual-Agent Architecture:** Semantic + Statistical agents working in tandem
3. **Explainable AI:** Every metric comes with business context and recommendations
4. **Production-Ready Design:** Scalable architecture, clean UI, comprehensive docs
5. **Extensible Template:** Same approach works for hotels, restaurants, e-commerce, healthcare

### 📦 Deliverables Checklist

- ✅ Public GitHub repository with complete source code
- ✅ Comprehensive README with problem statement, architecture, examples
- ✅ Working web application (React + FastAPI)
- ✅ MindsDB Knowledge Base with 20,000+ ingested reviews
- ✅ Demo video on youtube
- ✅ Sample SQL queries and natural language examples
- ✅ Blog post explaining the build process 


---

## 📺 Demo & Screenshots

### 🎥 Demo Video

🎬 **[Watch the Demo on YouTube](https://youtu.be/KkKMPIlNErU?si=fk1KI5NXdW6ItY4l)**

> A short walkthrough showcasing AIRLYTICS in action, from semantic search to AI-driven insights and hybrid analytics.

---



### 🖼️ Screenshots

#### 🏠 Landing Page

<img width="1918" height="1017" alt="Landing Page" src="https://github.com/user-attachments/assets/88011a2a-6206-4812-9766-9c168ef335a7" />

#### 🧭 Home Dashboard

<img width="1918" height="1020" alt="Home Dashboard" src="https://github.com/user-attachments/assets/dc572930-875e-4938-a6a7-432a575f995c" />

#### ℹ️ About Page

<img width="1918" height="1018" alt="About Page" src="https://github.com/user-attachments/assets/ea40e52b-b250-4a23-afe6-f1f4ddf753cf" />

---

#### 🔍 Semantic Search Interface

<img width="1918" height="1020" alt="Semantic Search" src="https://github.com/user-attachments/assets/a78eccaf-aafc-4ef4-9967-04b2c58618a7" />






---

#### 📊 Base Statistics Summary

<div align="center">

<table>
  <tr>
    <td><img width="320" alt="Image1" src="https://github.com/user-attachments/assets/9ecb9bc3-66a3-4f89-bdd0-2e83333f69d7" /></td>
    <td><img width="320" alt="Image2" src="https://github.com/user-attachments/assets/36d8e08f-3800-4787-840b-8c4646648505" /></td>
    <td><img width="320" alt="Image3" src="https://github.com/user-attachments/assets/6e82c03d-4249-4d1f-acaa-145479ddacee" /></td>
  </tr>
  <tr>
    <td><img width="320" alt="Image4" src="https://github.com/user-attachments/assets/1ca6873c-f136-4e28-aa87-a92d42dd0c70" /></td>
    <td><img width="320" alt="Image5" src="https://github.com/user-attachments/assets/60780e15-ef60-45e3-9c0c-797ab3d3ed82" /></td>
    <td><img width="320" alt="Image6" src="https://github.com/user-attachments/assets/636e4a1c-bc9f-4972-b1e0-afa42cefdb17" /></td>
  </tr>
</table>

</div>

---



### 🧩 Advanced Analytics Functions

#### ① Conditional Rating | ② Rating-to-Rating | ③ Category-to-Category

| Conditional Rating                                                                                                                               | Rating-to-Rating                                                                                                                               | Category-to-Category                                                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| <img width="1918" height="910" alt="Conditional Rating" src="https://github.com/user-attachments/assets/13616085-abc5-4095-bfd8-86eeb132df82" /> | <img width="1918" height="910" alt="Rating-to-Rating" src="https://github.com/user-attachments/assets/61ceaae0-cc30-4cf7-98c5-18610d10a002" /> | <img width="1918" height="916" alt="Category-to-Category" src="https://github.com/user-attachments/assets/2fca5efb-c49d-4e77-8653-08f59849bac8" /> |

#### ④ Distribution Analysis | ⑤ Percentage Distribution | ⑥ Base Case Handling

| Distribution Analysis                                                                                                                               | Percentage Distribution                                                                                                                               | Base Case Handling                                                                                                                               |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| <img width="1918" height="912" alt="Distribution Analysis" src="https://github.com/user-attachments/assets/7240bbb1-5a7b-4821-9a35-673bb034d1d8" /> | <img width="1918" height="912" alt="Percentage Distribution" src="https://github.com/user-attachments/assets/f808f0d4-cb18-4bb8-9f2d-3ef0491a9665" /> | <img width="1918" height="907" alt="Base Case Handling" src="https://github.com/user-attachments/assets/4d710a03-2b90-46a4-9a6e-e8e102598a53" /> |

---

#### 💬 AI InsightInterpreter

<img width="1918" height="1018" alt="Insight Interpreter" src="https://github.com/user-attachments/assets/97d0fc0b-1848-413a-9fb1-a0fcabc89ddc" />

---

#### 🧾 Dataset Overview

<img width="1666" height="420" alt="Dataset Overview" src="https://github.com/user-attachments/assets/737cecec-a833-40b7-864c-1e233c693100" />

---


## 📊 Example Use Cases & SQL Queries

### Semantic Search Queries
- *"excellent legroom and comfortable seats"*
- *"best crew and inflight service"*
- *"very long delay and bad food"*
- *"lost baggage, delayed luggage, mishandled bag"*
- *"value for money, affordable tickets"*

---

### Analytical Queries

#### 1. General Percentage Distribution
**Natural Language:** *"Out of all reviews mentioning slow boarding, what percentage of passengers rated value for money above 4?"*

```sql
SELECT answer
FROM analytics_query_agent 
WHERE question = 'Out of all reviews mentioning slow boarding, what percentage of passengers rated value for money above 4?';
```

**Use Case:** Fast threshold KPI to gauge price fairness perception after service issues.

---

#### 2. Conditional Distribution Analysis
**Natural Language:** *"For passengers complaining about check-in delays, show seat type distribution among those who rated ground service above 3."*

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'For passengers complaining about check-in delays, show seat type distribution among those who rated ground service above 3.';
```

**Use Case:** Understand how service issues vary by cabin class when ground service remains decent.

---

#### 3. Conditional Category-to-Category Analysis
**Natural Language:** *"Among reviews mentioning crowded flights, how many Economy Class passengers were Solo Leisure travelers compared to Business travelers?"*

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Among reviews mentioning crowded flights, how many Economy Class passengers were Solo Leisure travelers compared to Business travelers?';
```

**Use Case:** Compare traveler demographics to target capacity management interventions.

---

#### 4. Conditional Rating-to-Rating Analysis
**Natural Language:** *"Users who complained about baggage claim delays — what percentage of those who rated ground service below 4 rated overall experience below 5?"*

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Users who complained about baggage claim delays — what percentage of those who rated ground service below 4 rated overall experience below 5?';
```

**Use Case:** Identify disconnects between strong individual touchpoints and weak overall satisfaction.

---

#### 5. Conditional Rating Analysis
**Natural Language:** *"Among users who mentioned poor inflight meals, what percent of those who rated food and beverages above 4 also recommended the airline?"*

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Among users who mentioned poor inflight meals, what percent of those who rated food and beverages above 4 also recommended the airline?';
```

**Use Case:** Check if high meal ratings align with recommendation intent despite negative meal mentions.



## 🤝 Contributing

We welcome contributions! Here's how to get involved:

1. **Fork this repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/YourFeature
   ```
3. **Commit your changes**
   ```bash
   git commit -m "Add YourFeature"
   ```
4. **Push the branch**
   ```bash
   git push origin feature/YourFeature
   ```

---

## 📜 License

Licensed under the **MIT License**.  
See [LICENSE](https://github.com/rajesh-adk-137/AIRLYTICS/blob/main/LICENSE) for details.

---

## 🧠 Acknowledgments

- **AI & Vector Search:** [MindsDB](https://mindsdb.com)
- **Backend Framework:** [FastAPI](https://fastapi.tiangolo.com/)
- **Frontend:** [React](https://react.dev/) + [TailwindCSS](https://tailwindcss.com/)
- **Dataset:** Real-world airline reviews (~20,000 entries)
- **Icons:** [Lucide React](https://lucide.dev/)
- **Event:** [MindsDB Hacktoberfest 2025](https://mindsdb.com/hacktoberfest-2025)



<p align="center">
  <strong>Built with ❤️ for MindsDB Hacktoberfest 2025</strong>
</p>

<p align="center">
  <em>Transforming unstructured feedback into strategic intelligence, one query at a time.</em>
</p>