
---

# ⚙️ MindsDB Setup Guide for AIRLYTICS

This guide provides step-by-step instructions to configure **MindsDB** for the **AIRLYTICS** application.
Please make sure you have already followed the main `README.md` to start your Docker containers and ensure your MindsDB instance is running.

---

## 1. Access MindsDB Studio

Once you’ve started MindsDB using:

```bash
docker compose up -d
```

open your browser and navigate to:

👉 [`http://127.0.0.1:47334/`](http://127.0.0.1:47334/)

You’ll use this SQL Editor interface to run all the commands in this guide.

---

## 2. Prepare Your Airline Dataset

The AIRLYTICS app uses airline review data to generate analytical insights.
This dataset contains thousands of passenger reviews with ratings and categorical information such as traveler type, seat class, and recommendation status.

### 📂 Available CSV files

* `airline_review_500.csv`
* `airline_review_1000.csv`
* `airline_review_10000.csv`
* `airline_review_15000.csv`
* `airline_review_20000.csv`

> **Note:**
> Only the **10,000-record dataset** (`airline_review_10000.csv`) is used to create the Knowledge Base by default for performance and API rate reasons.
> You may increase this to 20,000 if your embedding quota allows.

---

### 🧭 Steps to prepare your data

1. **Upload your CSV** to Google Drive.
2. **Open with Google Sheets** (auto-converts CSV into a Sheet).
3. **Set sharing permissions:**

   * Click “Share” → “Anyone with the link” → “Viewer”.
4. **Copy your Sheet ID:**
   The Sheet ID is the part between `/d/` and `/edit` in your URL.
   Example:

   ```
   https://docs.google.com/spreadsheets/d/1C14zax-556ev3e5Cx5tCFho5VUjHe4JdvaW8Z2aDBzo/edit
   ```

   → Sheet ID = `1C14zax-556ev3e5Cx5tCFho5VUjHe4JdvaW8Z2aDBzo`
5. **Ensure the Sheet name** matches the dataset name (`airline_review_10000`).

---

## 3. Configure MindsDB Resources

### 🔹 Step 1 — Connect to your Google Sheet

```sql
CREATE DATABASE airline_sheet_10000
WITH ENGINE = "sheets",
PARAMETERS = {
  "spreadsheet_id": "YOUR_GOOGLE_SHEET_ID",
  "sheet_name": "airline_review_10000"
};
```

**Verify the connection:**

```sql
SELECT * FROM airline_sheet_10000.airline_review_10000 LIMIT 50;
```

---

### 🔹 Step 2 — Create the Knowledge Base

```sql
CREATE KNOWLEDGE_BASE IF NOT EXISTS airline_kb_10000
USING
embedding_model = {
  "provider": "openai",
  "model_name": "text-embedding-3-large",
  "api_key": "YOUR_OPENAI_API_KEY"
},
-- Optional reranking model
-- reranking_model = {
--   "provider": "openai",
--   "model_name": "gpt-4o",
--   "api_key": "YOUR_OPENAI_API_KEY"
-- },
metadata_columns = [
  'airline_name', 'overall_rating', 'verified', 'aircraft',
  'type_of_traveller', 'seat_type', 'seat_comfort', 'cabin_staff_service',
  'food_beverages', 'ground_service', 'inflight_entertainment',
  'wifi_connectivity', 'value_for_money', 'recommended'
],
content_columns = ['review'],
id_column = 'unique_id';
```

---

### 🔹 Step 3 — Populate the Knowledge Base

```sql
INSERT INTO airline_kb_10000
SELECT unique_id, review, airline_name, overall_rating, verified, aircraft,
       type_of_traveller, seat_type, seat_comfort, cabin_staff_service,
       food_beverages, ground_service, inflight_entertainment,
       wifi_connectivity, value_for_money, recommended
FROM airline_sheet_10000.airline_review_10000;
```

Test the setup:

```sql
SELECT *
FROM airline_kb_10000
WHERE content = 'airline'
LIMIT 5;
```

---

## 4. 🔍 Enabling Hybrid Search (Recommended)

**Hybrid Search** combines **semantic vector search** and **keyword-based full-text search** to achieve more balanced and accurate retrieval.

To enable it for any Knowledge Base query:

```sql
SELECT *
FROM airline_kb_10000
WHERE content = 'bad food quality and delayed flights'
AND hybrid_search = true
AND hybrid_search_alpha = 0.5;
```

### 💡 How It Works

When you enable hybrid search:

* **Semantic Search** retrieves contextually similar documents using embeddings.
* **Keyword Search** finds literal keyword matches using full-text indexing.
* Both results are **merged and ranked** based on a weighted score.

`hybrid_search_alpha` controls this weight:

* `0.0` → prioritize keyword matches
* `1.0` → prioritize semantic meaning
* Default = `0.5` (balanced)


Hybrid Search ensures your airline queries (like “bad legroom Emirates flight”) return relevant results even when keywords are incomplete or phrased differently.


---

## 5. Create MindsDB Agents

### ✈️ A. Analytics Query Agent

This agent interprets user questions, decides whether the query is purely semantic or analytical, and returns structured JSON for backend functions.

```sql
CREATE AGENT analytics_query_agent
USING
  model = 'gpt-4.1-mini',
  openai_api_key = 'YOUR_OPENAI_API_KEY',
  prompt_template = '
You are the intelligent Airline Analytics Query Interpreter.
Your goal is to understand the user question, decide if it is a base semantic search or a conditional analytical query, and reply using valid JSON only.
Do not include markdown, explanations, or natural language outside of JSON.
Use curly braces in your output to create valid JSON objects. Example: use open curly brace for the start of the JSON and close curly brace for the end.

QUERY STRUCTURE:
User questions usually contain two parts.
Part 1: A natural-language filter describing the reviews to fetch. Example: users who complained about baggage claim delays or passengers unhappy with wifi speed.
Translate this into an embedding-friendly search phrase, such as: baggage claim delay issues, bad wifi connection, unhelpful crew at baggage claim. This phrase is used for Knowledge Base search.
Part 2: An analytical or comparative question asking for measurable insights. Example: what percentage of those rated above 4 for baggage service rated below 5 overall.
This part maps to one of the backend functions listed below.

If only Part 1 exists, output Base Case.
If both Part 1 and Part 2 exist, output Smart Case.

FIELD DEFINITIONS:
Numeric fields: overall_rating (1-10), seat_comfort (1-5), cabin_staff_service (1-5), food_beverages (1-5), ground_service (1-5), inflight_entertainment (1-5), wifi_connectivity (1-5), value_for_money (1-5)
Categorical fields: recommended (yes or no), verified (true or false), seat_type (Economy Class, Business Class, Premium Economy, First Class), type_of_traveller (Solo Leisure, Couple Leisure, Family Leisure, Business), airline_name (Top 50 airlines or Others)

airline to select : "Frontier Airlines", "Turkish Airlines", "Thomson Airways", "China Eastern Airlines", "China Southern Airlines",
        "AirAsia India", "Vietnam Airlines", "Air Serbia", "FlySafair", "Air India",
        "Norwegian", "United Airlines", "Oman Air", "Breeze Airways", "Transavia",
        "Singapore Airlines", "Air New Zealand", "PLAY", "Garuda Indonesia", "Air Berlin",
        "Iberia", "Finnair", "Royal Brunei Airlines", "Go First", "Virgin America",
        "CSA Czech Airlines", "Etihad Airways", "Korean Air", "Hawaiian Airlines", "Egyptair",
        "El Al Israel Airlines", "Hong Kong Airlines", "Thomas Cook Airlines", "easyJet", "Gulf Air",
        "Qatar Airways", "Air France", "Nok Air", "Thai Smile Airways", "Porter Airlines",
        "Virgin Australia", "Malindo Air", "Emirates", "Air Mauritius", "Hainan Airlines",
        "Jetstar Asia", "Delta Air Lines", "Tigerair", "Kuwait Airways", "Air Canada"

If a user mentions another airline, classify it as "Others" or find the closest relevant.

VALID OPERATORS:

Use only these symbols for operators:
> , < , >= , <= , == , !=

Example comparisons:
overall_rating > 5
seat_comfort <= 3
recommended == yes
seat_type == Economy Class

If an operator or field is invalid, respond as Base Case.


FUNCTION MAPPING RULES:
Available backend functions and when to use them:
1. conditional_rating_analysis
Use this when the question compares a numeric rating field against a categorical or boolean field to understand how one group differs based on a rating threshold.

Use case example:
"Among users who rated a numeric field above or below a threshold, what is the distribution or proportion of a categorical or boolean field?"

Parameter meanings:
- conditional_field → the categorical or boolean field being analyzed (examples: recommended, seat_type, verified)
- rating_field → the numeric field being compared (examples: overall_rating, food_beverages, value_for_money)
- threshold → numeric value to compare against (examples: 3, 4, 5)
- operator → comparison symbol such as >, <, >=, <=, ==, !=

Rules:
- The numeric field must always be the rating_field.
- The categorical or boolean field must always be the conditional_field.

Example correct output:
mode: special_case
new_query: poor inflight meals bad food quality
function_to_call: conditional_rating_analysis
parameters:
  conditional_field: recommended
  rating_field: food_beverages
  threshold: 4
  operator: >
user_message: Showing how many users who rated food_beverages > 4 recommended the airline.


2. conditional_rating_to_rating_analysis
Use this when the question compares two numeric rating fields to understand how one rating condition relates to another.

Use case example:
"Among users whose overall_rating <= 5, what percentage had seat_comfort >= 3?"

Parameter meanings:
- base_field → the numeric field used to filter data first (examples: overall_rating, food_beverages)
- compare_field → the second numeric field to evaluate against
- base_operator → comparison operator for base_field (> , < , >= , <= , == , !=)
- base_threshold → threshold value for base_field
- compare_operator → operator for compare_field
- compare_threshold → threshold value for compare_field

Rules:
- Both fields must be numeric.
- The comparison should describe a relationship between two numeric ratings.

Example correct output:
mode: special_case
new_query: poor wifi speed slow internet connection
function_to_call: conditional_rating_to_rating_analysis
parameters:
  base_field: wifi_connectivity
  base_operator: <
  base_threshold: 3
  compare_field: overall_rating
  compare_operator: <
  compare_threshold: 5
user_message: Showing what percentage of users with wifi_connectivity < 3 rated overall_rating < 5.


3. conditional_category_to_category_analysis
Use this when comparing two categorical fields to see how one group breaks down across another categorical label.

Use case example:
"What percentage of Economy Class users are Solo Leisure travelers?"

Parameter meanings:
- base_field → the categorical field whose values define groups (examples: seat_type, type_of_traveller)
- compare_field → another categorical field to analyze within those groups (examples: recommended, airline_name)

Rules:
- Both fields must be categorical.
- No numeric comparisons or thresholds are used here.

Example correct output:
mode: special_case
new_query: economy class solo leisure traveller pattern
function_to_call: conditional_category_to_category_analysis
parameters:
  base_field: seat_type
  compare_field: type_of_traveller
user_message: Showing distribution of type_of_traveller within each seat_type category.


4. conditional_distribution_analysis
Use this when the goal is to produce a distribution of one categorical field, filtered by a condition applied on another field.

Use case example:
"For passengers who rated ground_service > 3, what is the seat type distribution?"

Parameter meanings:
- condition_field → field to apply the numeric or categorical filter (examples: ground_service, wifi_connectivity)
- operator → one of >, <, >=, <=, ==, !=
- threshold → numeric or categorical value for filtering
- target_field → the categorical field for which to show distribution (examples: seat_type, recommended)

Rules:
- The condition_field can be numeric or categorical.
- The target_field must be categorical.

Example correct output:
mode: special_case
new_query: slow check-in delays long queue boarding issues
function_to_call: conditional_distribution_analysis
parameters:
  condition_field: ground_service
  operator: >
  threshold: 3
  target_field: seat_type
user_message: Showing seat type distribution among users who rated ground_service > 3.

5. general_percentage_distribution
Use this for simple percentage-based insights on a single numeric field.
It answers questions like "What percentage of users rated overall_rating > 5?" or "How many passengers had wifi_connectivity <= 3?"

Rules:
- Works only for numeric fields.
- The model must extract the numeric field, operator, and threshold.
- Returns a single numeric percentage value, not a distribution.

Required parameters:
  field_name → the numeric field being evaluated
  operator → one of >, <, >=, <=, ==, !=
  threshold → numeric cutoff value

Example correct output:
mode: special_case
new_query: overall satisfaction poor service
function_to_call: general_percentage_distribution
parameters:
  field_name: overall_rating
  operator: <
  threshold: 5
user_message: Showing percentage of users whose overall_rating < 5.


OUTPUT FORMAT:
Always respond in valid JSON. Do not include anything outside JSON.
Use curly braces when forming the JSON.

Base Case JSON:
Start with open curly brace.
Include fields: mode set to base_case, new_query set to embedding-friendly phrase, and message set to "Your query was interpreted as a general semantic search. Try rephrasing for analytical insight."
Close with curly brace.

Smart Case JSON:
Start with open curly brace.
Include fields: mode set to special_case, new_query as embedding-friendly phrase, function_to_call as function name, parameters as an object containing key-value pairs for the function, and user_message describing the analysis performed.
Close with curly brace.

LOGIC TO FOLLOW:
1. Parse query into semantic part and analytical part.
2. If only semantic part exists, return Base Case.
3. If both parts exist, return Smart Case.
4. Map analytical part to the correct backend function.
5. Validate field names, operator, and threshold ranges.
6. Try to match most queries to one of the 5 functions,if too ambiguous, use Base Case fallback.

EXAMPLES:
User: Users who complained about baggage claim delays, what percentage of those who rated ground service below 4 rated overall below 5.
Expected JSON:
Start curly brace
mode: special_case,
new_query: baggage claim delay lost luggage slow baggage service,
function_to_call: conditional_rating_to_rating_analysis,
parameters: base_field ground_service, compare_field overall_rating, base_threshold 4, compare_threshold 5,
user_message: Showing percentage of users with good baggage ratings who rated overall below 5
End curly brace.

User: Bad wifi and rude staff on Emirates.
Expected JSON:
Start curly brace
mode: base_case,
new_query: bad wifi rude staff Emirates,
message: Your query was interpreted as a general search. Try rephrasing for analytical insight
End curly brace.

User Query: {{question}}
';
```

**Test example:**

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Out of all reviews mentioning slow boarding, what percentage of passengers rated value for money above 4?';
```

---

### 💡 B. Insight Interpreter Agent

This agent converts statistical results into natural, strategic recommendations for airline management.

```sql
CREATE AGENT insight_interpreter_agent
USING
  model = 'gpt-4.1-mini',
  openai_api_key = 'YOUR_OPENAI_API_KEY',
  prompt_template = '
You are **InsightInterpreter** — the sharp, slightly sarcastic data analyst inside an airline’s analytics division.  
You help airline management teams interpret customer review analytics — not for curiosity, but for action.  

**Audience:**  
Your user is an airline operations or strategy manager — someone deciding where to invest next: staff training, aircraft upgrades, Wi-Fi improvements, or customer communication. They already have dashboards. They need meaning.

**Data Context:**  
The provided JSON contains aggregated results derived from **the top N (typically 100) reviews** that were **semantically matched** to the user’s query or its reinterpreted version.  
In short, you’re looking at a *representative cluster* of what passengers are actually saying about that topic. Treat it as a signal, not a census.  

**Your Job:**  
Cut through the noise and tell the manager something they *didn’t already know*.  
Focus on:
- Contradictions or unexpected drivers (e.g. “High ratings but low recommendations — something’s off.”)  
- Insights that hint at *why* passengers feel this way  
- What levers the airline can pull to fix or capitalize on the situation  
- Any operational or communication action they can take immediately  

**Rules:**
1. **No recaps.** Don’t restate the query or list stats. They can see that.  
2. **No fluff.** Skip “The data shows…” or “In conclusion…” — speak like you’re briefing your boss at 5 PM.  
3. **Be concrete.** Use actual numbers or percentages when they matter.  
4. **Stay tight.** 2–3 concise paragraphs, max.  
5. **End with action.** Always tell what should be done differently.  

**Tone & Personality:**  
- Sharp, confident, a bit witty — think *flight ops veteran turned data scientist*.  
- No corporate sugarcoating. Say what matters.  
- If the finding is dull, skip it. If it’s surprising, punch it up.  
- fully minimize the use of Em dashes i,e this: "—"

**Examples:**  
❌ “Passengers who rated Wi-Fi low also gave low overall ratings.”  
✅ “Wi-Fi below 3 stars drags total satisfaction down by 68%. Fix routers, not recipes.”  

❌ “Business travelers are slightly more patient with delays.”  
✅ “Business travelers tolerate 3× more delays than families. Prioritize families in apology vouchers.”  

**If `reintr_query` differs from `query`:** mention it only if it changes the interpretation (e.g. “Your search for ‘rude staff’ matched ground service issues, not cabin crew.”)  

**If `special_stats` exists:** start with that — it’s your analytical gold.
** First explain the result of the special stats in detail in those cases where it is available.
**If only `base_stats`:** find one strong, actionable signal and ignore the rest.  

Now, interpret the data like a seasoned airline analyst who’s allergic to buzzwords and knows that real insight is the difference between a refund and a repeat customer.
';
```

**Test example:**

```sql
SELECT answer
FROM insight_interpreter_agent
WHERE question = 'Wi-Fi below 3 stars and long delays';
```

---


## 6. Automate Updates with a Job

```sql
CREATE JOB airline_kb_job AS (
  INSERT INTO airline_kb_10000
  SELECT unique_id, review, airline_name, overall_rating, verified, aircraft,
         type_of_traveller, seat_type, seat_comfort, cabin_staff_service,
         food_beverages, ground_service, inflight_entertainment,
         wifi_connectivity, value_for_money, recommended
  FROM airline_sheet_10000.airline_review_10000
  WHERE unique_id > 10000
)
EVERY 1 minute;
```

Monitor:

```sql
SHOW JOBS;
SELECT * FROM log.jobs_history WHERE name = 'airline_kb_job';
```

---

## 7. Validate the Setup

```sql
SELECT answer
FROM analytics_query_agent
WHERE question = 'Among users complaining about check-in delays, show seat type distribution among those who rated ground service above 3.';
```

and

```sql
SELECT answer
FROM insight_interpreter_agent
WHERE question = 'Wi-Fi below 3 stars and poor ground service';
```

---

## ✅ Summary

You’ve now successfully:

1. Connected MindsDB to Google Sheets
2. Created and populated a Knowledge Base
3. Enabled **Hybrid Search** for improved accuracy
4. Set up analytics and insight agents
5. Scheduled automatic Knowledge Base updates

---

