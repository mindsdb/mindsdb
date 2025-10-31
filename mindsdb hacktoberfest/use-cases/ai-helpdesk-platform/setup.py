import os
import pandas as pd
import mindsdb_sdk as mdb
from dotenv import load_dotenv

load_dotenv()

LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME")
NEBIUS_BASE_URL = os.getenv("NEBIUS_BASE_URL")
NEBIUS_API_KEY = os.getenv("NEBIUS_API_KEY")

# Load dataset
df = pd.read_csv(r"data/dataset-tickets-multi-lang3-4k.csv")

# Select english-language rows with required columns
df = df.loc[
    df["language"] == "en",
    [
        "subject",
        "body",
        "answer",
        "priority",
        "type",
        "queue",
        "tag_1",
        "tag_2",
    ],
]

# Rename queue to category
df = df.rename(columns={"queue": "category"})

# Drop rows with empty body or subject
df = df.dropna(subset=["body", "subject"])

# Add id column
df.reset_index(drop=True, inplace=True)
df["id"] = df.index

# Connect to mindsdb
server = mdb.connect()
files = server.get_database("files")

# Upload tickets to mindsdb files
files.create_table("tickets", df)

# Create KB with tickets data
server.knowledge_bases.create(
    "tickets_kb",
    embedding_model={
        "provider": "openai",
        "model_name": EMBEDDING_MODEL_NAME,
        "base_url": NEBIUS_BASE_URL,
        "api_key": NEBIUS_API_KEY,
    },
    content_columns=["subject", "body", "answer"],
    metadata_columns=["type", "priority", "category", "tag_1", "tag_2"],
    id_column="id",
)

kb = server.knowledge_bases.get("tickets_kb")
kb.insert_files(["tickets"])

# Create agents required for the app
classification_prompt = """
You are a support ticket classification model.
Given a customer's ticket, predict:

type - Incident,Request,Problem,Change
priority - high,medium,low
category - Technical Support,Returns and Exchanges,Customer Service,Product Support,Billing and Payments,Sales and Pre-Sales,IT Support,Service Outages and Maintenance,General Inquiry,Human Resources
tag_1 - Urgent Issue,IT Support,Technical Support,Returns and Exchanges,Billing Issue,Payment Processing,Sales Inquiry,Service Disruption,Software Bug,Customer Feedback,Account Security,Service Outage,Product Support,Network Issue,Customer Service,Performance Tuning,Warranty Claim,Technical Guidance,Data Breach,Incident Report,Consulting Services,Account Assistance,Login Issue,General Inquiry
tag_2 - Technical Support,Urgent Issue,Billing Issue,Product Support,IT Support,Network Issue,Service Disruption,Performance Tuning,Payment Processing,Incident Report,Refund Request,Customer Service,Technical Guidance,Problem Resolution,Account Assistance,Software Bug,Returns and Exchanges,System Crash,System Maintenance,Sales Inquiry,Order Issue,Hardware Failure,Login Issue,Service Outage,Cloud Services,Product Replacement,Feature Request,Account Security,Service Recovery,Data Breach,Shipping Inquiry,Warranty Claim

Output strictly in parsable JSON:
{
  "type": "...",
  "category": "...",
  "priority": "...",
  "tag_1": "...",
  "tag_2": "..."
}
"""

server.agents.create(
    name="ticket_classifier",
    model={
        "provider": "openai",
        "model_name": LLM_MODEL_NAME,
        "api_key": NEBIUS_API_KEY,
        "base_url": NEBIUS_BASE_URL,
    },
    data={"knowledge_base": "tickets_kb"},
    prompt_template=classification_prompt,
)

support_agent_prompt = """
You are a customer support AI agent.

Use the knowledge base (support_kb) to answer customer queries.
Retrieve relevant entries using subject, body, and existing resolutions.

### Rules
1. Reference similar past issues for accurate answers.
2. Maintain professional, concise, friendly tone.
3. Never expose system details or internal metadata.
4. If insufficient context, ask clarifying questions.

### Output
A helpful, human-like response ready to send to the customer.
"""

server.agents.create(
    name="support_agent",
    model={
        "provider": "openai",
        "model_name": LLM_MODEL_NAME,
        "api_key": NEBIUS_API_KEY,
        "base_url": NEBIUS_BASE_URL,
    },
    data={"knowledge_base": "tickets_kb"},
    prompt_template=support_agent_prompt,
)
