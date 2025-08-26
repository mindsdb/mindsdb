import os

from langchain.agents import AgentType
from langchain_openai import OpenAIEmbeddings

from types import MappingProxyType

# the same as
# from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS
OPEN_AI_CHAT_MODELS = (
    "gpt-3.5-turbo",
    "gpt-3.5-turbo-16k",
    "gpt-3.5-turbo-instruct",
    "gpt-4",
    "gpt-4-32k",
    "gpt-4-1106-preview",
    "gpt-4-0125-preview",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4o",
    "o4-mini",
    "o3-mini",
    "o1-mini",
)

SUPPORTED_PROVIDERS = {
    "openai",
    "anthropic",
    "litellm",
    "ollama",
    "nvidia_nim",
    "vllm",
    "google",
    "writer",
}
# Chat models
ANTHROPIC_CHAT_MODELS = (
    "claude-3-opus-20240229",
    "claude-3-sonnet-20240229",
    "claude-3-haiku-20240307",
    "claude-2.1",
    "claude-2.0",
    "claude-instant-1.2",
)

OLLAMA_CHAT_MODELS = (
    "gemma",
    "llama2",
    "mistral",
    "mixtral",
    "llava",
    "neural-chat",
    "codellama",
    "dolphin-mixtral",
    "qwen",
    "llama2-uncensored",
    "mistral-openorca",
    "deepseek-coder",
    "nous-hermes2",
    "phi",
    "orca-mini",
    "dolphin-mistral",
    "wizard-vicuna-uncensored",
    "vicuna",
    "tinydolphin",
    "llama2-chinese",
    "openhermes",
    "zephyr",
    "nomic-embed-text",
    "tinyllama",
    "openchat",
    "wizardcoder",
    "phind-codellama",
    "starcoder",
    "yi",
    "orca2",
    "falcon",
    "starcoder2",
    "wizard-math",
    "dolphin-phi",
    "nous-hermes",
    "starling-lm",
    "stable-code",
    "medllama2",
    "bakllava",
    "codeup",
    "wizardlm-uncensored",
    "solar",
    "everythinglm",
    "sqlcoder",
    "nous-hermes2-mixtral",
    "stable-beluga",
    "yarn-mistral",
    "samantha-mistral",
    "stablelm2",
    "meditron",
    "stablelm-zephyr",
    "magicoder",
    "yarn-llama2",
    "wizard-vicuna",
    "llama-pro",
    "deepseek-llm",
    "codebooga",
    "mistrallite",
    "dolphincoder",
    "nexusraven",
    "open-orca-platypus2",
    "all-minilm",
    "goliath",
    "notux",
    "alfred",
    "megadolphin",
    "xwinlm",
    "wizardlm",
    "duckdb-nsql",
    "notus",
)

NVIDIA_NIM_CHAT_MODELS = (
    "microsoft/phi-3-mini-4k-instruct",
    "mistralai/mistral-7b-instruct-v0.2",
    "writer/palmyra-med-70b",
    "mistralai/mistral-large",
    "mistralai/codestral-22b-instruct-v0.1",
    "nvidia/llama3-chatqa-1.5-70b",
    "upstage/solar-10.7b-instruct",
    "google/gemma-2-9b-it",
    "adept/fuyu-8b",
    "google/gemma-2b",
    "databricks/dbrx-instruct",
    "meta/llama-3_1-8b-instruct",
    "microsoft/phi-3-medium-128k-instruct",
    "01-ai/yi-large",
    "nvidia/neva-22b",
    "meta/llama-3_1-70b-instruct",
    "google/codegemma-7b",
    "google/recurrentgemma-2b",
    "google/gemma-2-27b-it",
    "deepseek-ai/deepseek-coder-6.7b-instruct",
    "mediatek/breeze-7b-instruct",
    "microsoft/kosmos-2",
    "microsoft/phi-3-mini-128k-instruct",
    "nvidia/llama3-chatqa-1.5-8b",
    "writer/palmyra-med-70b-32k",
    "google/deplot",
    "meta/llama-3_1-405b-instruct",
    "aisingapore/sea-lion-7b-instruct",
    "liuhaotian/llava-v1.6-mistral-7b",
    "microsoft/phi-3-small-8k-instruct",
    "meta/codellama-70b",
    "liuhaotian/llava-v1.6-34b",
    "nv-mistralai/mistral-nemo-12b-instruct",
    "microsoft/phi-3-medium-4k-instruct",
    "seallms/seallm-7b-v2.5",
    "mistralai/mixtral-8x7b-instruct-v0.1",
    "mistralai/mistral-7b-instruct-v0.3",
    "google/paligemma",
    "google/gemma-7b",
    "mistralai/mixtral-8x22b-instruct-v0.1",
    "google/codegemma-1.1-7b",
    "nvidia/nemotron-4-340b-instruct",
    "meta/llama3-70b-instruct",
    "microsoft/phi-3-small-128k-instruct",
    "ibm/granite-8b-code-instruct",
    "meta/llama3-8b-instruct",
    "snowflake/arctic",
    "microsoft/phi-3-vision-128k-instruct",
    "meta/llama2-70b",
    "ibm/granite-34b-code-instruct",
)

GOOGLE_GEMINI_CHAT_MODELS = (
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.5-pro-preview-03-25",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
    "gemini-1.5-pro",
)

WRITER_CHAT_MODELS = ("palmyra-x5", "palmyra-x4")

# Define a read-only dictionary mapping providers to their models
PROVIDER_TO_MODELS = MappingProxyType(
    {
        "anthropic": ANTHROPIC_CHAT_MODELS,
        "ollama": OLLAMA_CHAT_MODELS,
        "openai": OPEN_AI_CHAT_MODELS,
        "nvidia_nim": NVIDIA_NIM_CHAT_MODELS,
        "google": GOOGLE_GEMINI_CHAT_MODELS,
        "writer": WRITER_CHAT_MODELS,
    }
)

ASSISTANT_COLUMN = "answer"
CONTEXT_COLUMN = "context"
TRACE_ID_COLUMN = "trace_id"
DEFAULT_AGENT_TIMEOUT_SECONDS = 300
# These should require no additional arguments.
DEFAULT_AGENT_TOOLS = []
DEFAULT_AGENT_TYPE = AgentType.CONVERSATIONAL_REACT_DESCRIPTION
DEFAULT_MAX_ITERATIONS = 10
DEFAULT_MAX_TOKENS = 8096
DEFAULT_MODEL_NAME = "gpt-4o"
DEFAULT_TEMPERATURE = 0.0
USER_COLUMN = "question"
DEFAULT_EMBEDDINGS_MODEL_PROVIDER = "openai"
DEFAULT_EMBEDDINGS_MODEL_CLASS = OpenAIEmbeddings
DEFAULT_TIKTOKEN_MODEL_NAME = os.getenv("DEFAULT_TIKTOKEN_MODEL_NAME", "gpt-4")
AGENT_CHUNK_POLLING_INTERVAL_SECONDS = os.getenv("AGENT_CHUNK_POLLING_INTERVAL_SECONDS", 1.0)
DEFAULT_TEXT2SQL_DATABASE = "mindsdb"
DEFAULT_AGENT_SYSTEM_PROMPT = """
You are an AI assistant powered by MindsDB. You have access to conversation history and should use it to provide contextual responses. When answering questions, follow these guidelines:

**CONVERSATION CONTEXT:**
- You have access to previous messages in this conversation through your memory system
- When users ask about previous questions, topics, or context, refer to the conversation history
- Maintain conversational continuity and reference earlier parts of the conversation when relevant
- When asked to retrieve or list past user questions, examine your conversation memory to identify and list previous user queries
- You can reference specific past questions by their content or by their position in the conversation (e.g., "your first question", "the question you asked earlier about...")

1. For factual questions about specific topics, use the knowledge base tools in this sequence:
   - First use kb_list_tool to see available knowledge bases
   - Then use kb_info_tool to understand the structure of relevant knowledge bases
   - Finally use kb_query_tool to query the knowledge base for specific information

2. For questions about database tables and their contents:
   - Use the sql_db_query to query the tables directly
   - You can join tables if needed to get comprehensive information
   - You are running on a federated query engine, so joins across multiple databases are allowed and supported
   - **Important Rule for SQL Queries:** If you formulate an SQL query as part of answering a user's question, you *must* then use the `sql_db_query` tool to execute that query and get its results. The SQL query string itself is NOT the final answer to the user unless the user has specifically asked for the query. Your final AI response should be based on the *results* obtained from executing the query.


For factual questions, ALWAYS use the available tools to look up information rather than relying on your internal knowledge.

"""

MINDSDB_PREFIX = """You are an AI assistant powered by MindsDB. You have access to conversation history and should use it to provide contextual responses. When answering questions, follow these guidelines:

**CONVERSATION CONTEXT:**
- You have access to previous messages in this conversation through your memory system
- When users ask about previous questions, topics, or context, refer to the conversation history
- Maintain conversational continuity and reference earlier parts of the conversation when relevant
- When asked to retrieve or list past user questions, examine your conversation memory to identify and list previous user queries
- You can reference specific past questions by their content or by their position in the conversation (e.g., "your first question", "the question you asked earlier about...")

1. For questions about database tables and their contents:
   - Use the sql_db_query to query the tables directly
   - You can join tables if needed to get comprehensive information
   - You are running on a federated query engine, so joins across multiple databases are allowed and supported
   - **Important Rule for SQL Queries:** If you formulate an SQL query as part of answering a user's question, you *must* then use the `sql_db_query` tool to execute that query and get its results. The SQL query string itself is NOT the final answer to the user unless the user has specifically asked for the query. Your final AI response should be based on the *results* obtained from executing the query.

2. For factual questions about specific topics, use the knowledge base tools, if available, in this sequence:
- First use kb_list_tool to see available knowledge bases
- Then use kb_info_tool to understand the structure of relevant knowledge bases
- Finally use kb_query_tool to query the knowledge base for specific information

For factual questions, ALWAYS use the available tools to look up information rather than relying on your internal knowledge.

Here is the user's question: {{question}}   

TOOLS:
------

Assistant has access to the following tools:"""

EXPLICIT_FORMAT_INSTRUCTIONS = """
<< TOOL CALLING INSTRUCTIONS >>

**It is critical you use the following format to call a tool**

```
Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
```

When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:

```
Thought: Do I need to use a tool? No
{ai_prefix}: [your response here]
```
"""
