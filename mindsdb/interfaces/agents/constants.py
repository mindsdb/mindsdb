from langchain.agents import AgentType
from langchain_openai import OpenAIEmbeddings

from types import MappingProxyType
from mindsdb.integrations.handlers.openai_handler.constants import (
    CHAT_MODELS as OPEN_AI_CHAT_MODELS,
)

SUPPORTED_PROVIDERS = {
    "openai",
    "anthropic",
    "anyscale",
    "litellm",
    "ollama",
    "nvidia_nim",
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

# Define a read-only dictionary mapping providers to their models
PROVIDER_TO_MODELS = MappingProxyType(
    {
        "anthropic": ANTHROPIC_CHAT_MODELS,
        "ollama": OLLAMA_CHAT_MODELS,
        "openai": OPEN_AI_CHAT_MODELS,
        "nvidia_nim": NVIDIA_NIM_CHAT_MODELS,
    }
)

ASSISTANT_COLUMN = "answer"
CONTEXT_COLUMN = "context"
DEFAULT_AGENT_TIMEOUT_SECONDS = 300
# These should require no additional arguments.
DEFAULT_AGENT_TOOLS = []
DEFAULT_AGENT_TYPE = AgentType.CONVERSATIONAL_REACT_DESCRIPTION
DEFAULT_MAX_ITERATIONS = 10
DEFAULT_MAX_TOKENS = 8096
DEFAULT_MODEL_NAME = "gpt-4o"
USER_COLUMN = "question"
DEFAULT_EMBEDDINGS_MODEL_PROVIDER = "openai"
DEFAULT_EMBEDDINGS_MODEL_CLASS = OpenAIEmbeddings
