from langchain.agents import AgentType

SUPPORTED_PROVIDERS = {'openai', 'anthropic', 'anyscale', 'litellm', 'ollama'}
# Chat models
ANTHROPIC_CHAT_MODELS = {
    'claude-3-opus-20240229',
    'claude-3-sonnet-20240229',
    'claude-3-haiku-20240307',
    'claude-2.1',
    'claude-2.0',
    'claude-instant-1.2'
}
# See https://ollama.com/library.
OLLAMA_CHAT_MODELS = {
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
    "notus"
}
DEFAULT_ASSISTANT_COLUMN = 'answer'
DEFAULT_AGENT_TIMEOUT_SECONDS = 300
# These should require no additional arguments.
DEFAULT_AGENT_TOOLS = []  
DEFAULT_AGENT_TYPE = AgentType.CONVERSATIONAL_REACT_DESCRIPTION
DEFAULT_MAX_ITERATIONS = 10
DEFAULT_MAX_TOKENS = 2048
DEFAULT_MODEL_NAME = 'gpt-4-0125-preview'
DEFAULT_USER_COLUMN = 'question'
DEFAULT_EMBEDDINGS_MODEL_PROVIDER = 'openai'