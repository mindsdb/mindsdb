# Novita API Configuration
# OpenAI-compatible endpoint
NOVITA_API_BASE = "https://api.novita.ai/openai/v1"

# Default models
DEFAULT_CHAT_MODEL = "moonshotai/kimi-k2.5"
DEFAULT_EMBEDDING_MODEL = "qwen/qwen3-embedding-0.6b"

# Supported chat models (from CLAUDE.md Model Catalog)
CHAT_MODELS = [
    "moonshotai/kimi-k2.5",
    "zai-org/glm-5",
    "minimax/minimax-m2.5",
]

# Supported embedding models
EMBEDDING_MODELS = [
    "qwen/qwen3-embedding-0.6b",
]

# Chat model prefixes for detection
CHAT_MODELS_PREFIXES = ("moonshotai/", "zai-org/", "minimax/")

# All supported models
ALL_SUPPORTED_MODELS = CHAT_MODELS + EMBEDDING_MODELS
