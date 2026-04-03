LEMONADE_API_BASE = "http://localhost:8000/api/v1"

# Lemonade supports various models, but we'll use common prefixes for chat models
CHAT_MODELS_PREFIXES = ("llama", "mistral", "gemma", "qwen", "phi", "codellama", "vicuna", "alpaca", "wizard")
COMPLETION_MODELS = ("llama", "mistral", "gemma", "qwen", "phi", "codellama", "vicuna", "alpaca", "wizard")
FINETUNING_MODELS = ()  # Lemonade doesn't support fine-tuning in the same way as OpenAI
COMPLETION_LEGACY_BASE_MODELS = ("llama", "mistral", "gemma", "qwen", "phi", "codellama", "vicuna", "alpaca", "wizard")
DEFAULT_CHAT_MODEL = "Llama-3.2-1B-Instruct-Hybrid"

FINETUNING_LEGACY_MODELS = FINETUNING_MODELS
COMPLETION_LEGACY_MODELS = COMPLETION_LEGACY_BASE_MODELS

# Lemonade doesn't support embeddings or image generation in the same way
DEFAULT_EMBEDDING_MODEL = "text-embedding-ada-002"  # Not supported by Lemonade
IMAGE_MODELS = ()  # Not supported by Lemonade
DEFAULT_IMAGE_MODEL = "dall-e-2"  # Not supported by Lemonade
