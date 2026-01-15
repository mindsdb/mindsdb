OPENAI_API_BASE = "https://api.openai.com/v1"

# Updated Jan 2026: streamlined support for GPT-5, GPT-4, and O-series families
CHAT_MODELS_PREFIXES = (
    "gpt-5",    # Covers all GPT-5 variants
    "gpt-4",    # Covers GPT-4, GPT-4o, GPT-4o-mini
    "o1",       # Covers o1, o1-mini, o1-pro
    "o3",       # Covers o3, o3-mini
    "o4",       # Covers o4, o4-mini
    "gpt-3.5",  # Legacy
)
COMPLETION_MODELS = ("babbage-002", "davinci-002")
FINETUNING_MODELS = ("gpt-3.5-turbo", "babbage-002", "davinci-002", "gpt-4")
COMPLETION_LEGACY_BASE_MODELS = ("davinci", "curie", "babbage", "ada")
DEFAULT_CHAT_MODEL = "gpt-4o-mini"

FINETUNING_LEGACY_MODELS = FINETUNING_MODELS
COMPLETION_LEGACY_MODELS = (
    COMPLETION_LEGACY_BASE_MODELS
    + tuple(f"text-{model}-001" for model in COMPLETION_LEGACY_BASE_MODELS)
    + ("text-davinci-002", "text-davinci-003")
)

DEFAULT_EMBEDDING_MODEL = "text-embedding-ada-002"

IMAGE_MODELS = ("dall-e-2", "dall-e-3")
DEFAULT_IMAGE_MODEL = "dall-e-2"
