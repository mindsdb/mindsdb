OPENAI_API_BASE = "https://api.openai.com/v1"

CHAT_MODELS_PREFIXES = ("gpt-3.5", "gpt-3.5", "gpt-3.5", "gpt-4", "o3-mini", "o1-mini")
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
