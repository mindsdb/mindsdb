OPENAI_API_BASE = 'https://api.openai.com/v1'

CHAT_MODELS = (
    'gpt-3.5-turbo',
    'gpt-3.5-turbo-16k',
    'gpt-3.5-turbo-instruct',
    'gpt-4',
    'gpt-4-32k',
    'gpt-4-1106-preview',
    'gpt-4-0125-preview',
    'gpt-4o'
)
COMPLETION_MODELS = ('babbage-002', 'davinci-002')
FINETUNING_MODELS = ('gpt-3.5-turbo', 'babbage-002', 'davinci-002', 'gpt-4')
COMPLETION_LEGACY_BASE_MODELS = ('davinci', 'curie', 'babbage', 'ada')
DEFAULT_CHAT_MODEL = 'gpt-3.5-turbo'

FINETUNING_LEGACY_MODELS = FINETUNING_MODELS
COMPLETION_LEGACY_MODELS = (
    COMPLETION_LEGACY_BASE_MODELS
    + tuple(f'text-{model}-001' for model in COMPLETION_LEGACY_BASE_MODELS)
    + ('text-davinci-002', 'text-davinci-003')
)

EMBEDDING_MODELS = (
    ('text-embedding-ada-002',)
    + tuple(f'text-similarity-{model}-001' for model in COMPLETION_LEGACY_BASE_MODELS)
    + tuple(f'text-search-{model}-query-001' for model in COMPLETION_LEGACY_BASE_MODELS)
    + tuple(f'text-search-{model}-doc-001' for model in COMPLETION_LEGACY_BASE_MODELS)
    + tuple(f'code-search-{model}-text-001' for model in COMPLETION_LEGACY_BASE_MODELS)
    + tuple(f'code-search-{model}-code-001' for model in COMPLETION_LEGACY_BASE_MODELS)
)
DEFAULT_EMBEDDING_MODEL = 'text-embedding-ada-002'

IMAGE_MODELS = ('dall-e-2', 'dall-e-3')
DEFAULT_IMAGE_MODEL = 'dall-e-2'
