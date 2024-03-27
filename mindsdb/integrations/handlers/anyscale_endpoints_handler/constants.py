# Dictionary mapping short names to original names
CHAT_COMPLETION_MODELS_SIMPLIFIED_MAP = {
    "llama-2-7b": "meta-llama/Llama-2-7b-chat-hf",
    "llama-2-13b": "meta-llama/Llama-2-13b-chat-hf",
    "llama-guard-7b": "Meta-Llama/Llama-Guard-7b",
    "llama-2-70b": "meta-llama/Llama-2-70b-chat-hf",
    "mistral-7b-openorca": "Open-Orca/Mistral-7B-OpenOrca",
    "codellama-34b": "codellama/CodeLlama-34b-Instruct-hf",
    "codellama-70b": "codellama/CodeLlama-70b-Instruct-hf",
    "zephyr-7b": "HuggingFaceH4/zephyr-7b-beta",
    "mistral-7b": "mistralai/Mistral-7B-Instruct-v0.1",
    "mixtral-8x7b": "mistralai/Mixtral-8x7B-Instruct-v0.1",
    "neuralhermes": "mlabonne/NeuralHermes-2.5-Mistral-7B",
    "bge-large": "BAAI/bge-large-en-v1.5",
    "gte-large": "thenlper/gte-large"
}

# List of short names
CHAT_COMPLETION_MODELS_SIMPLIFIED = list(CHAT_COMPLETION_MODELS_SIMPLIFIED_MAP.keys())
