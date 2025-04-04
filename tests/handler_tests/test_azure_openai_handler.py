import pandas as pd
import os
from mindsdb.integrations.handlers.azure_openai_handler import AzureOpenAIHandler

# Set up your credentials
api_key = os.getenv("AZURE_OPENAI_API_KEY", "your-api-key-here")
api_base = os.getenv("AZURE_OPENAI_API_BASE", "https://<your-endpoint>.openai.azure.com/")
api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
model_name = os.getenv("AZURE_OPENAI_MODEL_NAME", "text-embedding-3-large")


# Minimal handler state emulation
class DummyStorage:
    def __init__(self, args):
        self._args = args

    def get_connection_args(self):
        return self._args

    def json_get(self, key):
        return self._args

    def json_set(self, key, val):
        self._args = val


# Simulate model storage args
args = {
    "mode": "embedding",
    "question_column": "text",
    "model_name": model_name,
    "target": "embedding",
    "api_base": api_base,
    "api_version": api_version,
    "azure_openai_api_key": api_key
}

# Simulate dataframe
df = pd.DataFrame({
    "text": [
        "The quick brown fox jumps over the lazy dog.",
        "MindsDB makes it easy to build AI-powered applications with SQL.",
        "Azure OpenAI service provides access to powerful language models."
    ]
})

# Create handler and plug in dummy storage
handler = AzureOpenAIHandler(model_storage="asdfasdf", engine_storage="asdfasdf")
handler.model_storage = DummyStorage(args)
handler.engine_storage = DummyStorage(args)

# Run prediction
print("Running embedding prediction...")
try:
    result = handler.predict(df)
    print(result)
    print("✅ Embedding test passed.")
except Exception as e:
    print("❌ Embedding test failed.")
    print(f"Error: {e}")
