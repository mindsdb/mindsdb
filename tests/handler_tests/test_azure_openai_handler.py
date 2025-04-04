import os
from mindsdb.integrations.handlers.azure_openai_handler import AzureOpenAIHandler

# Example connection args – update these or set as env vars
connection_args = {
    'azure_openai_api_key': os.getenv('AZURE_OPENAI_API_KEY', 'your-key-here'),
    'api_base': os.getenv('AZURE_OPENAI_API_BASE', 'https://<your-endpoint>.openai.azure.com/'),
    'api_version': os.getenv('AZURE_OPENAI_API_VERSION', '2024-12-01-preview'),
    'model_name': os.getenv('AZURE_OPENAI_MODEL_NAME', 'text-embedding-3-large'),
}

# Instantiate the handler
handler = AzureOpenAIHandler(model_storage="asdfas", engine_storage="asdfadsdaf")

# Run the engine creation to test connection
try:
    print("Creating engine...")
    handler.create_engine(connection_args)
    print("✅ AzureOpenAIHandler connection test passed.")
except Exception as e:
    print("❌ AzureOpenAIHandler connection test failed.")
    print(f"Error: {e}")
