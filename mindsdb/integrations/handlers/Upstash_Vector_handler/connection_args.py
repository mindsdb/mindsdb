# Connection arguments required for Upstash Vector integration
connection_args = {
    "host": {
        "type": "string",
        "description": "The base URL of the Upstash Vector instance, typically in the format 'https://<your-instance>.upstash.io'."
    },
    "api_key": {
        "type": "string",
        "description": "API key provided by Upstash for secure authentication to the instance."
    }
}

# Example values for connection arguments
connection_args_example = {
    "host": "https://example.upstash.io",
    "api_key": "your_api_key",
}
