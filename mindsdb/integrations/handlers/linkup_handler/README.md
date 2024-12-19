
# Linkup Handler for MindsDB

The **Linkup Handler** integrates the Linkup API with MindsDB to provide contextual information retrieval capabilities. This handler allows you to perform searches using the Linkup API and retrieve structured results.

## Features
- **Search Capabilities**: Perform API calls to Linkup to retrieve contextual data.
- **Customizable Output**: Specify search depth and output types.

## Installation

Install the dependence using:

```bash
pip install linkup-sdk

```

## Usage

### Initialization
The handler is initialized with your Linkup API key:

```python
from linkup_handler.linkup_handler import LinkupSearchTool

# Replace 'your_api_key' your real API key that you can have from linkup.io.
linkup_tool = LinkupSearchTool(api_key="your_api_key")
```

### Perform a Search
Use the `run_search` method to execute a search query:

```python
results = linkup_tool.run_search(
    query="Women with nobel price",
    depth="standard",
    output_type="searchResults"
)

if results["success"]:
    print("Search Results:", results["results"])
else:
    print("Error:", results["error"])
```

### Parameters
- **query** *(str)*: The search query (e.g., `"Women with nobel price?"`).
- **depth** *(str, optional)*: The search depth (default: `"standard"`).
- **output_type** *(str, optional)*: The type of output desired (default: `"searchResults"`).

### Example Output
```json
{
    "success": true,
    "results": [
        {
            "name": "MindsDB Overview",
            "url": "https://example.com/mindsdb-overview",
            "content": "MindsDB is a machine learning platform..."
        },
        {
            "name": "Linkup Integration",
            "url": "https://example.com/linkup-integration",
            "content": "Integrate Linkup with MindsDB to enhance search..."
        }
    ]
}
```

## Test

To run test

```bash
    python -m pytest linkup_handler/tests/
```