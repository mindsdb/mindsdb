import json
import random
from typing import Any, AsyncIterable, Dict, Optional
from dotenv import load_dotenv
#from flat_ai import FlatAI
import os
import constants

def get_api_key() -> str:
  """Helper method to handle API Key."""
  load_dotenv()
  return os.getenv("GOOGLE_API_KEY")

class MindsDBAgent:
  """An agent that handles reimbursement requests."""

  SUPPORTED_CONTENT_TYPES = ["text", "text/plain"]

  def __init__(self):
    #self.llm = FlatAI(api_key=get_api_key(),  model="gemini-2.0-flash", base_url="https://generativelanguage.googleapis.com/v1beta/openai/")
    pass

  def invoke(self, query, session_id) -> str:
    
    return {"content": "Use stream method to get the results!"}

  async def stream(self, query, session_id) -> AsyncIterable[Dict[str, Any]]:
    import asyncio
    
    subtypes = ["plan", "query", "curate", "validate", "respond"]
    for i in range(5):
      await asyncio.sleep(1) # Simulate processing time
      
      yield {
          "is_task_complete": False,
          "parts": [{
            "type": 'text',
            "text": constants.TEXT_BY_SUBTYPE[subtypes[i]],
          }],
          "metadata": {
            "type": "reasoning",
            "subtype": subtypes[i],
          }
      }
    
    # One final delay before the completion
    await asyncio.sleep(0.2)
    
    yield {
        "is_task_complete": True,
        "parts":[
          {
            "type": 'text',
            "text": constants.TEXT_BY_SUBTYPE[subtypes[4]],
          },
          {
            "type": 'data',
            "data": {'col1': [1, 2], 'col2': ['a', 'b']},
            "metadata": {
              "subtype": "json",
            }
          }
        ] 
    }
