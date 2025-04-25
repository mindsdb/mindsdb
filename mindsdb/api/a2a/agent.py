import json
import random
from typing import Any, AsyncIterable, Dict, Optional
from dotenv import load_dotenv
#from flat_ai import FlatAI
import os

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

    for i in range(5):
      subtype = ["plan_task", "read_schema", "compose_query", "execute_query", "evaluate_results"]
      yield {
          "is_task_complete": False,
          "parts": [{
            "type": 'text',
            "text":"Processing the reimbursement request...",
          }],
          "metadata": {
            "type": "reasoning",
            "subtype": subtype[i],
          }
      }
    yield {
        "is_task_complete": True,
        "parts":[
          {
            "type": 'text',
            "text":"Processing the reimbursement request...",
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

  



