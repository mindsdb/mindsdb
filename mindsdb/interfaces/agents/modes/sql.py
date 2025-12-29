from pydantic import BaseModel, Field

from . import prompts
from .base import ResponseType

sql_description = prompts.sql_description
sql_with_kb_description = prompts.sql_with_kb_description
markdown_instructions = prompts.markdown_instructions

planning_prompt = prompts.planning_prompt_base.format(response_instruction="If there is no need for exploratory steps, describe how would you write the final query. and that you can solve this in one step.")


class AgentResponse(BaseModel):
    sql_query: str = Field(..., description="The SQL query to run")
    short_description: str = Field(..., description="A short summary or description of the SQL query's purpose")
    type: str = Field(ResponseType.FINAL_QUERY, description="Type of query: 'final_query' for the main query if we can solve the question we were asked, 'exploratory_query' for queries used to explore or collect info to solve the question, if we need to interrogagte the database a bit more")

