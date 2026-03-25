from pydantic import BaseModel, Field


from . import prompts

sql_description = prompts.sql_description
sql_with_kb_description = prompts.sql_with_kb_description
markdown_instructions = prompts.markdown_instructions
planning_prompt = prompts.planning_prompt_base.format(
    response_instruction="2.1 If there is no need for exploratory steps, describe how would you write the final query or final text response to the user. and that you can solve this in one step."
)


class AgentResponse(BaseModel):
    sql_query: str = Field(..., description="The SQL query to run")
    text: str = Field(..., description="Text response to the user")
    short_description: str = Field(..., description="A short summary or description of the SQL query's purpose")
    type: str = Field(
        ...,
        description="Type of query: "
        "'exploratory_query' for queries used to explore or collect info to solve the question, if we need to interrogagte the database a bit more, "
        "'final_query' for the main sql query if we can solve the question we were asked by returning result of this query, "
        "'final_text' to return text response to user if we can answer without executing query",
    )
