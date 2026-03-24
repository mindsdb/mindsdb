from pydantic import BaseModel, Field


class PlanResponse(BaseModel):
    plan: str = Field(
        ..., description="A step-by-step plan for solving the question, identifying data sources and steps needed"
    )
    estimated_steps: int = Field(..., description="Estimated number of steps needed to solve the question")


class ResponseType:
    FINAL_QUERY = "final_query"  # this is the final query
    EXPLORATORY = "exploratory_query"  # this is a query to explore and collect info to solve the challenge (e.g., distinct values of a categorical column, schema inference, etc.)
    FINAL_TEXT = "final_text"  # this is the final query
