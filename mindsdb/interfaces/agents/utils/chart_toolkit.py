"""Chart.js configuration toolkit for Pydantic AI agents"""

from typing import Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class ChartConfig(BaseModel):
    """Pydantic model for Chart.js configuration and data query"""
    
    model_config = ConfigDict(extra="allow", strict=False)
    
    chartjs_config: Dict[str, Any] = Field(
        ...,
        description="Chart.js configuration dictionary. Must include: 'type' (one of 'line', 'bar', 'pie', 'doughnut'), 'options' (object with chart options), 'labels' (empty array []), 'datasets' (array of objects, each with 'label' and empty 'data' array []). Example: {'type': 'line', 'options': {'responsive': True, 'plugins': {'title': {'display': True, 'text': 'Chart Title'}}}, 'labels': [], 'datasets': [{'label': 'Dataset 1', 'data': []}]}"
    )
    
    data_query_string: str = Field(
        ...,
        description="SQL query string for data transformation. Must return: first column as labels (for x-axis or pie labels), subsequent columns as dataset values. Format: SELECT labels, <dataset_col1>, <dataset_col2>, ... FROM (<transformation query>). Example: SELECT date AS labels, sales FROM (SELECT date, SUM(amount) AS sales FROM table GROUP BY date)"
    )

