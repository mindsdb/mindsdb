"""Chart generation agent using Pydantic AI"""

from typing import Optional
import copy

from pydantic_ai import Agent

from mindsdb.utilities import log
from mindsdb.interfaces.agents.utils.chart_toolkit import ChartConfig
from mindsdb.interfaces.agents.utils.pydantic_ai_model_factory import (
    get_model_instance_from_kwargs
)
from mindsdb.interfaces.agents.prompts import agent_prompts

logger = log.getLogger(__name__)


class ChartAgent:
    """Lightweight agent for generating Chart.js configurations"""
    
    def __init__(self):
        """
        Initialize Chart Agent using system default LLM configuration.
        """
        self.args = self._initialize_args()
        
        # Provider model instance
        self.model_instance = get_model_instance_from_kwargs(self.args)
        
        # System prompt for chart generation
        self.system_prompt = self.args.get(
            "prompt_template",
            "You are an expert at generating Chart.js configurations from SQL queries"
        )
    
    def _initialize_args(self) -> dict:
        """
        Initialize the arguments for agent execution using system default LLM config.
        
        Returns:
            dict: Final parameters for agent execution
        """
        from mindsdb.utilities.config import config
        
        # Get default LLM config from system config
        args = copy.deepcopy(config.get("default_llm", {}))
        
        # Remove use_default_llm flag if present
        args.pop("use_default_llm", None)
        
        if "model_name" not in args:
            raise ValueError(
                "No model name provided for chart agent. Please configure default_llm in system settings."
            )
        
        return args
    
    def generate_chart_config(self, query: str, prompt: Optional[str] = None) -> ChartConfig:
        """
        Generate Chart.js configuration and data query from SQL query.
        
        Args:
            query: SQL query string
            prompt: Optional prompt describing chart intent
            
        Returns:
            ChartConfig: Pydantic model with chartjs_config and data_query_string
        """
        # Create agent with ChartConfig as output type
        agent = Agent(
            self.model_instance,
            system_prompt=self.system_prompt,
            output_type=ChartConfig
        )
        
        # Build prompt for chart generation
        chart_prompt = f"""Given the following SQL query and optional intent, generate a Chart.js configuration and data transformation query.

SQL Query:
{query}
"""
        
        if prompt:
            chart_prompt += f"\nChart Intent: {prompt}\n"
        
        chart_prompt += f"\n{agent_prompts.chart_generation_prompt}"
        
        logger.debug(f"ChartAgent.generate_chart_config: Sending prompt to LLM")
        
        try:
            # Generate chart config
            result = agent.run_sync(chart_prompt)
            chart_config = result.output
            
            logger.debug(f"ChartAgent.generate_chart_config: Received chart config with type: {chart_config.chartjs_config.get('type', 'unknown')}")
            
            return chart_config
        except Exception as e:
            logger.error(f"ChartAgent.generate_chart_config: Error generating chart config: {e}")
            # Log the raw result if available for debugging
            if hasattr(e, 'result') and hasattr(e.result, 'output'):
                logger.debug(f"ChartAgent.generate_chart_config: Raw output: {e.result.output}")
            raise

