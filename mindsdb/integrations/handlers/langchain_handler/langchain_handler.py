from concurrent.futures import as_completed, TimeoutError
from typing import Optional, Dict
import re

from langchain.agents import AgentExecutor
from langchain.agents.initialize import initialize_agent
from langchain.chains.conversation.memory import ConversationSummaryBufferMemory
from langchain_core.prompts import PromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage

import numpy as np
import pandas as pd

from mindsdb.interfaces.agents.safe_output_parser import SafeOutputParser
from mindsdb.interfaces.agents.langchain_agent import (
    get_llm_provider,
    get_embedding_model_provider,
    create_chat_model,
    get_chat_model_params,
)

from mindsdb.interfaces.agents.constants import (
    DEFAULT_AGENT_TIMEOUT_SECONDS,
    DEFAULT_AGENT_TOOLS,
    DEFAULT_AGENT_TYPE,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL_NAME,
    USER_COLUMN,
    ASSISTANT_COLUMN,
)
from mindsdb.integrations.utilities.rag.settings import DEFAULT_RAG_PROMPT_TEMPLATE
from mindsdb.integrations.handlers.langchain_handler.tools import setup_tools
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.interfaces.storage.model_fs import HandlerStorage, ModelStorage
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import (
    construct_model_from_args,
)
from mindsdb.integrations.handlers.openai_handler.constants import CHAT_MODELS_PREFIXES  # noqa: F401 - for dependency checker

from mindsdb.utilities import log
from mindsdb.utilities.context_executor import ContextThreadPoolExecutor

_PARSING_ERROR_PREFIXES = ["An output parsing error occured", "Could not parse LLM output"]

logger = log.getLogger(__name__)


class LangChainHandler(BaseMLEngine):
    """
    This is a MindsDB integration for the LangChain library, which provides a unified interface for interacting with
    various large language models (LLMs).

    Supported LLM providers:
        - OpenAI
        - Anthropic
        - Google
        - LiteLLM
        - Ollama

    Supported standard tools:
        - python_repl
        - serper.dev search
    """

    name = "langchain"

    def __init__(self, model_storage: ModelStorage, engine_storage: HandlerStorage, **kwargs):
        super().__init__(model_storage, engine_storage, **kwargs)
        # if True, the target column name does not have to be specified at creation time.
        self.generative = True
        self.default_agent_tools = DEFAULT_AGENT_TOOLS

    def _create_embeddings_model(self, args: Dict):
        return construct_model_from_args(args)

    def _handle_parsing_errors(self, error: Exception) -> str:
        response = str(error)
        for p in _PARSING_ERROR_PREFIXES:
            if response.startswith(p):
                # As a somewhat dirty workaround, we accept the output formatted incorrectly and use it as a response.
                #
                # Ideally, in the future, we would write a parser that is more robust and flexible than the one Langchain uses.
                # Response is wrapped in ``
                logger.info("Handling parsing error, salvaging response...")
                response_output = response.split("`")
                if len(response_output) >= 2:
                    response = response_output[-2]

                # Wrap response in Langchain conversational react format.
                langchain_react_formatted_response = f"""Thought: Do I need to use a tool? No
AI: {response}"""
                return langchain_react_formatted_response
        return f"Agent failed with error:\n{str(error)}..."

    def create(self, target: str, args: Dict = None, **kwargs):
        self.default_agent_tools = args.get("tools", self.default_agent_tools)

        args = args["using"]
        args["target"] = target
        args["model_name"] = args.get("model_name", DEFAULT_MODEL_NAME)
        args["provider"] = args.get("provider", get_llm_provider(args))
        args["embedding_model_provider"] = args.get("embedding_model", get_embedding_model_provider(args))
        if args.get("mode") == "retrieval":
            # use default prompt template for retrieval i.e. RAG if not provided
            if "prompt_template" not in args:
                args["prompt_template"] = DEFAULT_RAG_PROMPT_TEMPLATE

        self.model_storage.json_set("args", args)

    @staticmethod
    def create_validation(_, args: Dict = None, **kwargs):
        if "using" not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args["using"]
        if "prompt_template" not in args:
            if not args.get("mode") == "retrieval":
                raise ValueError("Please provide a `prompt_template` for this engine.")

    def predict(self, df: pd.DataFrame, args: Dict = None) -> pd.DataFrame:
        """
        Dispatch is performed depending on the underlying model type. Currently, only the default text completion
        is supported.
        """
        pred_args = args["predict_params"] if args else {}
        args = self.model_storage.json_get("args")
        if "prompt_template" not in args and "prompt_template" not in pred_args:
            raise ValueError("This model expects a `prompt_template`, please provide one.")
        # Back compatibility for old models
        args["provider"] = args.get("provider", get_llm_provider(args))
        args["embedding_model_provider"] = args.get("embedding_model", get_embedding_model_provider(args))

        df = df.reset_index(drop=True)

        if pred_args.get("mode") == "chat_model":
            return self.call_llm(df, args, pred_args)

        agent = self.create_agent(df, args, pred_args)
        # Use last message as prompt, remove other questions.
        user_column = args.get("user_column", USER_COLUMN)
        if user_column not in df.columns:
            raise Exception(
                f"Expected user input in column `{user_column}`, which is not found in the input data. Either provide the column, or redefine the expected column at model creation (`USING user_column = 'value'`)"
            )  # noqa
        df.iloc[:-1, df.columns.get_loc(user_column)] = None
        return self.run_agent(df, agent, args, pred_args)

    def call_llm(self, df, args=None, pred_args=None):
        llm = create_chat_model({**args, **pred_args})

        user_column = args.get("user_column", USER_COLUMN)
        assistant_column = args.get("assistant_column", ASSISTANT_COLUMN)

        question = df[user_column].iloc[-1]
        resp = llm([HumanMessage(question)], stop=["\nObservation:", "\n\tObservation:"])

        return pd.DataFrame([resp.content], columns=[assistant_column])

    def create_agent(self, df: pd.DataFrame, args: Dict = None, pred_args: Dict = None) -> AgentExecutor:
        pred_args = pred_args if pred_args else {}

        # Set up tools.
        model_kwargs = get_chat_model_params({**args, **pred_args})
        llm = create_chat_model({**args, **pred_args})

        tools = setup_tools(llm, model_kwargs, pred_args, self.default_agent_tools)

        # Prefer prediction prompt template over original if provided.
        prompt_template = pred_args.get("prompt_template", args["prompt_template"])
        if "context" in pred_args:
            prompt_template += "\n\n" + "Useful information:\n" + pred_args["context"] + "\n"

        # Set up memory.
        memory = ConversationSummaryBufferMemory(
            llm=llm, max_token_limit=model_kwargs.get("max_tokens", DEFAULT_MAX_TOKENS), memory_key="chat_history"
        )
        memory.chat_memory.messages.insert(0, SystemMessage(content=prompt_template))
        # User - Assistant conversation. All except the last message.
        user_column = args.get("user_column", USER_COLUMN)
        assistant_column = args.get("assistant_column", ASSISTANT_COLUMN)
        for row in df[:-1].to_dict("records"):
            question = row[user_column]
            answer = row[assistant_column]
            if question:
                memory.chat_memory.add_user_message(question)
            if answer:
                memory.chat_memory.add_ai_message(answer)

        agent_type = args.get("agent_type", DEFAULT_AGENT_TYPE)
        agent_executor = initialize_agent(
            tools,
            llm,
            agent=agent_type,
            # Use custom output parser to handle flaky LLMs that don't ALWAYS conform to output format.
            agent_kwargs={"output_parser": SafeOutputParser()},
            # Calls the agent’s LLM Chain one final time to generate a final answer based on the previous steps
            early_stopping_method="generate",
            handle_parsing_errors=self._handle_parsing_errors,
            # Timeout per agent invocation.
            max_execution_time=pred_args.get(
                "timeout_seconds", args.get("timeout_seconds", DEFAULT_AGENT_TIMEOUT_SECONDS)
            ),
            max_iterations=pred_args.get("max_iterations", args.get("max_iterations", DEFAULT_MAX_ITERATIONS)),
            memory=memory,
            verbose=pred_args.get("verbose", args.get("verbose", True)),
        )
        return agent_executor

    def run_agent(self, df: pd.DataFrame, agent: AgentExecutor, args: Dict, pred_args: Dict) -> pd.DataFrame:
        # Prefer prediction time prompt template, if available.
        base_template = pred_args.get("prompt_template", args["prompt_template"])

        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace("{", "").replace("}", ""))
        empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

        base_template = base_template.replace("{{", "{").replace("}}", "}")
        prompts = []

        user_column = args.get("user_column", USER_COLUMN)
        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                prompt = PromptTemplate(input_variables=input_variables, template=base_template)
                kwargs = {}
                for col in input_variables:
                    kwargs[col] = row[col] if row[col] is not None else ""  # add empty quote if data is missing
                prompts.append(prompt.format(**kwargs))
            elif row.get(user_column):
                # Just add prompt
                prompts.append(row[user_column])

        def _invoke_agent_executor_with_prompt(agent_executor, prompt):
            if not prompt:
                return ""
            try:
                answer = agent_executor.invoke(prompt)
            except Exception as e:
                answer = str(e)
                if not answer.startswith("Could not parse LLM output: `"):
                    raise e
                answer = {"output": answer.removeprefix("Could not parse LLM output: `").removesuffix("`")}

            if "output" not in answer:
                # This should never happen unless Langchain changes invoke output format, but just in case.
                return agent_executor.run(prompt)
            return answer["output"]

        completions = []
        # max_workers defaults to number of processors on the machine multiplied by 5.
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        max_workers = args.get("max_workers", None)
        agent_timeout_seconds = args.get("timeout", DEFAULT_AGENT_TIMEOUT_SECONDS)
        executor = ContextThreadPoolExecutor(max_workers=max_workers)
        futures = [executor.submit(_invoke_agent_executor_with_prompt, agent, prompt) for prompt in prompts]
        try:
            for future in as_completed(futures, timeout=agent_timeout_seconds):
                completions.append(future.result())
        except TimeoutError:
            completions.append(
                f"I'm sorry! I couldn't generate a response within the allotted time ({agent_timeout_seconds} seconds). "
                "If you need more time for processing, you can adjust the timeout settings. "
                "Please refer to the documentation for instructions on how to change the timeout value. "
                "Feel free to try your request again."
            )
        # Can't use ThreadPoolExecutor as context manager since we need wait=False.
        executor.shutdown(wait=False)

        # Add null completion for empty prompts
        for i in sorted(empty_prompt_ids)[:-1]:
            completions.insert(i, None)

        pred_df = pd.DataFrame(completions, columns=[args["target"]])

        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        tables = ["info"]
        return pd.DataFrame(tables, columns=["tables"])

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        raise NotImplementedError("Fine-tuning is not supported for LangChain models")
