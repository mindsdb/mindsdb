import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from mindsdb.integrations.libs.llm.config import (
    AnthropicConfig,
    BaseLLMConfig,
    GoogleConfig,
    LiteLLMConfig,
    OllamaConfig,
    OpenAIConfig,
    NvidiaNIMConfig,
    MindsdbConfig,
    WriterConfig,
    BedrockConfig,
)
from mindsdb.utilities.config import config


# Default to latest GPT-4 model (https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo)
DEFAULT_OPENAI_MODEL = "gpt-4o"
# Requires more than vanilla OpenAI due to ongoing summarization and 3rd party input.
DEFAULT_OPENAI_MAX_TOKENS = 8096
DEFAULT_OPENAI_MAX_RETRIES = 3

DEFAULT_ANTHROPIC_MODEL = "claude-3-haiku-20240307"

DEFAULT_GOOGLE_MODEL = "gemini-2.5-pro-preview-03-25"

DEFAULT_LITELLM_MODEL = "gpt-3.5-turbo"
DEFAULT_LITELLM_PROVIDER = "openai"
DEFAULT_LITELLM_BASE_URL = "https://ai.dev.mindsdb.com"

DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "llama2"

DEFAULT_NVIDIA_NIM_BASE_URL = "http://localhost:8000/v1"  # Assumes local port forwarding through ssh
DEFAULT_NVIDIA_NIM_MODEL = "meta/llama-3_1-8b-instruct"
DEFAULT_VLLM_SERVER_URL = "http://localhost:8000/v1"


def get_completed_prompts(base_template: str, df: pd.DataFrame, strict=True) -> Tuple[List[str], np.ndarray]:
    """
    Helper method that produces formatted prompts given a template and data in a Pandas DataFrame.
    It also returns the ID of any empty templates that failed to be filled due to missing data.

    :param base_template: string with placeholders for each column in the DataFrame. Placeholders should follow double curly braces format, e.g. `{{column_name}}`. All placeholders should have matching columns in `df`.
    :param df: pd.DataFrame to generate full prompts. Each placeholder in `base_template` must exist as a column in the DataFrame. If a column is not in the template, it is ignored entirely.
    :param strict: raise exception if base_template doesn't contain placeholders

    :return prompts: list of in-filled prompts using `base_template` and relevant columns from `df`
    :return empty_prompt_ids: np.int numpy array (shape (n_missing_rows,)) with the row indexes where in-fill failed due to missing data.
    """  # noqa
    columns = []
    spans = []
    matches = list(re.finditer("{{(.*?)}}", base_template))

    if len(matches) == 0:
        # no placeholders
        if strict:
            raise AssertionError("No placeholders found in the prompt, please provide a valid prompt template.")
        prompts = [base_template] * len(df)
        return prompts, np.ndarray(0)

    first_span = matches[0].start()
    last_span = matches[-1].end()

    for m in matches:
        columns.append(m[0].replace("{", "").replace("}", ""))
        spans.extend((m.start(), m.end()))

    spans = spans[1:-1]  # omit first and last, they are added separately
    template = [
        base_template[s:e] for s, e in list(zip(spans, spans[1:]))[::2]
    ]  # take every other to skip placeholders  # noqa
    template.insert(0, base_template[0:first_span])  # add prompt start
    template.append(base_template[last_span:])  # add prompt end

    empty_prompt_ids = np.where(df[columns].isna().all(axis=1).values)[0]

    df["__mdb_prompt"] = ""
    for i in range(len(template)):
        atom = template[i]
        if i < len(columns):
            col = df[columns[i]].replace(to_replace=[None], value="")  # add empty quote if data is missing
            df["__mdb_prompt"] = df["__mdb_prompt"].apply(lambda x: x + atom) + col.astype("string")
        else:
            df["__mdb_prompt"] = df["__mdb_prompt"].apply(lambda x: x + atom)
    prompts = list(df["__mdb_prompt"])

    return prompts, empty_prompt_ids


def get_llm_config(provider: str, args: Dict) -> BaseLLMConfig:
    """
    Helper method that returns the configuration for a given LLM provider.

    :param provider: string with the name of the provider.
    :param config: dictionary with the configuration for the provider.

    :return: LLMConfig object with the configuration for the provider.
    """
    temperature = min(1.0, max(0.0, args.get("temperature", 0.0)))
    if provider == "openai":
        if any(x in args.get("model_name", "") for x in ["o1", "o3"]):
            # for o1 and 03, 'temperature' does not support 0.0 with this model. Only the default (1) value is supported
            temperature = 1

        return OpenAIConfig(
            model_name=args.get("model_name", DEFAULT_OPENAI_MODEL),
            temperature=temperature,
            max_retries=args.get("max_retries", DEFAULT_OPENAI_MAX_RETRIES),
            max_tokens=args.get("max_tokens", DEFAULT_OPENAI_MAX_TOKENS),
            openai_api_base=args.get("base_url", None),
            openai_api_key=args["api_keys"].get("openai", None),
            openai_organization=args.get("api_organization", None),
            request_timeout=args.get("request_timeout", None),
        )
    if provider == "anthropic":
        return AnthropicConfig(
            model=args.get("model_name", DEFAULT_ANTHROPIC_MODEL),
            temperature=temperature,
            max_tokens=args.get("max_tokens", None),
            top_p=args.get("top_p", None),
            top_k=args.get("top_k", None),
            default_request_timeout=args.get("default_request_timeout", None),
            anthropic_api_key=args["api_keys"].get("anthropic", None),
            anthropic_api_url=args.get("base_url", None),
        )
    if provider == "litellm":
        model_kwargs = {
            "api_key": args["api_keys"].get("litellm", None),
            "top_p": args.get("top_p", None),
            "request_timeout": args.get("request_timeout", None),
            "frequency_penalty": args.get("frequency_penalty", None),
            "presence_penalty": args.get("presence_penalty", None),
            "logit_bias": args.get("logit_bias", None),
        }
        return LiteLLMConfig(
            model=args.get("model_name", DEFAULT_LITELLM_MODEL),
            temperature=temperature,
            api_base=args.get("base_url", DEFAULT_LITELLM_BASE_URL),
            max_retries=args.get("max_retries", DEFAULT_OPENAI_MAX_RETRIES),
            max_tokens=args.get("max_tokens", DEFAULT_OPENAI_MAX_TOKENS),
            top_p=args.get("top_p", None),
            top_k=args.get("top_k", None),
            custom_llm_provider=args.get("custom_llm_provider", DEFAULT_LITELLM_PROVIDER),
            model_kwargs=model_kwargs,
        )
    if provider == "ollama":
        return OllamaConfig(
            base_url=args.get("base_url", DEFAULT_OLLAMA_BASE_URL),
            model=args.get("model_name", DEFAULT_OLLAMA_MODEL),
            temperature=temperature,
            top_p=args.get("top_p", None),
            top_k=args.get("top_k", None),
            timeout=args.get("request_timeout", None),
            format=args.get("format", None),
            headers=args.get("headers", None),
            num_predict=args.get("num_predict", None),
            num_ctx=args.get("num_ctx", None),
            num_gpu=args.get("num_gpu", None),
            repeat_penalty=args.get("repeat_penalty", None),
            stop=args.get("stop", None),
            template=args.get("template", None),
        )
    if provider == "nvidia_nim":
        return NvidiaNIMConfig(
            base_url=args.get("base_url", DEFAULT_NVIDIA_NIM_BASE_URL),
            model=args.get("model_name", DEFAULT_NVIDIA_NIM_MODEL),
            temperature=temperature,
            top_p=args.get("top_p", None),
            timeout=args.get("request_timeout", None),
            format=args.get("format", None),
            headers=args.get("headers", None),
            num_predict=args.get("num_predict", None),
            num_ctx=args.get("num_ctx", None),
            num_gpu=args.get("num_gpu", None),
            repeat_penalty=args.get("repeat_penalty", None),
            stop=args.get("stop", None),
            template=args.get("template", None),
            nvidia_api_key=args["api_keys"].get("nvidia_nim", None),
        )
    if provider == "mindsdb":
        return MindsdbConfig(
            model_name=args["model_name"],
            project_name=args.get("project_name", config.get("default_project")),
        )
    if provider == "vllm":
        return OpenAIConfig(
            model_name=args.get("model_name"),
            temperature=temperature,
            max_retries=args.get("max_retries", DEFAULT_OPENAI_MAX_RETRIES),
            max_tokens=args.get("max_tokens", DEFAULT_OPENAI_MAX_TOKENS),
            openai_api_base=args.get("base_url", DEFAULT_VLLM_SERVER_URL),
            openai_api_key=args["api_keys"].get("vllm", "EMPTY`"),
            openai_organization=args.get("api_organization", None),
            request_timeout=args.get("request_timeout", None),
        )
    if provider == "google":
        return GoogleConfig(
            model=args.get("model_name", DEFAULT_GOOGLE_MODEL),
            temperature=temperature,
            top_p=args.get("top_p", None),
            top_k=args.get("top_k", None),
            max_output_tokens=args.get("max_tokens", None),
            google_api_key=args["api_keys"].get("google", None),
        )
    if provider == "writer":
        return WriterConfig(
            model_name=args.get("model_name", "palmyra-x5"),
            temperature=temperature,
            max_tokens=args.get("max_tokens", None),
            top_p=args.get("top_p", None),
            stop=args.get("stop", None),
            best_of=args.get("best_of", None),
            writer_api_key=args["api_keys"].get("writer", None),
            writer_org_id=args.get("writer_org_id", None),
            base_url=args.get("base_url", None),
        )
    if provider == "bedrock":
        return BedrockConfig(
            model_id=args.get("model_name"),
            temperature=temperature,
            max_tokens=args.get("max_tokens", None),
            stop=args.get("stop", None),
            base_url=args.get("endpoint_url", None),
            aws_access_key_id=args.get("aws_access_key_id", None),
            aws_secret_access_key=args.get("aws_secret_access_key", None),
            aws_session_token=args.get("aws_session_token", None),
            region_name=args.get("aws_region_name", None),
            credentials_profile_name=args.get("credentials_profile_name", None),
            model_kwargs=args.get("model_kwargs", None),
        )

    raise ValueError(f"Provider {provider} is not supported.")
