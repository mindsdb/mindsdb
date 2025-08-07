import textwrap
import json
import numpy as np
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
import os
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.libs.base import BaseMLEngine
from openai import OpenAI, NotFoundError, AuthenticationError
import pandas as pd
from .constants import AIMLAPI_API_BASE, DEFAULT_MODEL
from typing import Dict, Optional, Text, Any
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class AimlapiHandler(BaseMLEngine):
    """Handle connection and inference with the Aimlapi API."""

    name = "aimlapi"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = DEFAULT_MODEL
        self.api_base = AIMLAPI_API_BASE
        self.generative = True

    def create_engine(self, connection_args: Dict) -> None:
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("aimlapi_api_key")
        if api_key is not None:
            org = connection_args.get("api_organization")
            api_base = connection_args.get("api_base") or os.environ.get("AIMLAPI_API_BASE", AIMLAPI_API_BASE)
            self.engine_storage.json_set("args", {"api_base": api_base, "api_organization": org})
            client = self._get_client(api_key=api_key, base_url=api_base, org=org)
            AimlapiHandler._check_client_connection(client)

    @staticmethod
    def _check_client_connection(client: OpenAI) -> None:
        try:
            client.models.retrieve("test")
        except NotFoundError:
            pass
        except AuthenticationError as e:
            if e.body["code"] == "invalid_api_key":
                raise Exception("Invalid api key")
            raise Exception(f"Something went wrong: {e}")

    @staticmethod
    def _get_client(api_key: Text, base_url: Text, org: Optional[Text] = None) -> OpenAI:
        headers = {
            "HTTP-Referer": "https://mindsdb.com/",
            "X-Title": "MindsDB",
        }

        client = OpenAI(api_key=api_key, base_url=base_url, organization=org, default_headers=headers)
        return client

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        args = args["using"]
        args["target"] = target
        self.model_storage.json_set("args", args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> pd.DataFrame:
        model_args = self.model_storage.json_get("args")
        engine_args = self.engine_storage.json_get("args") or {}

        pred_args = args["predict_params"] if args else {}
        df = df.reset_index(drop=True)

        if pred_args.get("prompt_template", False):
            base_template = pred_args["prompt_template"]
        elif model_args.get("prompt_template", False):
            base_template = model_args["prompt_template"]
        else:
            base_template = None

        if model_args.get("prompt_template", False):
            prompts, empty_prompt_ids = get_completed_prompts(base_template, df)
        elif model_args.get("context_column", False):
            empty_prompt_ids = np.where(
                df[[model_args["context_column"], model_args["question_column"]]].isna().all(axis=1).values
            )[0]
            contexts = list(df[model_args["context_column"]].apply(lambda x: str(x)))
            questions = list(df[model_args["question_column"]].apply(lambda x: str(x)))
            prompts = [
                f"Give only answer for: \nContext: {c}\nQuestion: {q}\nAnswer: " for c, q in zip(contexts, questions)
            ]
        elif model_args.get("json_struct", False):
            empty_prompt_ids = np.where(df[[model_args["question_column"]]].isna().all(axis=1).values)[0]
            prompts = []
            for i in df.index:
                if "json_struct" in df.columns:
                    if isinstance(df["json_struct"][i], str):
                        df["json_struct"][i] = json.loads(df["json_struct"][i])
                    json_struct = ""
                    for ind, val in enumerate(df["json_struct"][i].values()):
                        json_struct = json_struct + f"{ind}. {val}\n"
                else:
                    json_struct = ""
                    for ind, val in enumerate(model_args["json_struct"].values()):
                        json_struct = json_struct + f"{ind + 1}. {val}\n"

                p = textwrap.dedent(
                    f"""
                    Using text starting after 'The text is:', give exactly {len(model_args["json_struct"])} answers to the questions:
                    {{json_struct}}
                    Answers should be in the same order as the questions.
                    Answer should be in form of one JSON Object eg. {{'key':'value',..}} where key=question and value=answer.
                    If there is no answer to the question in the text, put a -.
                    Answers should be as short as possible, ideally 1-2 words (unless otherwise specified).
                    The text is:
                    {{{{ {model_args["question_column"]} }}}}
                """
                )
                p = p.replace("{json_struct}", json_struct)
                for key in df.columns:
                    if key == "json_struct":
                        continue
                    placeholder = f"{{{{ {key} }}}}"
                    value = str(df[key][i])
                    p = p.replace(placeholder, value)
                prompts.append(p)
        elif "prompt" in model_args:
            empty_prompt_ids = []
            prompts = list(df[model_args["user_column"]])
        else:
            empty_prompt_ids = np.where(df[[model_args["question_column"]]].isna().all(axis=1).values)[0]
            prompts = list(df[model_args["question_column"]].apply(lambda x: str(x)))

        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        api_base = engine_args.get("api_base", AIMLAPI_API_BASE)
        api_key = get_api_key(self.name, model_args, engine_storage=self.engine_storage)

        client = self._get_client(api_key=api_key, base_url=api_base)
        model = model_args.get("model_name", self.default_model)
        results = []

        for prompt in prompts:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": prompt},
                ],
                stream=False,
            )
            answer = response.choices[0].message.content
            results.append(answer)

        pred_df = pd.DataFrame(results, columns=[model_args["target"]])
        return pred_df
