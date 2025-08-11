import os
import math
import json
import shutil
import tempfile
import datetime
import textwrap
import subprocess
import concurrent.futures
from typing import Text, Tuple, Dict, List, Optional, Any
import openai
from openai.types.fine_tuning import FineTuningJob
from openai import OpenAI, AzureOpenAI, NotFoundError, AuthenticationError
import numpy as np
import pandas as pd

from mindsdb.utilities.hooks import before_openai_query, after_openai_query
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.openai_handler.helpers import (
    retry_with_exponential_backoff,
    truncate_msgs_for_token_limit,
    get_available_models,
    PendingFT,
)
from mindsdb.integrations.handlers.openai_handler.constants import (
    CHAT_MODELS_PREFIXES,
    IMAGE_MODELS,
    FINETUNING_MODELS,
    OPENAI_API_BASE,
    DEFAULT_CHAT_MODEL,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_IMAGE_MODEL,
)
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
from mindsdb.integrations.utilities.handler_utils import get_api_key

logger = log.getLogger(__name__)


class OpenAIHandler(BaseMLEngine):
    """
    This handler handles connection and inference with the OpenAI API.
    """

    name = "openai"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_model = DEFAULT_CHAT_MODEL
        self.default_embedding_model = DEFAULT_EMBEDDING_MODEL
        self.default_image_model = DEFAULT_IMAGE_MODEL
        self.default_mode = "default"  # can also be 'conversational' or 'conversational-full'
        self.supported_modes = [
            "default",
            "conversational",
            "conversational-full",
            "image",
            "embedding",
        ]
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100
        self.supported_ft_models = FINETUNING_MODELS  # base models compatible with finetuning
        # For now this are only used for handlers that inherits OpenAIHandler and don't need to override base methods
        self.api_key_name = getattr(self, "api_key_name", self.name)
        self.api_base = getattr(self, "api_base", OPENAI_API_BASE)

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validate the OpenAI API credentials on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("openai_api_key")
        if api_key is not None:
            org = connection_args.get("api_organization")
            api_base = connection_args.get("api_base") or os.environ.get("OPENAI_API_BASE", OPENAI_API_BASE)
            client = self._get_client(api_key=api_key, base_url=api_base, org=org, args=connection_args)
            OpenAIHandler._check_client_connection(client)

    @staticmethod
    def is_chat_model(model_name):
        for prefix in CHAT_MODELS_PREFIXES:
            if model_name.startswith(prefix):
                return True
        return False

    @staticmethod
    def _check_client_connection(client: OpenAI) -> None:
        """
        Check the OpenAI engine client connection by retrieving a model.

        Args:
            client (openai.OpenAI): OpenAI client configured with the API credentials.

        Raises:
            Exception: If the client connection (API key) is invalid or something else goes wrong.

        Returns:
            None
        """
        try:
            client.models.retrieve("test")
        except NotFoundError:
            pass
        except AuthenticationError as e:
            if e.body["code"] == "invalid_api_key":
                raise Exception("Invalid api key")
            raise Exception(f"Something went wrong: {e}")

    @staticmethod
    def create_validation(target: Text, args: Dict = None, **kwargs: Any) -> None:
        """
        Validate the OpenAI API credentials on model creation.

        Args:
            target (Text): Target column name.
            args (Dict): Parameters for the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        if "using" not in args:
            raise Exception("OpenAI engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args["using"]

        if len(set(args.keys()) & {"question_column", "prompt_template", "prompt"}) == 0:
            raise Exception("One of `question_column`, `prompt_template` or `prompt` is required for this engine.")

        keys_collection = [
            ["prompt_template"],
            ["question_column", "context_column"],
            ["prompt", "user_column", "assistant_column"],
        ]
        for keys in keys_collection:
            if keys[0] in args and any(x[0] in args for x in keys_collection if x != keys):
                raise Exception(
                    textwrap.dedent(
                        """\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `prompt', 'user_column' and 'assistant_column`
                """
                    )
                )

        # for all args that are not expected, raise an error
        known_args = set()
        # flatten of keys_collection
        for keys in keys_collection:
            known_args = known_args.union(set(keys))

        # TODO: need a systematic way to maintain a list of known args
        known_args = known_args.union(
            {
                "target",
                "model_name",
                "mode",
                "predict_params",
                "json_struct",
                "ft_api_info",
                "ft_result_stats",
                "runtime",
                "max_tokens",
                "temperature",
                "openai_api_key",
                "api_organization",
                "api_base",
                "api_version",
                "provider",
            }
        )

        unknown_args = set(args.keys()) - known_args
        if unknown_args:
            # return a list of unknown args as a string
            raise Exception(
                f"Unknown arguments: {', '.join(unknown_args)}.\n Known arguments are: {', '.join(known_args)}"
            )

        engine_storage = kwargs["handler_storage"]
        connection_args = engine_storage.get_connection_args()
        api_key = get_api_key("openai", args, engine_storage=engine_storage)
        api_base = (
            args.get("api_base")
            or connection_args.get("api_base")
            or os.environ.get("OPENAI_API_BASE", OPENAI_API_BASE)
        )
        org = args.get("api_organization")
        client = OpenAIHandler._get_client(api_key=api_key, base_url=api_base, org=org, args=args)
        OpenAIHandler._check_client_connection(client)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Create a model by connecting to the OpenAI API.

        Args:
            target (Text): Target column name.
            args (Dict): Parameters for the model.
            kwargs (Any): Other keyword arguments.

        Raises:
            Exception: If the model is not configured with valid parameters.

        Returns:
            None
        """
        args = args["using"]
        args["target"] = target
        try:
            api_key = get_api_key(self.api_key_name, args, self.engine_storage)
            connection_args = self.engine_storage.get_connection_args()
            api_base = (
                args.get("api_base")
                or connection_args.get("api_base")
                or os.environ.get("OPENAI_API_BASE")
                or self.api_base
            )
            client = self._get_client(api_key=api_key, base_url=api_base, org=args.get("api_organization"), args=args)
            available_models = get_available_models(client)

            if not args.get("mode"):
                args["mode"] = self.default_mode
            elif args["mode"] not in self.supported_modes:
                raise Exception(f"Invalid operation mode. Please use one of {self.supported_modes}")

            if not args.get("model_name"):
                if args["mode"] == "embedding":
                    args["model_name"] = self.default_embedding_model
                elif args["mode"] == "image":
                    args["model_name"] = self.default_image_model
                else:
                    args["model_name"] = self.default_model
            elif (args["model_name"] not in available_models) and (args["mode"] != "embedding"):
                raise Exception(f"Invalid model name. Please use one of {available_models}")
        finally:
            self.model_storage.json_set("args", args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Make predictions using a model connected to the OpenAI API.

        Args:
            df (pd.DataFrame): Input data to make predictions on.
            args (Dict): Parameters passed when making predictions.

        Raises:
            Exception: If the model is not configured with valid parameters or if the input data is not in the expected format.

        Returns:
            pd.DataFrame: Input data with the predicted values in a new column.
        """  # noqa
        # TODO: support for edits, embeddings and moderation

        pred_args = args["predict_params"] if args else {}
        args = self.model_storage.json_get("args")
        connection_args = self.engine_storage.get_connection_args()

        args["api_base"] = (
            pred_args.get("api_base")
            or args.get("api_base")
            or connection_args.get("api_base")
            or os.environ.get("OPENAI_API_BASE")
            or self.api_base
        )

        if pred_args.get("api_organization"):
            args["api_organization"] = pred_args["api_organization"]
        df = df.reset_index(drop=True)

        if pred_args.get("mode"):
            if pred_args["mode"] in self.supported_modes:
                args["mode"] = pred_args["mode"]
            else:
                raise Exception(f"Invalid operation mode. Please use one of {self.supported_modes}.")  # noqa

        strict_prompt_template = True
        if pred_args.get("prompt_template", False):
            base_template = pred_args["prompt_template"]  # override with predict-time template if available
            strict_prompt_template = False
        elif args.get("prompt_template", False):
            base_template = args["prompt_template"]
        else:
            base_template = None

        # Embedding mode
        if args.get("mode", self.default_mode) == "embedding":
            api_args = {
                "question_column": pred_args.get("question_column", None),
                "model": pred_args.get("model_name") or args.get("model_name"),
            }
            model_name = "embedding"
            if args.get("question_column"):
                prompts = list(df[args["question_column"]].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(df[[args["question_column"]]].isna().all(axis=1).values)[0]
            else:
                raise Exception("Embedding mode needs a question_column")

        # Image mode
        elif args.get("mode", self.default_mode) == "image":
            api_args = {
                "n": pred_args.get("n", None),
                "size": pred_args.get("size", None),
                "response_format": pred_args.get("response_format", None),
            }
            api_args = {k: v for k, v in api_args.items() if v is not None}  # filter out non-specified api args
            model_name = pred_args.get("model_name") or args.get("model_name")

            if args.get("question_column"):
                prompts = list(df[args["question_column"]].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(df[[args["question_column"]]].isna().all(axis=1).values)[0]
            elif args.get("prompt_template"):
                prompts, empty_prompt_ids = get_completed_prompts(base_template, df)
            else:
                raise Exception("Image mode needs either `prompt_template` or `question_column`.")

        # Chat or normal completion mode
        else:
            if args.get("question_column", False) and args["question_column"] not in df.columns:
                raise Exception(f"This model expects a question to answer in the '{args['question_column']}' column.")

            if args.get("context_column", False) and args["context_column"] not in df.columns:
                raise Exception(f"This model expects context in the '{args['context_column']}' column.")

            # API argument validation
            model_name = args.get("model_name", self.default_model)
            api_args = {
                "max_tokens": pred_args.get("max_tokens", args.get("max_tokens", self.default_max_tokens)),
                "temperature": min(
                    1.0,
                    max(0.0, pred_args.get("temperature", args.get("temperature", 0.0))),
                ),
                "top_p": pred_args.get("top_p", None),
                "n": pred_args.get("n", None),
                "stop": pred_args.get("stop", None),
                "presence_penalty": pred_args.get("presence_penalty", None),
                "frequency_penalty": pred_args.get("frequency_penalty", None),
                "best_of": pred_args.get("best_of", None),
                "logit_bias": pred_args.get("logit_bias", None),
                "user": pred_args.get("user", None),
            }

            if args.get("prompt_template", False):
                prompts, empty_prompt_ids = get_completed_prompts(base_template, df, strict=strict_prompt_template)

            elif args.get("context_column", False):
                empty_prompt_ids = np.where(
                    df[[args["context_column"], args["question_column"]]].isna().all(axis=1).values
                )[0]
                contexts = list(df[args["context_column"]].apply(lambda x: str(x)))
                questions = list(df[args["question_column"]].apply(lambda x: str(x)))
                prompts = [f"Context: {c}\nQuestion: {q}\nAnswer: " for c, q in zip(contexts, questions)]

            elif "prompt" in args:
                empty_prompt_ids = []
                prompts = list(df[args["user_column"]])
            else:
                empty_prompt_ids = np.where(df[[args["question_column"]]].isna().all(axis=1).values)[0]
                prompts = list(df[args["question_column"]].apply(lambda x: str(x)))

            # add json struct if available
            if args.get("json_struct", False):
                for i, prompt in enumerate(prompts):
                    json_struct = ""
                    if "json_struct" in df.columns and i not in empty_prompt_ids:
                        # if row has a specific json, we try to use it instead of the base prompt template
                        try:
                            if isinstance(df["json_struct"][i], str):
                                df["json_struct"][i] = json.loads(df["json_struct"][i])
                            for ind, val in enumerate(df["json_struct"][i].values()):
                                json_struct = json_struct + f"{ind}. {val}\n"
                        except Exception:
                            pass  # if the row's json is invalid, we use the prompt template instead

                    if json_struct == "":
                        for ind, val in enumerate(args["json_struct"].values()):
                            json_struct = json_struct + f"{ind + 1}. {val}\n"

                    p = textwrap.dedent(
                        f"""\
                            Based on the text following 'The reference text is:', assign values to the following {len(args["json_struct"])} JSON attributes:
                            {{{{json_struct}}}}

                            Values should follow the same order as the attributes above.
                            Each line in the answer should start with a dotted number, and should not repeat the name of the attribute, just the value.
                            Each answer must end with new line.
                            If there is no valid value to a given attribute in the text, answer with a - character.
                            Values should be as short as possible, ideally 1-2 words (unless otherwise specified).

                            Here is an example input of 3 attributes:
                                1. rental price
                                2. location
                                3. number of bathrooms

                            Here is an example output for the input:
                                1. 3000
                                2. Manhattan
                                3. 2

                            Now for the real task. The reference text is:
                            {prompt}
                        """
                    )

                    p = p.replace("{{json_struct}}", json_struct)
                    prompts[i] = p

        # remove prompts without signal from completion queue
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        api_key = get_api_key(self.api_key_name, args, self.engine_storage)
        api_args = {k: v for k, v in api_args.items() if v is not None}  # filter out non-specified api args
        completion = self._completion(model_name, prompts, api_key, api_args, args, df)

        # add null completion for empty prompts
        for i in sorted(empty_prompt_ids):
            completion.insert(i, None)

        pred_df = pd.DataFrame(completion, columns=[args["target"]])

        # restore json struct
        if args.get("json_struct", False):
            for i in pred_df.index:
                try:
                    if "json_struct" in df.columns:
                        json_keys = df["json_struct"][i].keys()
                    else:
                        json_keys = args["json_struct"].keys()
                    responses = pred_df[args["target"]][i].split("\n")
                    responses = [x[3:] for x in responses]  # del question index

                    pred_df[args["target"]][i] = {key: val for key, val in zip(json_keys, responses)}
                except Exception:
                    pred_df[args["target"]][i] = None

        return pred_df

    def _completion(
        self,
        model_name: Text,
        prompts: List[Text],
        api_key: Text,
        api_args: Dict,
        args: Dict,
        df: pd.DataFrame,
        parallel: bool = True,
    ) -> List[Any]:
        """
        Handles completion for an arbitrary amount of rows using a model connected to the OpenAI API.

        This method consists of several inner methods:
            - _submit_completion: Submit a request to the relevant completion endpoint of the OpenAI API based on the type of task.
            - _submit_normal_completion: Submit a request to the completion endpoint of the OpenAI API.
            - _submit_embedding_completion: Submit a request to the embeddings endpoint of the OpenAI API.
            - _submit_chat_completion: Submit a request to the chat completion endpoint of the OpenAI API.
            - _submit_image_completion: Submit a request to the image completion endpoint of the OpenAI API.
            - _log_api_call: Log the API call made to the OpenAI API.

        There are a couple checks that should be done when calling OpenAI's API:
          - account max batch size, to maximize batch size first
          - account rate limit, to maximize parallel calls second

        Additionally, single completion calls are done with exponential backoff to guarantee all prompts are processed,
        because even with previous checks the tokens-per-minute limit may apply.

        Args:
            model_name (Text): OpenAI Model name.
            prompts (List[Text]): List of prompts.
            api_key (Text): OpenAI API key.
            api_args (Dict): OpenAI API arguments.
            args (Dict): Parameters for the model.
            df (pd.DataFrame): Input data to run completion on.
            parallel (bool): Whether to use parallel processing.

        Returns:
            List[Any]: List of completions. The type of completion depends on the task type.
        """

        @retry_with_exponential_backoff()
        def _submit_completion(
            model_name: Text, prompts: List[Text], api_args: Dict, args: Dict, df: pd.DataFrame
        ) -> List[Text]:
            """
            Submit a request to the relevant completion endpoint of the OpenAI API based on the type of task.

            Args:
                model_name (Text): OpenAI Model name.
                prompts (List[Text]): List of prompts.
                api_args (Dict): OpenAI API arguments.
                args (Dict): Parameters for the model.
                df (pd.DataFrame): Input data to run completion on.

            Returns:
                List[Text]: List of completions.
            """
            kwargs = {
                "model": model_name,
            }
            if model_name in IMAGE_MODELS:
                return _submit_image_completion(kwargs, prompts, api_args)
            elif model_name == "embedding":
                return _submit_embedding_completion(kwargs, prompts, api_args)
            elif self.is_chat_model(model_name):
                if model_name == "gpt-3.5-turbo-instruct":
                    return _submit_normal_completion(kwargs, prompts, api_args)
                else:
                    return _submit_chat_completion(
                        kwargs,
                        prompts,
                        api_args,
                        df,
                        mode=args.get("mode", "conversational"),
                    )
            else:
                return _submit_normal_completion(kwargs, prompts, api_args)

        def _log_api_call(params: Dict, response: Any) -> None:
            """
            Log the API call made to the OpenAI API.

            Args:
                params (Dict): Parameters for the API call.
                response (Any): Response from the API.

            Returns:
                None
            """
            after_openai_query(params, response)

            params2 = params.copy()
            params2.pop("api_key", None)
            params2.pop("user", None)
            logger.debug(f">>>openai call: {params2}:\n{response}")

        def _submit_normal_completion(kwargs: Dict, prompts: List[Text], api_args: Dict) -> List[Text]:
            """
            Submit a request to the completion endpoint of the OpenAI API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the completion endpoint of the OpenAI API.

            Args:
                kwargs (Dict): OpenAI API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other OpenAI API arguments.

            Returns:
                List[Text]: List of text completions.
            """

            def _tidy(comp: openai.types.completion.Completion) -> List[Text]:
                """
                Parse and tidy up the response from the completion endpoint of the OpenAI API.

                Args:
                    comp (openai.types.completion.Completion): Completion object.

                Returns:
                    List[Text]: List of completions as text.
                """
                tidy_comps = []
                for c in comp.choices:
                    if hasattr(c, "text"):
                        tidy_comps.append(c.text.strip("\n").strip(""))
                return tidy_comps

            kwargs = {**kwargs, **api_args}

            before_openai_query(kwargs)
            responses = []
            for prompt in prompts:
                responses.extend(_tidy(client.completions.create(prompt=prompt, **kwargs)))
            _log_api_call(kwargs, responses)
            return responses

        def _submit_embedding_completion(kwargs: Dict, prompts: List[Text], api_args: Dict) -> List[float]:
            """
            Submit a request to the embeddings endpoint of the OpenAI API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the embeddings endpoint of the OpenAI API.

            Args:
                kwargs (Dict): OpenAI API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other OpenAI API arguments.

            Returns:
                List[float]: List of embeddings as numbers.
            """

            def _tidy(comp: openai.types.create_embedding_response.CreateEmbeddingResponse) -> List[float]:
                """
                Parse and tidy up the response from the embeddings endpoint of the OpenAI API.

                Args:
                    comp (openai.types.create_embedding_response.CreateEmbeddingResponse): Embedding object.

                Returns:
                    List[float]: List of embeddings as numbers.
                """
                tidy_comps = []
                for c in comp.data:
                    if hasattr(c, "embedding"):
                        tidy_comps.append([c.embedding])
                return tidy_comps

            kwargs["input"] = prompts
            kwargs = {**kwargs, **api_args}

            before_openai_query(kwargs)
            resp = _tidy(client.embeddings.create(**kwargs))
            _log_api_call(kwargs, resp)
            return resp

        def _submit_chat_completion(
            kwargs: Dict, prompts: List[Text], api_args: Dict, df: pd.DataFrame, mode: Text = "conversational"
        ) -> List[Text]:
            """
            Submit a request to the chat completion endpoint of the OpenAI API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the chat completion endpoint of the OpenAI API.

            Args:
                kwargs (Dict): OpenAI API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other OpenAI API arguments.
                df (pd.DataFrame): Input data to run chat completion on.
                mode (Text): Mode of operation.

            Returns:
                List[Text]: List of chat completions as text.
            """

            def _tidy(comp: openai.types.chat.chat_completion.ChatCompletion) -> List[Text]:
                """
                Parse and tidy up the response from the chat completion endpoint of the OpenAI API.

                Args:
                    comp (openai.types.chat.chat_completion.ChatCompletion): Chat completion object.

                Returns:
                    List[Text]: List of chat completions as text.
                """
                tidy_comps = []
                for c in comp.choices:
                    if hasattr(c, "message"):
                        tidy_comps.append(c.message.content.strip("\n").strip(""))
                return tidy_comps

            completions = []
            if mode != "conversational" or "prompt" not in args:
                initial_prompt = {
                    "role": "system",
                    "content": "You are a helpful assistant. Your task is to continue the chat.",
                }  # noqa
            else:
                # get prompt from model
                initial_prompt = {"role": "system", "content": args["prompt"]}  # noqa

            kwargs["messages"] = [initial_prompt]
            last_completion_content = None

            for pidx in range(len(prompts)):
                if mode != "conversational":
                    kwargs["messages"].append({"role": "user", "content": prompts[pidx]})
                else:
                    question = prompts[pidx]
                    if question:
                        kwargs["messages"].append({"role": "user", "content": question})

                    assistant_column = args.get("assistant_column")
                    if assistant_column in df.columns:
                        answer = df.iloc[pidx][assistant_column]
                    else:
                        answer = None
                    if answer:
                        kwargs["messages"].append({"role": "assistant", "content": answer})

                if mode == "conversational-full" or (mode == "conversational" and pidx == len(prompts) - 1):
                    kwargs["messages"] = truncate_msgs_for_token_limit(
                        kwargs["messages"], kwargs["model"], api_args["max_tokens"]
                    )
                    pkwargs = {**kwargs, **api_args}

                    before_openai_query(kwargs)
                    resp = _tidy(client.chat.completions.create(**pkwargs))
                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                elif mode == "default":
                    kwargs["messages"] = [initial_prompt] + [kwargs["messages"][-1]]
                    pkwargs = {**kwargs, **api_args}

                    before_openai_query(kwargs)
                    resp = _tidy(client.chat.completions.create(**pkwargs))
                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                else:
                    # in "normal" conversational mode, we request completions only for the last row
                    last_completion_content = None
                    completions.extend([""])

                if last_completion_content:
                    # interleave assistant responses with user input
                    kwargs["messages"].append({"role": "assistant", "content": last_completion_content[0]})

            return completions

        def _submit_image_completion(kwargs: Dict, prompts: List[Text], api_args: Dict) -> List[Text]:
            """
            Submit a request to the image generation endpoint of the OpenAI API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the image generation endpoint of the OpenAI API.

            Args:
                kwargs (Dict): OpenAI API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other OpenAI API arguments.

            Raises:
                Exception: If the maximum batch size is reached.

            Returns:
                List[Text]: List of image completions as URLs or base64 encoded images.
            """

            def _tidy(comp: List[openai.types.image.Image]) -> List[Text]:
                """
                Parse and tidy up the response from the image generation endpoint of the OpenAI API.

                Args:
                    comp (List[openai.types.image.Image]): Image completion objects.

                Returns:
                    List[Text]: List of image completions as URLs or base64 encoded images.
                """
                return [c.url if hasattr(c, "url") else c.b64_json for c in comp]

            completions = [client.images.generate(**{"prompt": p, **kwargs, **api_args}).data[0] for p in prompts]
            return _tidy(completions)

        client = self._get_client(
            api_key=api_key,
            base_url=args.get("api_base"),
            org=args.pop("api_organization") if "api_organization" in args else None,
            args=args,
        )

        try:
            # check if simple completion works
            completion = _submit_completion(model_name, prompts, api_args, args, df)
            return completion
        except Exception as e:
            # else, we get the max batch size
            if "you can currently request up to at most a total of" in str(e):
                pattern = "a total of"
                max_batch_size = int(e[e.find(pattern) + len(pattern) :].split(").")[0])
            else:
                max_batch_size = self.max_batch_size  # guards against changes in the API message

        if not parallel:
            completion = None
            for i in range(math.ceil(len(prompts) / max_batch_size)):
                partial = _submit_completion(
                    model_name,
                    prompts[i * max_batch_size : (i + 1) * max_batch_size],
                    api_args,
                    args,
                    df,
                )
                if not completion:
                    completion = partial
                else:
                    completion["choices"].extend(partial["choices"])
                    for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
                        completion["usage"][field] += partial["usage"][field]
        else:
            promises = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for i in range(math.ceil(len(prompts) / max_batch_size)):
                    logger.debug(f"{i * max_batch_size}:{(i + 1) * max_batch_size}/{len(prompts)}")
                    future = executor.submit(
                        _submit_completion,
                        model_name,
                        prompts[i * max_batch_size : (i + 1) * max_batch_size],
                        api_args,
                        args,
                        df,
                    )
                    promises.append({"choices": future})
            completion = None
            for p in promises:
                if not completion:
                    completion = p["choices"].result()
                else:
                    completion.extend(p["choices"].result())

        return completion

    def describe(self, attribute: Optional[Text] = None) -> pd.DataFrame:
        """
        Get the metadata or arguments of a model.

        Args:
            attribute (Optional[Text]): Attribute to describe. Can be 'args' or 'metadata'.

        Returns:
            pd.DataFrame: Model metadata or model arguments.
        """
        # TODO: Update to use update() artifacts

        args = self.model_storage.json_get("args")
        api_key = get_api_key(self.api_key_name, args, self.engine_storage)
        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
        elif attribute == "metadata":
            model_name = args.get("model_name", self.default_model)
            try:
                client = self._get_client(
                    api_key=api_key,
                    base_url=args.get("api_base"),
                    org=args.get("api_organization"),
                    args=args,
                )
                meta = client.models.retrieve(model_name)
            except Exception as e:
                meta = {"error": str(e)}
            return pd.DataFrame(dict(meta).items(), columns=["key", "value"])
        else:
            tables = ["args", "metadata"]
            return pd.DataFrame(tables, columns=["tables"])

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Fine-tune OpenAI GPT models via a MindsDB model connected to the OpenAI API.
        Steps are roughly:
          - Analyze input data and modify it according to suggestions made by the OpenAI utility tool
          - Get a training and validation file
          - Determine base model to use
          - Submit a fine-tuning job via the OpenAI API
          - Monitor progress with exponential backoff (which has been modified for greater control given a time budget in hours),
          - Gather stats once fine-tuning finishes
          - Modify model metadata so that the new version triggers the fine-tuned version of the model (stored in the user's OpenAI account)

        Caveats:
          - As base fine-tuning models, OpenAI only supports the original GPT ones: `ada`, `babbage`, `curie`, `davinci`. This means if you fine-tune successively more than once, any fine-tuning other than the most recent one is lost.
          - A bunch of helper methods exist to be overridden in other handlers that follow the OpenAI API, e.g. Anyscale

        Args:
            df (Optional[pd.DataFrame]): Input data to fine-tune on.
            args (Optional[Dict]): Parameters for the fine-tuning process.

        Raises:
            Exception: If the model does not support fine-tuning.

        Returns:
            None
        """  # noqa
        args = args if args else {}

        api_key = get_api_key(self.api_key_name, args, self.engine_storage)

        using_args = args.pop("using") if "using" in args else {}

        api_base = using_args.get("api_base", os.environ.get("OPENAI_API_BASE", OPENAI_API_BASE))
        org = using_args.get("api_organization")
        client = self._get_client(api_key=api_key, base_url=api_base, org=org, args=args)

        args = {**using_args, **args}
        prev_model_name = self.base_model_storage.json_get("args").get("model_name", "")

        if prev_model_name not in self.supported_ft_models:
            # base model may be already FTed, check prefixes
            for model in self.supported_ft_models:
                if model in prev_model_name:
                    break
            else:
                raise Exception(
                    f"This model cannot be finetuned. Supported base models are {self.supported_ft_models}."
                )

        finetune_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        temp_storage_path = tempfile.mkdtemp()
        temp_file_name = f"ft_{finetune_time}"
        temp_model_storage_path = f"{temp_storage_path}/{temp_file_name}.jsonl"

        file_names = self._prepare_ft_jsonl(df, temp_storage_path, temp_file_name, temp_model_storage_path)

        jsons = {k: None for k in file_names.keys()}
        for split, file_name in file_names.items():
            if os.path.isfile(os.path.join(temp_storage_path, file_name)):
                jsons[split] = client.files.create(
                    file=open(f"{temp_storage_path}/{file_name}", "rb"), purpose="fine-tune"
                )

        if type(jsons["train"]) is openai.types.FileObject:
            train_file_id = jsons["train"].id
        else:
            train_file_id = jsons["base"].id

        if type(jsons["val"]) is openai.types.FileObject:
            val_file_id = jsons["val"].id
        else:
            val_file_id = None

        # `None` values are internally imputed by OpenAI to `null` or default values
        ft_params = {
            "training_file": train_file_id,
            "validation_file": val_file_id,
            "model": self._get_ft_model_type(prev_model_name),
        }
        ft_params = self._add_extra_ft_params(ft_params, using_args)

        start_time = datetime.datetime.now()

        ft_stats, result_file_id = self._ft_call(ft_params, client, args.get("hour_budget", 8))
        ft_model_name = ft_stats.fine_tuned_model

        end_time = datetime.datetime.now()
        runtime = end_time - start_time
        name_extension = client.files.retrieve(file_id=result_file_id).filename
        result_path = f"{temp_storage_path}/ft_{finetune_time}_result_{name_extension}"

        try:
            client.files.content(file_id=result_file_id).stream_to_file(result_path)
            if ".csv" in name_extension:
                # legacy endpoint
                train_stats = pd.read_csv(result_path)
                if "validation_token_accuracy" in train_stats.columns:
                    train_stats = train_stats[train_stats["validation_token_accuracy"].notnull()]
                args["ft_api_info"] = ft_stats.dict()
                args["ft_result_stats"] = train_stats.to_dict()

            elif ".json" in name_extension:
                train_stats = pd.read_json(path_or_buf=result_path, lines=True)  # new endpoint
                args["ft_api_info"] = args["ft_result_stats"] = train_stats.to_dict()

        except Exception:
            logger.info(
                f"Error retrieving fine-tuning results. Please check manually for information on job {ft_stats.id} (result file {result_file_id})."
            )

        args["model_name"] = ft_model_name
        args["runtime"] = runtime.total_seconds()
        args["mode"] = self.base_model_storage.json_get("args").get("mode", self.default_mode)

        self.model_storage.json_set("args", args)
        shutil.rmtree(temp_storage_path)

    @staticmethod
    def _prepare_ft_jsonl(df: pd.DataFrame, _, temp_filename: Text, temp_model_path: Text) -> Dict:
        """
        Prepare the input data for fine-tuning.

        Args:
            df (pd.DataFrame): Input data to fine-tune on.
            temp_filename (Text): Temporary filename.
            temp_model_path (Text): Temporary model path.

        Returns:
            Dict: File names for the fine-tuning process.
        """
        df.to_json(temp_model_path, orient="records", lines=True)

        # TODO avoid subprocess usage once OpenAI enables non-CLI access, or refactor to use our own LLM utils instead
        subprocess.run(
            [
                "openai",
                "tools",
                "fine_tunes.prepare_data",
                "-f",
                temp_model_path,  # from file
                "-q",  # quiet mode (accepts all suggestions)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        file_names = {
            "original": f"{temp_filename}.jsonl",
            "base": f"{temp_filename}_prepared.jsonl",
            "train": f"{temp_filename}_prepared_train.jsonl",
            "val": f"{temp_filename}_prepared_valid.jsonl",
        }
        return file_names

    def _get_ft_model_type(self, model_name: Text) -> Text:
        """
        Get the model to use for fine-tuning. If the model is not supported, the default model (babbage-002) is used.

        Args:
            model_name (Text): Model name.

        Returns:
            Text: Model to use for fine-tuning.
        """
        for model_type in self.supported_ft_models:
            if model_type in model_name.lower():
                return model_type
        return "babbage-002"

    @staticmethod
    def _add_extra_ft_params(ft_params: Dict, using_args: Dict) -> Dict:
        """
        Add extra parameters to the fine-tuning process.

        Args:
            ft_params (Dict): Parameters for the fine-tuning process required by the OpenAI API.
            using_args (Dict): Parameters passed when calling the fine-tuning process via a model.

        Returns:
            Dict: Fine-tuning parameters with extra parameters.
        """
        extra_params = {
            "n_epochs": using_args.get("n_epochs", None),
            "batch_size": using_args.get("batch_size", None),
            "learning_rate_multiplier": using_args.get("learning_rate_multiplier", None),
            "prompt_loss_weight": using_args.get("prompt_loss_weight", None),
            "compute_classification_metrics": using_args.get("compute_classification_metrics", None),
            "classification_n_classes": using_args.get("classification_n_classes", None),
            "classification_positive_class": using_args.get("classification_positive_class", None),
            "classification_betas": using_args.get("classification_betas", None),
        }
        return {**ft_params, **extra_params}

    def _ft_call(self, ft_params: Dict, client: OpenAI, hour_budget: int) -> Tuple[FineTuningJob, Text]:
        """
        Submit a fine-tuning job via the OpenAI API.
        This method handles requests to both the legacy and new endpoints.
        Currently, `OpenAIHandler` uses the legacy endpoint. Others, like `AnyscaleEndpointsHandler`, use the new endpoint.

        This method consists of an inner method:
            - _check_ft_status: Check the status of a fine-tuning job via the OpenAI API.

        Args:
            ft_params (Dict): Fine-tuning parameters.
            client (openai.OpenAI): OpenAI client.
            hour_budget (int): Hour budget for the fine-tuning process.

        Raises:
            PendingFT: If the fine-tuning process is still pending.

        Returns:
            Tuple[FineTuningJob, Text]: Fine-tuning stats and result file ID.
        """
        ft_result = client.fine_tuning.jobs.create(**{k: v for k, v in ft_params.items() if v is not None})

        @retry_with_exponential_backoff(
            hour_budget=hour_budget,
        )
        def _check_ft_status(job_id: Text) -> FineTuningJob:
            """
            Check the status of a fine-tuning job via the OpenAI API.

            Args:
                job_id (Text): Fine-tuning job ID.

            Raises:
                PendingFT: If the fine-tuning process is still pending.

            Returns:
                FineTuningJob: Fine-tuning stats.
            """
            ft_retrieved = client.fine_tuning.jobs.retrieve(fine_tuning_job_id=job_id)
            if ft_retrieved.status in ("succeeded", "failed", "cancelled"):
                return ft_retrieved
            else:
                raise PendingFT("Fine-tuning still pending!")

        ft_stats = _check_ft_status(ft_result.id)

        if ft_stats.status != "succeeded":
            err_message = ft_stats.events[-1].message if hasattr(ft_stats, "events") else "could not retrieve!"
            ft_status = ft_stats.status if hasattr(ft_stats, "status") else "N/A"
            raise Exception(
                f"Fine-tuning did not complete successfully (status: {ft_status}). Error message: {err_message}"
            )  # noqa

        result_file_id = client.fine_tuning.jobs.retrieve(fine_tuning_job_id=ft_result.id).result_files[0]
        if hasattr(result_file_id, "id"):
            result_file_id = result_file_id.id  # legacy endpoint

        return ft_stats, result_file_id

    @staticmethod
    def _get_client(api_key: Text, base_url: Text, org: Optional[Text] = None, args: dict = None) -> OpenAI:
        """
        Get an OpenAI client with the given API key, base URL, and organization.

        Args:
            api_key (Text): OpenAI API key.
            base_url (Text): OpenAI base URL.
            org (Optional[Text]): OpenAI organization.

        Returns:
            openai.OpenAI: OpenAI client.
        """
        if args is not None and args.get("provider") == "azure":
            return AzureOpenAI(
                api_key=api_key, azure_endpoint=base_url, api_version=args.get("api_version"), organization=org
            )
        return OpenAI(api_key=api_key, base_url=base_url, organization=org)
