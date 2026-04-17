import concurrent.futures
import json
import math
import os
import textwrap
from typing import Text, Dict, List, Optional, Any

import numpy as np
import openai
import pandas as pd
from mindsdb.integrations.handlers.lemonade_handler.constants import (
    CHAT_MODELS_PREFIXES,
    IMAGE_MODELS,
    FINETUNING_MODELS,
    LEMONADE_API_BASE,
    DEFAULT_CHAT_MODEL,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_IMAGE_MODEL,
)
from mindsdb.integrations.handlers.lemonade_handler.helpers import (
    retry_with_exponential_backoff,
    truncate_msgs_for_token_limit,
    get_available_models,
    PendingFT,
)
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.utilities import log
from mindsdb.utilities.hooks import before_openai_query, after_openai_query
from openai import OpenAI, NotFoundError, AuthenticationError
from openai.types.fine_tuning import FineTuningJob

logger = log.getLogger(__name__)


class LemonadeHandler(BaseMLEngine):
    """
    This handler handles connection and inference with the Lemonade API.
    Lemonade is a local LLM server that provides OpenAI-compatible API endpoints.
    """

    name = "lemonade"

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
            # Note: Lemonade doesn't support image generation or embeddings like OpenAI
        ]
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100
        self.supported_ft_models = FINETUNING_MODELS  # Lemonade doesn't support fine-tuning
        # For now this are only used for handlers that inherits LemonadeHandler and don't need to override base methods
        self.api_key_name = getattr(self, "api_key_name", self.name)
        self.api_base = getattr(self, "api_base", LEMONADE_API_BASE)

    def create_engine(self, connection_args: Dict) -> None:
        """
        Validate the Lemonade API credentials on engine creation.

        Args:
            connection_args (Dict): Parameters for the engine.

        Raises:
            Exception: If the handler is not configured with valid API credentials.

        Returns:
            None
        """
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get("lemonade_api_key", "lemonade")  # Default key for Lemonade
        api_base = connection_args.get("api_base") or os.environ.get("LEMONADE_API_BASE", LEMONADE_API_BASE)
        client = self._get_client(api_key=api_key, base_url=api_base, org=None, args=connection_args)
        LemonadeHandler._check_client_connection(client)

    @staticmethod
    def is_chat_model(model_name):
        for prefix in CHAT_MODELS_PREFIXES:
            if model_name.lower().startswith(prefix.lower()):
                return True
        return False

    @staticmethod
    def _check_client_connection(client: OpenAI) -> None:
        """
        Check the Lemonade engine client connection by retrieving a model.

        Args:
            client (openai.OpenAI): Lemonade client configured with the API credentials.

        Raises:
            Exception: If the client connection (API key) is invalid or something else goes wrong.

        Returns:
            None
        """
        try:
            # Try to list models to check connection
            client.models.list()
        except NotFoundError:
            pass
        except AuthenticationError as e:
            if e.body["code"] == "invalid_api_key":
                raise Exception("Invalid api key")
            raise Exception(f"Something went wrong: {e}")
        except Exception as e:
            # Lemonade might not be running, which is okay for now
            logger.warning(f"Could not connect to Lemonade server: {e}")

    @staticmethod
    def create_validation(target: Text, args: Dict = None, **kwargs: Any) -> None:
        """
        Validate the Lemonade API credentials on model creation.

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
            raise Exception("Lemonade engine requires a USING clause! Refer to its documentation for more details.")
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
                "lemonade_api_key",
                "api_base",
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
        api_key = get_api_key("lemonade", args, engine_storage=engine_storage)
        api_base = (
            args.get("api_base")
            or connection_args.get("api_base")
            or os.environ.get("LEMONADE_API_BASE", LEMONADE_API_BASE)
        )
        client = LemonadeHandler._get_client(api_key=api_key, base_url=api_base, org=None, args=args)
        LemonadeHandler._check_client_connection(client)

    def create(self, target, args: Dict = None, **kwargs: Any) -> None:
        """
        Create a model by connecting to the Lemonade API.

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
                or os.environ.get("LEMONADE_API_BASE")
                or self.api_base
            )
            client = self._get_client(api_key=api_key, base_url=api_base, org=None, args=args)
            available_models = get_available_models(client)

            if not args.get("mode"):
                args["mode"] = self.default_mode
            elif args["mode"] not in self.supported_modes:
                raise Exception(f"Invalid operation mode. Please use one of {self.supported_modes}")

            if not args.get("model_name"):
                args["model_name"] = self.default_model
            elif (args["model_name"] not in available_models) and (args["mode"] != "embedding"):
                # For Lemonade, we're more lenient with model names since they can vary
                logger.warning(f"Model {args['model_name']} not found in available models, but proceeding anyway.")
        finally:
            self.model_storage.json_set("args", args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Make predictions using a model connected to the Lemonade API.

        Args:
            df (pd.DataFrame): Input data to make predictions on.
            args (Dict): Parameters passed when making predictions.

        Raises:
            Exception: If the model is not configured with valid parameters or if the input data is not in the expected format.

        Returns:
            pd.DataFrame: Input data with the predicted values in a new column.
        """  # noqa
        # Note: Lemonade doesn't support embeddings or image generation like OpenAI

        pred_args = args["predict_params"] if args else {}
        args = self.model_storage.json_get("args")
        connection_args = self.engine_storage.get_connection_args()

        args["api_base"] = (
            pred_args.get("api_base")
            or args.get("api_base")
            or connection_args.get("api_base")
            or os.environ.get("LEMONADE_API_BASE")
            or self.api_base
        )

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

        # Chat or normal completion mode (Lemonade doesn't support embeddings or images)
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
        Handles completion for an arbitrary amount of rows using a model connected to the Lemonade API.

        This method consists of several inner methods:
            - _submit_completion: Submit a request to the relevant completion endpoint of the Lemonade API based on the type of task.
            - _submit_normal_completion: Submit a request to the completion endpoint of the Lemonade API.
            - _submit_chat_completion: Submit a request to the chat completion endpoint of the Lemonade API.
            - _log_api_call: Log the API call made to the Lemonade API.

        There are a couple checks that should be done when calling Lemonade's API:
          - account max batch size, to maximize batch size first
          - account rate limit, to maximize parallel calls second

        Additionally, single completion calls are done with exponential backoff to guarantee all prompts are processed,
        because even with previous checks the tokens-per-minute limit may apply.

        Args:
            model_name (Text): Lemonade Model name.
            prompts (List[Text]): List of prompts.
            api_key (Text): Lemonade API key.
            api_args (Dict): Lemonade API arguments.
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
            Submit a request to the relevant completion endpoint of the Lemonade API based on the type of task.

            Args:
                model_name (Text): Lemonade Model name.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Lemonade API arguments.
                args (Dict): Parameters for the model.
                df (pd.DataFrame): Input data to run completion on.

            Returns:
                List[Text]: List of completions.
            """
            kwargs = {
                "model": model_name,
            }
            if self.is_chat_model(model_name):
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
            Log the API call made to the Lemonade API.

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
            logger.debug(f">>>lemonade call: {params2}:\n{response}")

        def _submit_normal_completion(kwargs: Dict, prompts: List[Text], api_args: Dict) -> List[Text]:
            """
            Submit a request to the completion endpoint of the Lemonade API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the completion endpoint of the Lemonade API.

            Args:
                kwargs (Dict): Lemonade API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other Lemonade API arguments.

            Returns:
                List[Text]: List of text completions.
            """

            def _tidy(comp: openai.types.completion.Completion) -> List[Text]:
                """
                Parse and tidy up the response from the completion endpoint of the Lemonade API.

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

        def _submit_chat_completion(
            kwargs: Dict, prompts: List[Text], api_args: Dict, df: pd.DataFrame, mode: Text = "conversational"
        ) -> List[Text]:
            """
            Submit a request to the chat completion endpoint of the Lemonade API.

            This method consists of an inner method:
                - _tidy: Parse and tidy up the response from the chat completion endpoint of the Lemonade API.

            Args:
                kwargs (Dict): Lemonade API arguments, including the model to use.
                prompts (List[Text]): List of prompts.
                api_args (Dict): Other Lemonade API arguments.
                df (pd.DataFrame): Input data to run chat completion on.
                mode (Text): Mode of operation.

            Returns:
                List[Text]: List of chat completions as text.
            """

            def _tidy(comp: openai.types.chat.chat_completion.ChatCompletion) -> List[Text]:
                """
                Parse and tidy up the response from the chat completion endpoint of the Lemonade API.

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

        client = self._get_client(
            api_key=api_key,
            base_url=args.get("api_base"),
            org=None,  # Lemonade doesn't use organization
            args=args,
        )

        try:
            # check if simple completion works
            completion = _submit_completion(model_name, prompts, api_args, args, df)
            return completion
        except Exception as e:
            # else, we get the max batch size
            error_str = str(e)
            if "you can currently request up to at most a total of" in error_str:
                pattern = "a total of"
                max_batch_size = int(error_str[error_str.find(pattern) + len(pattern) :].split(").")[0])
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
                    org=None,  # Lemonade doesn't use organization
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
        Fine-tuning is not supported by Lemonade.

        Args:
            df (Optional[pd.DataFrame]): Input data to fine-tune on.
            args (Optional[Dict]): Parameters for the fine-tuning process.

        Raises:
            Exception: Fine-tuning is not supported by Lemonade.

        Returns:
            None
        """
        raise Exception("Fine-tuning is not supported by Lemonade. Lemonade is designed for running pre-trained models locally.")

    @staticmethod
    def _get_client(api_key: Text, base_url: Text, org: Optional[Text] = None, args: dict = None) -> OpenAI:
        """
        Get a Lemonade client with the given API key and base URL.

        Args:
            api_key (Text): Lemonade API key (can be any value, required but unused).
            base_url (Text): Lemonade base URL.
            org (Optional[Text]): Organization (not used by Lemonade).

        Returns:
            openai.OpenAI: Lemonade client.
        """
        return OpenAI(api_key=api_key, base_url=base_url, organization=org)
