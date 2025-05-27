import textwrap
from pydantic import BaseModel, Extra

import google.generativeai as palm
import numpy as np
import pandas as pd

from mindsdb.utilities.hooks import before_palm_query, after_palm_query
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts

from mindsdb.integrations.utilities.handler_utils import get_api_key

CHAT_MODELS = (
    "models/chat-bison-001",
    "models/embedding-gecko-001",
    "models/text-bison-001",
)

logger = log.getLogger(__name__)


class PalmHandlerArgs(BaseModel):
    target: str = None
    model_name: str = "models/chat-bison-001"
    mode: str = "default"
    predict_params: dict = None
    input_text: str = None
    ft_api_info: dict = None
    ft_result_stats: dict = None
    runtime: str = None
    max_output_tokens: int = 64
    temperature: float = 0.0
    api_key: str = None
    palm_api_key: str = None

    question_column: str = None
    answer_column: str = None
    context_column: str = None
    prompt_template: str = None
    prompt: str = None
    user_column: str = None
    assistant_column: str = None

    class Config:
        # for all args that are not expected, raise an error
        extra = Extra.forbid


class PalmHandler(BaseMLEngine):
    name = "palm"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.model_name = "models/chat-bison-001"
        self.model_name = (
            "default"  # can also be 'conversational' or 'conversational-full'
        )
        self.supported_modes = [
            "default",
            "conversational",
            "conversational-full",
            "embedding",
        ]
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_output_tokens = 64
        self.chat_completion_models = CHAT_MODELS

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "palm engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

        if (
            len(set(args.keys()) & {"question_column", "prompt_template", "prompt"})
            == 0
        ):
            raise Exception(
                "One of `question_column` or `prompt_template` is required for this engine."
            )

        # TODO: add example_column for conversational mode
        keys_collection = [
            ["prompt_template"],
            ["question_column", "context_column"],
            ["prompt", "user_column", "assistant_column"],
        ]
        for keys in keys_collection:
            if keys[0] in args and any(
                x[0] in args for x in keys_collection if x != keys
            ):
                raise Exception(
                    textwrap.dedent(
                        """\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `prompt' and 'user_column' and 'assistant_column`
                """
                    )
                )

    def create(self, target, args=None, **kwargs):
        args = args["using"]
        args_model = PalmHandlerArgs(**args)

        args_model.target = target
        api_key = get_api_key("palm", args["using"], self.engine_storage, strict=False)

        # Set palm api key
        palm.configure(api_key=api_key)

        available_models = [m.name for m in palm.list_models()]

        if not args_model.model_name:
            args_model.model_name = self.model_name
        elif args_model.model_name not in available_models:
            raise Exception(f"Invalid model name. Please use one of {available_models}")

        if not args_model.mode:
            args_model.mode = self.model_name
        elif args_model.mode not in self.supported_modes:
            raise Exception(
                f"Invalid operation mode. Please use one of {self.supported_modes}"
            )

        self.model_storage.json_set("args", args_model.model_dump())

    def predict(self, df, args=None):
        """
        If there is a prompt template, we use it. Otherwise, we use the concatenation of `context_column` (optional) and `question_column` to ask for a completion.
        """  # noqa
        # TODO: support for edits, embeddings and moderation

        pred_args = args["predict_params"] if args else {}
        args_model = PalmHandlerArgs(**self.model_storage.json_get("args"))
        df = df.reset_index(drop=True)

        if pred_args.get("mode"):
            if pred_args["mode"] in self.supported_modes:
                args_model.mode = pred_args["mode"]
            else:
                raise Exception(
                    f"Invalid operation mode. Please use one of {self.supported_modes}."
                )  # noqa

        if pred_args.get("prompt_template", False):
            base_template = pred_args[
                "prompt_template"
            ]  # override with predict-time template if available
        elif args_model.prompt_template:
            base_template = args_model.prompt_template
        else:
            base_template = None

        # Embedding Mode
        if args_model.mode == "embedding":
            api_args = {
                "model": pred_args.get("model_name", "models/embedding-gecko-001")
            }
            model_name = "models/embedding-gecko-001"
            if args_model.question_column:
                prompts = list(df[args_model.question_column].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(
                    df[[args_model.question_column]].isna().all(axis=1).values
                )[0]
            else:
                raise Exception("Embedding mode needs a question_column")

        # Chat or normal completion mode
        else:
            if (
                args_model.question_column
                and args_model.question_column not in df.columns
            ):
                raise Exception(
                    f"This model expects a question to answer in the '{args_model.question_column}' column."
                )

            if (
                args_model.context_column
                and args_model.context_column not in df.columns
            ):
                raise Exception(
                    f"This model expects context in the '{args_model.context_column}' column."
                )

            # api argument validation
            model_name = args_model.model_name
            api_args = {
                "max_output_tokens": pred_args.get(
                    "max_output_tokens",
                    args_model.max_output_tokens,
                ),
                "temperature": min(
                    1.0,
                    max(0.0, pred_args.get("temperature", args_model.temperature)),
                ),
                "top_p": pred_args.get("top_p", None),
                "candidate_count": pred_args.get("candidate_count", None),
                "stop_sequences": pred_args.get("stop_sequences", None),
            }

            if (
                args_model.mode != "default"
                and model_name not in self.chat_completion_models
            ):
                raise Exception(
                    f"Conversational modes are only available for the following models: {', '.join(self.chat_completion_models)}"
                )  # noqa

            if args_model.prompt_template:
                prompts, empty_prompt_ids = get_completed_prompts(
                    base_template, df
                )
                if len(prompts) == 0:
                    raise Exception("No prompts found")

            elif args_model.context_column:
                empty_prompt_ids = np.where(
                    df[[args_model.context_column, args_model.question_column]]
                    .isna()
                    .all(axis=1)
                    .values
                )[0]
                contexts = list(df[args_model.context_column].apply(lambda x: str(x)))
                questions = list(df[args_model.question_column].apply(lambda x: str(x)))
                prompts = [
                    f"Context: {c}\nQuestion: {q}\nAnswer: "
                    for c, q in zip(contexts, questions)
                ]
                api_args["context"] = "".join(contexts)

            elif args_model.prompt:
                empty_prompt_ids = []
                prompts = list(df[args_model.user_column])
                if len(prompts) == 0:
                    raise Exception("No prompts found")
            else:
                empty_prompt_ids = np.where(
                    df[[args_model.question_column]].isna().all(axis=1).values
                )[0]
                prompts = list(df[args_model.question_column].apply(lambda x: str(x)))

        # remove prompts without signal from completion queue
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        api_key = get_api_key("palm", args["using"], self.engine_storage, strict=False)
        api_args = {
            k: v for k, v in api_args.items() if v is not None
        }  # filter out non-specified api args
        completion = self._completion(
            model_name, prompts, api_key, api_args, args_model, df
        )

        # add null completion for empty prompts
        for i in sorted(empty_prompt_ids):
            completion.insert(i, None)

        pred_df = pd.DataFrame(completion, columns=[args_model.target])

        return pred_df

    def _completion(self, model_name, prompts, api_key, api_args, args_model, df):
        """
        Handles completion for an arbitrary amount of rows.
        Additionally, single completion calls are done with exponential backoff to guarantee all prompts are processed,
        because even with previous checks the tokens-per-minute limit may apply.
        """

        def _submit_completion(model_name, prompts, api_key, api_args, args_model, df):
            kwargs = {
                "model": model_name,
            }

            # configure the PaLM SDK with the provided API KEY
            palm.configure(api_key=api_key)

            if model_name == "models/embedding-gecko-001":
                prompts = "".join(prompts)
                return _submit_embedding_completion(kwargs, prompts, api_args)
            elif model_name == args_model.model_name:
                return _submit_chat_completion(
                    kwargs,
                    prompts,
                    api_args,
                    df,
                    mode=args_model.mode,
                )
            else:
                prompts = "".join(prompts)
                return _submit_normal_completion(kwargs, prompts, api_args)

        def _log_api_call(params, response):
            after_palm_query(params, response)

            params2 = params.copy()
            params2.pop("palm_api_key", None)
            params2.pop("user", None)
            logger.debug(f">>>palm call: {params2}:\n{response}")

        def _submit_normal_completion(kwargs, prompts, api_args):
            def _tidy(comp):
                tidy_comps = []
                if comp.candidates and len(comp.candidates) == 0:
                    return ["No completions found"]
                for c in comp.candidates:
                    if "output" in c:
                        tidy_comps.append(c["output"].strip("\n").strip(""))
                return tidy_comps

            kwargs["prompt"] = prompts
            kwargs = {**kwargs, **api_args}

            before_palm_query(kwargs)

            # call the palm sdk with text-bison-001 model
            resp = _tidy(palm.generate_text(**kwargs))
            _log_api_call(kwargs, resp)
            return resp

        def _submit_embedding_completion(kwargs, prompts, api_args):
            def _tidy(comp):
                tidy_comps = []
                if "embedding" not in comp:
                    return [f"No completion found, err {comp}"]
                for c in comp["embedding"]:
                    tidy_comps.append([c])
                return tidy_comps

            kwargs = {}
            kwargs["model"] = api_args["model"]
            kwargs["text"] = prompts

            before_palm_query(kwargs)

            # call the palm sdk with embedding-gecko-001 model
            resp = _tidy(palm.generate_embeddings(**kwargs))
            _log_api_call(kwargs, resp)
            return resp

        def _submit_chat_completion(
            kwargs, prompts, api_args, df, mode="conversational"
        ):
            def _tidy(comp):
                tidy_comps = []
                if comp.candidates and len(comp.candidates) == 0:
                    return ["No completions found"]

                for c in comp.candidates:
                    if "content" in c:
                        tidy_comps.append(c["content"].strip("\n").strip(""))
                    if "output" in c:
                        tidy_comps.append(c["output"].strip("\n").strip(""))
                return tidy_comps

            completions = []
            if mode != "conversational":
                initial_prompt = {
                    "author": "system",
                    "content": "You are a helpful assistant. Your task is to continue the chat.",
                }  # noqa
            else:
                # get prompt from model
                prompt = "".join(prompts)
                initial_prompt = {"author": "system", "content": prompt}  # noqa
                kwargs["messages"] = [initial_prompt]

            last_completion_content = None

            for pidx in range(len(prompts)):
                if mode == "conversational":
                    kwargs["messages"].append(
                        {"author": "user", "content": prompts[pidx]}
                    )

                if mode == "conversational-full" or (
                    mode == "conversational" and pidx == len(prompts) - 1
                ):
                    pkwargs = {**kwargs, **api_args}
                    pkwargs["candidate_count"] = 3
                    pkwargs.pop("max_output_tokens")
                    before_palm_query(kwargs)

                    # call the palm sdk with chat-bison-001 model
                    resp = _tidy(palm.chat(**pkwargs))

                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                elif mode == "default":
                    pkwargs = {**kwargs, **api_args}

                    pkwargs["model"] = "models/text-bison-001"
                    pkwargs["prompt"] = prompts[pidx]
                    before_palm_query(kwargs)
                    if pkwargs["prompt"] == "":
                        return ["No prompt provided"]

                    # call the palm sdk with text-bison-001 model
                    resp = _tidy(palm.generate_text(**pkwargs))
                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                else:
                    # in "normal" conversational mode, we request completions only for the last row
                    last_completion_content = None
                    if args_model.answer_column in df.columns:
                        # insert completion if provided, which saves redundant API calls
                        completions.extend([df.iloc[pidx][args_model.answer_column]])
                    else:
                        completions.extend([""])

                if args_model.answer_column in df.columns:
                    kwargs["messages"].append(
                        {
                            "author": "assistant",
                            "content": df.iloc[pidx][args_model.answer_column],
                        }
                    )
                elif last_completion_content:
                    # interleave assistant responses with user input
                    kwargs["messages"].append(
                        {"author": "assistant", "content": last_completion_content[0]}
                    )

            return completions

        try:
            completion = _submit_completion(
                model_name, prompts, api_key, api_args, args_model, df
            )
            return completion
        except Exception as e:
            completion = []
            logger.exception(e)
            completion.extend({"error": str(e)})

        return completion
