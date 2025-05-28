from typing import Optional, Dict

import pandas as pd
import openai
from llama_index.llms.openai import OpenAI
from llama_index.core import Document
from llama_index.readers.web import SimpleWebPageReader
from llama_index.core import PromptTemplate
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config
from mindsdb.utilities.security import validate_urls
from mindsdb.integrations.handlers.llama_index_handler.settings import llama_index_config, LlamaIndexModel
from mindsdb.integrations.libs.api_handler_exceptions import MissingConnectionParams
from mindsdb.integrations.utilities.handler_utils import get_api_key


class LlamaIndexHandler(BaseMLEngine):
    """Integration with the LlamaIndex data framework for LLM applications."""

    name = "llama_index"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_index_class = llama_index_config.DEFAULT_INDEX_CLASS
        self.supported_index_class = llama_index_config.SUPPORTED_INDEXES
        self.default_reader = llama_index_config.DEFAULT_READER
        self.supported_reader = llama_index_config.SUPPORTED_READERS
        self.config = Config()

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise MissingConnectionParams("LlamaIndex engine requires USING clause!")
        else:
            args = args['using']
            LlamaIndexModel(**args)

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:
        # workaround to create llama model without input data
        if df is None or df.empty:
            df = pd.DataFrame([{"text": ""}])

        args_reader = args.get("using", {}).get("reader", self.default_reader)

        if args_reader == "DFReader":
            dstrs = df.apply(
                lambda x: ", ".join(
                    [f"{col}: {str(entry)}" for col, entry in zip(df.columns, x)]
                ),
                axis=1,
            )
            reader = list(map(lambda x: Document(text=x), dstrs.tolist()))
        elif args_reader == "SimpleWebPageReader":
            url = args["using"]["source_url_link"]
            allowed_urls = self.config.get('web_crawling_allowed_sites', [])
            if allowed_urls and not validate_urls(url, allowed_urls):
                raise ValueError(f"The provided URL is not allowed for web crawling. Please use any of {', '.join(allowed_urls)}.")
            reader = SimpleWebPageReader(html_to_text=True).load_data([url])
        else:
            raise Exception(
                f"Invalid operation mode. Please use one of {self.supported_reader}."
            )
        self.model_storage.json_set("args", args)
        index = self._setup_index(reader)
        path = self.model_storage.folder_get("context")
        index.storage_context.persist(persist_dir=path)
        self.model_storage.folder_sync("context")

    def update(self, args) -> None:
        args_cur = self.model_storage.json_get("args")
        args_cur["using"].update(args["using"])

        # check new set of arguments
        self.create_validation(None, args_cur)

        self.model_storage.json_set("args", args_cur)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        pred_args = args["predict_params"] if args else {}

        args = self.model_storage.json_get("args")
        engine_kwargs = {}

        if args["using"].get("mode") == "conversational":
            user_column = args["using"]["user_column"]
            assistant_column = args["using"]["assistant_column"]

            messages = []
            for row in df[:-1].to_dict("records"):
                messages.append(f"user: {row[user_column]}")
                messages.append(f"assistant: {row[assistant_column]}")

            conversation = "\n".join(messages)

            questions = [df.iloc[-1][user_column]]

            if "prompt" in pred_args and pred_args["prompt"] is not None:
                user_prompt = pred_args["prompt"]
            else:
                user_prompt = args["using"].get("prompt", "")

            prompt_template = (
                f"{user_prompt}\n"
                f"---------------------\n"
                f"We have provided context information below. \n"
                f"{{context_str}}\n"
                f"---------------------\n"
                f"This is previous conversation history:\n"
                f"{conversation}\n"
                f"---------------------\n"
                f"Given this information, please answer the question: {{query_str}}"
            )

            engine_kwargs["text_qa_template"] = PromptTemplate(prompt_template)

        else:
            input_column = args["using"].get("input_column", None)

            prompt_template = args["using"].get(
                "prompt_template", args.get("prompt_template", None)
            )
            if prompt_template is not None:
                self.create_validation(args=args)
                engine_kwargs["text_qa_template"] = PromptTemplate(prompt_template)

            if input_column is None:
                raise Exception(
                    "`input_column` must be provided at model creation time or through USING clause when predicting. Please try again."
                )  # noqa

            if input_column not in df.columns:
                raise Exception(
                    f'Column "{input_column}" not found in input data! Please try again.'
                )

            questions = df[input_column]

        index_path = self.model_storage.folder_get("context")
        storage_context = StorageContext.from_defaults(persist_dir=index_path)
        self._get_service_context()

        index = load_index_from_storage(
            storage_context
        )
        query_engine = index.as_query_engine(**engine_kwargs)

        results = []

        for question in questions:
            query_results = query_engine.query(
                question
            )  # TODO: provide extra_info in explain_target col
            results.append(query_results.response)

        result_df = pd.DataFrame(
            {"question": questions, args["target"]: results}
        )  # result_df['answer'].tolist()
        return result_df

    def _get_service_context(self) -> None:
        args = self.model_storage.json_get("args")
        engine_storage = self.engine_storage
        openai_api_key = get_api_key('openai', args["using"], engine_storage, strict=True)
        llm_kwargs = {"api_key": openai_api_key}

        if "temperature" in args["using"]:
            llm_kwargs["temperature"] = args["using"]["temperature"]
        if "model_name" in args["using"]:
            llm_kwargs["model_name"] = args["using"]["model_name"]
        if "max_tokens" in args["using"]:
            llm_kwargs["max_tokens"] = args["using"]["max_tokens"]
        # only way this works is by sending the key through openai

        openai.api_key = openai_api_key
        if Settings.llm is None:
            llm = OpenAI()
            Settings.llm = llm
        if Settings.embed_model is None:
            embed_model = OpenAIEmbedding()
            Settings.embed_model = embed_model
        # TODO: all usual params should be added to Settings

    def _setup_index(self, documents):
        self._get_service_context()
        index = VectorStoreIndex.from_documents(
            documents
        )
        return index
