import os
from typing import Optional, Dict

import openai
import pandas as pd
import llama_index

from llama_index.llms.openai import OpenAI
from llama_index.readers.schema.base import Document
from llama_index.readers import SimpleWebPageReader
from llama_index.prompts import PromptTemplate
from llama_index import ServiceContext, StorageContext, load_index_from_storage
from llama_index import LLMPredictor, OpenAIEmbedding
from llama_index.indices.vector_store.base import VectorStore

from llama_hub.github_repo import GithubClient, GithubRepositoryReader
from llama_hub.youtube_transcript import YoutubeTranscriptReader, is_youtube_video

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config
from mindsdb.utilities.security import is_private_url
from mindsdb.integrations.handlers.llama_index_handler import config
from mindsdb.integrations.handlers.llama_index_handler.github_loader_helper import (
    _get_github_token,
    _get_filter_file_extensions,
    _get_filter_directories,
)
from mindsdb.integrations.utilities.handler_utils import get_api_key


def _validate_prompt_template(prompt_template: str):
    if "{context_str}" not in prompt_template or "{query_str}" not in prompt_template:
        raise Exception(
            "Provided prompt template is invalid, missing `{context_str}`, `{query_str}`. Please ensure both placeholders are present and try again."
        )  # noqa


class LlamaIndexHandler(BaseMLEngine):
    """Integration with the LlamaIndex data framework for LLM applications."""

    name = "llama_index"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_index_class = "GPTVectorStoreIndex"
        self.supported_index_class = ["GPTVectorStoreIndex", "VectorStoreIndex"]
        self.default_reader = "DFReader"
        self.supported_reader = [
            "DFReader",
            "SimpleWebPageReader",
            "GithubRepositoryReader",
            "YoutubeTranscriptReader",
        ]

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        reader = args["using"].get("reader", "DFReader")

        if reader not in config.data_loaders:
            raise Exception(
                f"Invalid reader argument. Please use one of {config.data_loaders.keys()}"
            )
        config_dict = config.data_loaders[reader]

        missing_keys = [key for key in config_dict if key not in args["using"]]
        if missing_keys:
            raise Exception(f"{reader} requires {missing_keys} arguments")

        if "prompt_template" in args["using"]:
            _validate_prompt_template(args["using"]["prompt_template"])

        if args["using"].get("mode") == "conversational":
            for param in ("user_column", "assistant_column"):
                if param not in args["using"]:
                    raise Exception(f"Conversational mode requires {param} parameter")

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:
        if "using" not in args:
            raise Exception(
                "LlamaIndex engine requires a USING clause! Refer to its documentation for more details."
            )

        if "index_class" not in args["using"]:
            args["using"]["index_class"] = self.default_index_class
        elif args["using"]["index_class"] not in self.supported_index_class:
            raise Exception(
                f"Invalid index class argument. Please use one of {self.supported_index_class}"
            )

        if "reader" not in args["using"]:
            args["using"]["reader"] = self.default_reader
        elif args["using"]["reader"] not in self.supported_reader:
            raise Exception(
                f"Invalid operation mode. Please use one of {self.supported_reader}"
            )

        # workaround to create llama model without input data
        if df is None or df.empty:
            df = pd.DataFrame([{"text": ""}])

        if args["using"]["reader"] == "DFReader":
            dstrs = df.apply(
                lambda x: ", ".join(
                    [f"{col}: {str(entry)}" for col, entry in zip(df.columns, x)]
                ),
                axis=1,
            )
            reader = list(map(lambda x: Document(text=x), dstrs.tolist()))

        elif args["using"]["reader"] == "SimpleWebPageReader":
            url = args["using"]["source_url_link"]
            config = Config()
            is_cloud = config.get("cloud", False)
            if is_cloud and is_private_url(url):
                raise Exception(f"URL is private: {url}")

            reader = SimpleWebPageReader(html_to_text=True).load_data([url])

        elif args["using"]["reader"] == "GithubRepositoryReader":
            engine_storage = self.engine_storage
            key = "GITHUB_TOKEN"
            github_token = get_api_key(
                key, args["using"], engine_storage, strict=False
            )
            if github_token is None:
                github_token = get_api_key(
                    key.lower(),
                    args["using"],
                    engine_storage,
                    strict=True,
                )

            github_client = GithubClient(github_token)
            owner = args["using"]["owner"]
            repo = args["using"]["repo"]
            filter_file_extensions = _get_filter_file_extensions(args["using"])
            filter_directories = _get_filter_directories(args["using"])

            reader = GithubRepositoryReader(
                github_client,
                owner=owner,
                repo=repo,
                verbose=True,
                filter_file_extensions=filter_file_extensions,
                filter_directories=filter_directories,
            ).load_data(branch=args["using"].get("branch", "main"))

        elif args["using"]["reader"] == "YoutubeTranscriptReader":
            ytlinks = args["using"]["ytlinks"]
            for link in ytlinks:
                if not is_youtube_video(link):
                    raise Exception(f"Invalid youtube link: {link}")
            reader = YoutubeTranscriptReader().load_data(ytlinks)

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
        prompt_template = args["using"].get(
            "prompt_template", args.get("prompt_template", None)
        )
        if prompt_template is not None:
            _validate_prompt_template(prompt_template)
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
                _validate_prompt_template(prompt_template)
                engine_kwargs["text_qa_template"] = PromptTemplate(prompt_template)

            if input_column is None:
                raise Exception(
                    f"`input_column` must be provided at model creation time or through USING clause when predicting. Please try again."
                )  # noqa

            if input_column not in df.columns:
                raise Exception(
                    f'Column "{input_column}" not found in input data! Please try again.'
                )

            questions = df[input_column]

        index_path = self.model_storage.folder_get("context")
        storage_context = StorageContext.from_defaults(persist_dir=index_path)
        service_context = self._get_service_context()
        index = load_index_from_storage(
            storage_context, service_context=service_context
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

    def _get_service_context(self):
        args = self.model_storage.json_get("args")
        engine_storage = self.engine_storage

        openai_api_key = get_api_key('openai', args["using"], engine_storage, strict=True)
        llm_kwargs = {"openai_api_key": openai_api_key}

        if "temperature" in args["using"]:
            llm_kwargs["temperature"] = args["using"]["temperature"]
        if "model_name" in args["using"]:
            llm_kwargs["model_name"] = args["using"]["model_name"]
        if "max_tokens" in args["using"]:
            llm_kwargs["max_tokens"] = args["using"]["max_tokens"]

        llm = OpenAI(**llm_kwargs)  # TODO: all usual params should go here
        embed_model = OpenAIEmbedding(api_key=openai_api_key)
        service_context = ServiceContext.from_defaults(
            llm=llm,
            embed_model=embed_model
        )
        return service_context

    def _setup_index(self, documents):
        args = self.model_storage.json_get("args")
        indexer: VectorStore = getattr(llama_index, args["using"]["index_class"])
        index = indexer.from_documents(
            documents, service_context=self._get_service_context()
        )

        return index
