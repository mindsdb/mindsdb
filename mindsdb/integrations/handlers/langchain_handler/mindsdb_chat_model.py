from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
)

import pandas as pd
from langchain_core.callbacks import (
    CallbackManagerForLLMRun,
)
from langchain_core.language_models.chat_models import (
    BaseChatModel,
)
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    ChatMessage,
    FunctionMessage,
    HumanMessage,
    SystemMessage,
)
from langchain_core.outputs import (
    ChatGeneration,
    ChatResult,
)
from langchain_core.pydantic_v1 import root_validator

logger = logging.getLogger(__name__)


def _convert_message_to_dict(message: BaseMessage) -> dict:
    if isinstance(message, ChatMessage):
        message_dict = {"role": message.role, "content": message.content}
    elif isinstance(message, HumanMessage):
        message_dict = {"role": "user", "content": message.content}
    elif isinstance(message, AIMessage):
        message_dict = {"role": "assistant", "content": message.content}
        if "function_call" in message.additional_kwargs:
            message_dict["function_call"] = message.additional_kwargs["function_call"]
    elif isinstance(message, SystemMessage):
        message_dict = {"role": "system", "content": message.content}
    elif isinstance(message, FunctionMessage):
        message_dict = {
            "role": "function",
            "content": message.content,
            "name": message.name,
        }
    else:
        raise ValueError(f"Got unknown type {message}")
    if "name" in message.additional_kwargs:
        message_dict["name"] = message.additional_kwargs["name"]
    return message_dict


class ChatMindsdb(BaseChatModel):
    """A chat model that uses the Mindsdb"""

    model_name: str
    project_name: Optional[str] = 'mindsdb'
    model_info: Optional[dict] = None
    output_col: Optional[str] = None
    project_datanode: Optional[Any] = None

    @property
    def _default_params(self) -> Dict[str, Any]:
        return {}

    def completion(
        self, messages: List[dict]
    ) -> Any:

        model_info = self.model_info

        # TODO create table for conversational model?
        if len(messages) > 1:
            content = '\n'.join([
                f"{m['role']}: {m['content']}"
                for m in messages
            ])
        else:
            content = messages[0]['content']

        record = {}
        params = {}
        if model_info.get('mode') == 'conversational':
            user_column = model_info['user_column']
            record[user_column] = content
        else:
            params['prompt_template'] = content

        predictions = self.project_datanode.predict(
            model_name=self.model_name,
            df=pd.DataFrame([record]),
            params=params,
        )

        col = self.output_col
        if col not in predictions.columns:
            # get first column
            col = predictions.columns[0]

        # get first row
        result = predictions[col][0]

        # TODO token calculation
        return {
            'messages': [result]
        }

    @root_validator()
    def validate_environment(cls, values: Dict) -> Dict:

        model_name = values['model_name']
        project_name = values['project_name']

        from mindsdb.api.executor.controllers import SessionController

        session = SessionController()
        session.database = 'mindsdb'

        model = session.model_controller.get_model(model_name, project_name=project_name)

        values['model_info'] = model['problem_definition'].get('using', {})
        values["output_col"] = model['predict']

        project_datanode = session.datahub.get(values['project_name'])

        values["project_datanode"] = project_datanode

        return values

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        stream: Optional[bool] = None,
        **kwargs: Any,
    ) -> ChatResult:

        message_dicts = [_convert_message_to_dict(m) for m in messages]

        response = self.completion(
            messages=message_dicts
        )
        return self._create_chat_result(response)

    def _create_chat_result(self, response: Mapping[str, Any]) -> ChatResult:
        generations = []
        for content in response["messages"]:
            message = AIMessage(content=content)
            gen = ChatGeneration(
                message=message,
                generation_info=dict(finish_reason=None),
            )
            generations.append(gen)
        token_usage = response.get("usage", {})
        set_model_value = self.model_name
        if self.model_name is not None:
            set_model_value = self.model_name
        llm_output = {"token_usage": token_usage, "model": set_model_value}
        return ChatResult(generations=generations, llm_output=llm_output)

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        set_model_value = self.model_name
        if self.model_name is not None:
            set_model_value = self.model_name
        return {
            "model_name": set_model_value,
        }

    @property
    def _llm_type(self) -> str:
        return "mindsdb"
