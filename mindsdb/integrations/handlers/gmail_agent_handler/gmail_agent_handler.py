import json
from shutil import copyfile
from collections import OrderedDict

import requests

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    HandlerStatusResponse
)

from langchain.llms import OpenAI
from langchain.agents import initialize_agent, AgentType
from langchain.tools.gmail.utils import build_resource_service, get_gmail_credentials
from langchain.agents.agent_toolkits import GmailToolkit

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb_sql.parser import ast
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb.utilities.config import Config

import os
import time
from typing import List
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.message import EmailMessage

from base64 import urlsafe_b64encode, urlsafe_b64decode

from .utils import AuthException, google_auth_flow, save_creds_to_file

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class GmailAgentHandler(APIHandler):
    """A class for handling connections and interactions with the Gmail API.

    Attributes:
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.api_resource = None
        self.connection_args = kwargs.get('connection_data', {})

    def back_office_config(self):
        tools = {
            'login': 'Log in to the gmail account. There is no input, just call the method.',
            'call_agent': 'Calls langchain agent. This is useful when performing any email operations.',
        }
        return {'tools': tools}
    
    def login(self, args={}):
        return ("Logged in")

    def call_agent(self, data):
        return ("Agent called")
        if self.api_resource is None:
            self.login()
        openai_key = os.environ["OPENAI_API_KEY"]
        llm = OpenAI(temperature=0, openai_api_key=openai_key)
        toolkit = GmailToolkit(api_resource=self.api_resource)
        agent = initialize_agent(
            tools=toolkit.get_tools(),
            llm=llm,
            agent=AgentType.STRUCTURED_CHAT_ZERO_SHOT_REACT_DESCRIPTION,
            verbose=True,
            return_intermediate_steps=True,
        )
        response = agent({"input": data})
        return response
    
    def connect(self):
        self.is_connected = True
        return self
    
    def check_connection(self) -> StatusResponse:
        """Checks connection to Gmail API.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(True)
        return response