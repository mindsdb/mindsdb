"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""
from mindsdb.api.executor.datahub.datanodes import InformationSchemaDataNode
from mindsdb.utilities.config import Config
from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.interfaces.functions.controller import FunctionController

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SessionController:
    """
    This class manages the server session
    """

    def __init__(self, api_type='http') -> object:
        """
        Initialize the session
        """
        self.api_type = api_type
        self.username = None
        self.auth = False
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = ModelController()

        # to prevent circular imports
        from mindsdb.interfaces.database.integrations import integration_controller
        self.integration_controller = integration_controller

        self.database_controller = DatabaseController()
        self.skills_controller = SkillsController()
        self.function_controller = FunctionController(self)

        # to prevent circular imports
        from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
        self.kb_controller = KnowledgeBaseController(self)

        self.datahub = InformationSchemaDataNode(self)
        self.agents_controller = AgentsController()

        self.prepared_stmts = {}
        self.packet_sequence_number = 0
        self.profiling = False
        self.predictor_cache = False if self.config.get('cache')['type'] == 'none' else True
        self.show_secrets = False

    def inc_packet_sequence_number(self):
        self.packet_sequence_number = (self.packet_sequence_number + 1) % 256

    def register_stmt(self, statement):
        i = 1
        while i in self.prepared_stmts and i < 100:
            i = i + 1
        if i == 100:
            raise Exception("Too many unclosed queries")

        self.prepared_stmts[i] = dict(type=None, statement=statement, fetched=0)
        return i

    def unregister_stmt(self, stmt_id):
        del self.prepared_stmts[stmt_id]

    def to_json(self):
        return {
            "username": self.username,
            "auth": self.auth,
            "database": self.database,
            "prepared_stmts": self.prepared_stmts,
            "packet_sequence_number": self.packet_sequence_number,
        }

    def from_json(self, updated):
        for key in updated:
            setattr(self, key, updated[key])
