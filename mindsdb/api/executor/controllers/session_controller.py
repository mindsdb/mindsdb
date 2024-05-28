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

from mindsdb.api.executor.datahub.datahub import init_datahub
from mindsdb.utilities.config import Config
from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SessionController:
    """
    This class manages the server session.
    """

    def __init__(self, api_type: str = 'http'):
        """
        Initialize the session.

        :param api_type: The type of API (default is 'http').
        """
        self.api_type = api_type
        self.username = None
        self.auth = False
        self.logging = logger
        self.database = None

        self.config = Config()

        self.model_controller = ModelController()
        self.integration_controller = integration_controller
        self.database_controller = DatabaseController()
        self.skills_controller = SkillsController()

        # To prevent circular imports
        from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
        self.kb_controller = KnowledgeBaseController(self)

        self.datahub = init_datahub(self)
        self.agents_controller = AgentsController(self.datahub)

        self.prepared_stmts = {}
        self.packet_sequence_number = 0
        self.profiling = False
        self.predictor_cache = True
        self.show_secrets = False

    def inc_packet_sequence_number(self) -> None:
        """
        Increment the packet sequence number, wrapping around at 256.
        """
        self.packet_sequence_number = (self.packet_sequence_number + 1) % 256

    def register_stmt(self, statement: str) -> int:
        """
        Register a new statement.

        :param statement: The SQL statement to register.
        :return: The ID of the registered statement.
        :raises Exception: If too many queries are registered.
        """
        if len(self.prepared_stmts) >= 100:
            logger.error("Too many unclosed queries.")
            raise Exception("Too many unclosed queries")

        # Find the first available ID
        stmt_id = next(i for i in range(1, 101) if i not in self.prepared_stmts)
        self.prepared_stmts[stmt_id] = {'type': None, 'statement': statement, 'fetched': 0}
        logger.debug(f"Registered statement with ID {stmt_id}: {statement}")
        return stmt_id

    def unregister_stmt(self, stmt_id: int) -> None:
        """
        Unregister a statement by ID.

        :param stmt_id: The ID of the statement to unregister.
        """
        if stmt_id in self.prepared_stmts:
            logger.debug(f"Unregistering statement with ID {stmt_id}")
            del self.prepared_stmts[stmt_id]
        else:
            logger.warning(f"Tried to unregister non-existing statement ID {stmt_id}")

    def to_json(self) -> dict:
        """
        Convert the session state to a JSON serializable dictionary.

        :return: The session state as a dictionary.
        """
        state = {
            "username": self.username,
            "auth": self.auth,
            "database": self.database,
            "prepared_stmts": self.prepared_stmts,
            "packet_sequence_number": self.packet_sequence_number,
        }
        logger.debug(f"Session state converted to JSON: {state}")
        return state

    def from_json(self, updated: dict) -> None:
        """
        Update the session state from a JSON dictionary.

        :param updated: The updated state as a dictionary.
        """
        for key, value in updated.items():
            setattr(self, key, value)
            logger.debug(f"Updated session attribute {key} to {value}")
