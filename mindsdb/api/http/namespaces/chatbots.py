from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.utilities.exception import EntityNotExistsError


def create_chatbot(project_name, name, chatbot):
    if name is None:
        return http_error(HTTPStatus.BAD_REQUEST, "Missing field", 'Missing "name" field for chatbot')

    if not name.islower():
        return http_error(HTTPStatus.BAD_REQUEST, "Wrong name", f"The name must be in lower case: {name}")

    model_name = chatbot.get("model_name", None)
    agent_name = chatbot.get("agent_name", None)
    if model_name is None and agent_name is None:
        return http_error(
            HTTPStatus.BAD_REQUEST,
            "Missing field",
            'Must include either "model_name" or "agent_name" field for chatbot',
        )

    session_controller = SessionController()

    if "database_id" in chatbot or "database_name" in chatbot or ("db_engine" in chatbot and "db_params" in chatbot):
        try:
            database_id = get_or_create_database_for_chatbot(chatbot, session_controller)
        except ValueError as value_error:
            return http_error(HTTPStatus.NOT_FOUND, "Database not found", str(value_error))

    else:
        return http_error(
            HTTPStatus.BAD_REQUEST,
            "Missing field",
            'Missing "database_id" or ("db_engine" and "database_param") fields for chatbot',
        )

    is_running = chatbot.get("is_running", False)
    params = chatbot.get("params", {})

    chatbot_controller = ChatBotController()

    # Chatbot can't already exist.
    # TODO all checks should be inside of controller

    try:
        existing_chatbot = chatbot_controller.get_chatbot(name, project_name=project_name)
    except EntityNotExistsError:
        # Project must exist.
        return http_error(HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist")
    if existing_chatbot is not None:
        return http_error(
            HTTPStatus.CONFLICT,
            "Chatbot already exists",
            f"Chatbot with name {name} already exists. Please use a different name",
        )

    # Model and agent need to exist.
    if agent_name is not None:
        agent = AgentsController().get_agent(agent_name, project_name)
        if agent is None:
            return http_error(HTTPStatus.NOT_FOUND, "Agent not found", f"Agent with name {agent_name} not found")
        model_name = agent.model_name

    model_name_no_version, version = Predictor.get_name_and_version(model_name)
    try:
        session_controller.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
    except PredictorRecordNotFound:
        return http_error(HTTPStatus.NOT_FOUND, "Model not found", f"Model with name {model_name} not found")

    created_chatbot = chatbot_controller.add_chatbot(
        name,
        project_name,
        model_name=model_name,
        agent_name=agent_name,
        database_id=database_id,
        is_running=is_running,
        params=params,
    )
    return created_chatbot.as_dict(), HTTPStatus.CREATED


def get_or_create_database_for_chatbot(chatbot: dict, session_controller: SessionController) -> int:
    """
    Get or create a database for a chatbot, based on the chatbot configuration provided in the request.

    Args:
        chatbot (dict): The chatbot configuration.
        session_controller (SessionController): The session controller.

    Returns:
        int: The database ID.
    """
    if "database_id" in chatbot:
        database_record = session_controller.integration_controller.get_by_id(chatbot["database_id"])
        if database_record:
            return database_record["id"]
        else:
            raise ValueError(f"Database with ID {chatbot['database_id']} not found")

    elif "database_name" in chatbot:
        database_record = session_controller.integration_controller.get(chatbot["database_name"])
        if database_record:
            return database_record["id"]
        else:
            raise ValueError(f"Database with name {chatbot['database_name']} not found")

    if "db_params" in chatbot and "db_engine" in chatbot:
        db_name = chatbot["name"] + "_db"

        # try to drop
        existing_db = session_controller.integration_controller.get(db_name)
        if existing_db:
            # drop
            session_controller.integration_controller.delete(db_name)

        return session_controller.integration_controller.add(db_name, chatbot["db_engine"], chatbot["db_params"])

    return None


@ns_conf.route("/<project_name>/chatbots")
class ChatBotsResource(Resource):
    @ns_conf.doc("list_chatbots")
    @api_endpoint_metrics("GET", "/chatbots")
    def get(self, project_name):
        """List all chatbots"""
        chatbot_controller = ChatBotController()
        try:
            all_bots = chatbot_controller.get_chatbots(project_name)
        except (ValueError, EntityNotExistsError):
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )
        return all_bots

    @ns_conf.doc("create_chatbot")
    @api_endpoint_metrics("POST", "/chatbots")
    def post(self, project_name):
        """Create a chatbot"""

        # Check for required parameters.
        if "chatbot" not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "chatbot" parameter in POST body'
            )

        chatbot = request.json["chatbot"]

        name = chatbot.get("name")
        return create_chatbot(project_name, name, chatbot)


@ns_conf.route("/<project_name>/chatbots/<chatbot_name>")
@ns_conf.param("project_name", "Name of the project")
@ns_conf.param("chatbot_name", "Name of the chatbot")
class ChatBotResource(Resource):
    @ns_conf.doc("get_chatbot")
    @api_endpoint_metrics("GET", "/chatbots/chatbot")
    def get(self, project_name, chatbot_name):
        """Gets a chatbot by name"""
        chatbot_controller = ChatBotController()
        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
            if existing_chatbot is None:
                return http_error(
                    HTTPStatus.NOT_FOUND, "Chatbot not found", f"Chatbot with name {chatbot_name} does not exist"
                )
            return existing_chatbot
        except (ValueError, EntityNotExistsError):
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

    @ns_conf.doc("update_chatbot")
    @api_endpoint_metrics("PUT", "/chatbots/chatbot")
    def put(self, project_name, chatbot_name):
        """Updates a chatbot by name, creating one if it doesn't exist"""

        # Check for required parameters.
        if "chatbot" not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "chatbot" parameter in POST body'
            )
        chatbot_controller = ChatBotController()

        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
        except EntityNotExistsError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        session = SessionController()

        chatbot = request.json["chatbot"]
        name = chatbot.get("name", None)
        agent_name = chatbot.get("agent_name", None)
        model_name = chatbot.get("model_name", None)
        try:
            database_id = get_or_create_database_for_chatbot(chatbot, session)
        except ValueError as value_error:
            return http_error(HTTPStatus.NOT_FOUND, "Database not found", str(value_error))
        is_running = chatbot.get("is_running", None)
        params = chatbot.get("params", None)

        # Model needs to exist.
        if model_name is not None:
            model_name_no_version, version = Predictor.get_name_and_version(model_name)
            try:
                session.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(HTTPStatus.NOT_FOUND, "Model not found", f"Model with name {model_name} not found")

        # Agent needs to exist.
        if agent_name is not None:
            agent = session.agents_controller.get_agent(agent_name, project_name)
            if agent is None:
                return http_error(HTTPStatus.NOT_FOUND, "Agent not found", f"Agent with name {agent_name} not found")

        # Chatbot must not exist with new name.
        if name is not None:
            if name != chatbot_name:
                chatbot_with_new_name = chatbot_controller.get_chatbot(name, project_name=project_name)
                if chatbot_with_new_name is not None:
                    return http_error(
                        HTTPStatus.CONFLICT,
                        "Chatbot already exists",
                        f"Chatbot with name {name} already exists. Please choose a different one.",
                    )
            if not name.islower():
                return http_error(HTTPStatus.BAD_REQUEST, "Wrong name", f"The name must be in lower case: {name}")

        if existing_chatbot is None:
            # Create
            return create_chatbot(project_name, name, chatbot)

        # Update
        updated_chatbot = chatbot_controller.update_chatbot(
            chatbot_name,
            project_name=project_name,
            name=name,
            model_name=model_name,
            agent_name=agent_name,
            database_id=database_id,
            is_running=is_running,
            params=params,
        )
        return updated_chatbot.as_dict()

    @ns_conf.doc("delete_chatbot")
    @api_endpoint_metrics("DELETE", "/chatbots/chatbot")
    def delete(self, project_name, chatbot_name):
        """Deletes a chatbot by name"""
        chatbot_controller = ChatBotController()
        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
            if existing_chatbot is None:
                return http_error(
                    HTTPStatus.NOT_FOUND, "Chatbot not found", f"Chatbot with name {chatbot_name} does not exist"
                )
        except EntityNotExistsError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND, "Project not found", f"Project with name {project_name} does not exist"
            )

        chatbot_controller.delete_chatbot(chatbot_name, project_name=project_name)
        return "", HTTPStatus.NO_CONTENT
