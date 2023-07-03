from http import HTTPStatus

from flask import request
from flask_restx import Resource
from sqlalchemy.exc import NoResultFound

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.http.utils import http_error
from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.storage.db import Predictor


@ns_conf.route('/<project_name>/chatbots')
class ChatBotsResource(Resource):
    @ns_conf.doc('list_chatbots')
    def get(self, project_name):
        ''' List all chatbots '''
        chatbot_controller = ChatBotController()
        try:
            all_bots = chatbot_controller.get_chatbots(project_name)
        except NoResultFound:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist')
        return [b.as_dict() for b in all_bots]

    @ns_conf.doc('create_chatbot')
    def post(self, project_name):
        '''Create a chatbot'''

        # Check for required parameters.
        if 'chatbot' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "chatbot" parameter in POST body'
            )

        chatbot = request.json['chatbot']
        if 'name' not in chatbot:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing field',
                'Missing "name" field for chatbot'
            )
        if 'model_name' not in chatbot:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing field',
                'Missing "model_name" field for chatbot'
            )
        if 'database_id' not in chatbot and 'chat_engine' not in chatbot:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing field',
                'Must provide either "database_id" or "chat_engine" when creating a chatbot'
            )

        name = chatbot['name']
        model_name = chatbot['model_name']
        database_id = chatbot.get('database_id', None)
        chat_engine = chatbot.get('chat_engine', None)
        is_running = chatbot.get('is_running', False)
        params = chatbot.get('params', {})

        chatbot_controller = ChatBotController()
        session_controller = SessionController()

        # Chatbot can't already exist.
        try:
            existing_chatbot = chatbot_controller.get_chatbot(name, project_name=project_name)
            if existing_chatbot is not None:
                return http_error(
                    HTTPStatus.CONFLICT,
                    'Chatbot already exists',
                    f'Chatbot with name {name} already exists. Please use a different name'
                )
        except NoResultFound:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        # Model needs to exist.
        model_name_no_version, version = Predictor.get_name_and_version(model_name)
        try:
            session_controller.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {model_name} not found')

        created_chatbot = chatbot_controller.add_chatbot(
            name,
            project_name,
            model_name,
            database_id=database_id,
            chat_engine=chat_engine,
            is_running=is_running,
            params=params
        )
        return created_chatbot.as_dict(), HTTPStatus.CREATED


@ns_conf.route('/<project_name>/chatbots/<chatbot_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('chatbot_name', 'Name of the chatbot')
class ChatBotResource(Resource):
    @ns_conf.doc('get_chatbot')
    def get(self, project_name, chatbot_name):
        '''Gets a chatbot by name'''
        chatbot_controller = ChatBotController()
        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
            if existing_chatbot is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Chatbot not found',
                    f'Chatbot with name {chatbot_name} does not exist'
                )
            return existing_chatbot.as_dict()
        except NoResultFound:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

    @ns_conf.doc('update_chatbot')
    def put(self, project_name, chatbot_name):
        '''Updates a chatbot by name, creating one if it doesn't exist'''

        # Check for required parameters.
        if 'chatbot' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "chatbot" parameter in POST body'
            )
        chatbot_controller = ChatBotController()
        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
        except NoResultFound:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        chatbot = request.json['chatbot']
        name = chatbot.get('name', None)
        model_name = chatbot.get('model_name', None)
        database_id = chatbot.get('database_id', None)
        chat_engine = chatbot.get('chat_engine', None)
        is_running = chatbot.get('is_running', None)
        params = chatbot.get('params', None)

        # Model needs to exist.
        if model_name is not None:
            session_controller = SessionController()
            model_name_no_version, version = Predictor.get_name_and_version(model_name)
            try:
                session_controller.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model_name} not found')

        # Chatbot must not exist with new name.
        if name is not None and name != chatbot_name:
            chatbot_with_new_name = chatbot_controller.get_chatbot(name, project_name=project_name)
            if chatbot_with_new_name is not None:
                return http_error(
                    HTTPStatus.CONFLICT,
                    'Chatbot already exists',
                    f'Chatbot with name {name} already exists. Please choose a different one.'
                )

        if existing_chatbot is None:
            # Create
            if name is None:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing field',
                    'Missing "name" field for chatbot'
                )
            if model_name is None:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing field',
                    'Missing "model_name" field for chatbot'
                )
            if database_id is None and chat_engine is None:
                return http_error(
                    HTTPStatus.BAD_REQUEST,
                    'Missing field',
                    'Must provide either "database_id" or "chat_engine" when creating a chatbot'
                )

            created_chatbot = chatbot_controller.add_chatbot(
                name,
                project_name,
                model_name,
                database_id=database_id,
                chat_engine=chat_engine,
                is_running=is_running,
                params=params if params else {}
            )
            return created_chatbot.as_dict(), HTTPStatus.CREATED

        # Update
        updated_chatbot = chatbot_controller.update_chatbot(
            chatbot_name,
            project_name=project_name,
            name=name,
            model_name=model_name,
            database_id=database_id,
            chat_engine=chat_engine,
            is_running=is_running,
            params=params
        )
        return updated_chatbot.as_dict()

    @ns_conf.doc('delete_chatbot')
    def delete(self, project_name, chatbot_name):
        '''Deletes a chatbot by name'''
        chatbot_controller = ChatBotController()
        try:
            existing_chatbot = chatbot_controller.get_chatbot(chatbot_name, project_name=project_name)
            if existing_chatbot is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Chatbot not found',
                    f'Chatbot with name {chatbot_name} does not exist'
                )
        except NoResultFound:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        chatbot_controller.delete_chatbot(chatbot_name, project_name=project_name)
        return '', HTTPStatus.NO_CONTENT
