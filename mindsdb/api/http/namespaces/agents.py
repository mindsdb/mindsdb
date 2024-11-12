import traceback
from http import HTTPStatus
from typing import Dict, Iterable, List
import json
import os

from flask import request, Response
from flask_restx import Resource

from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.storage import db
from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities.log import getLogger
from mindsdb.utilities.exception import EntityNotExistsError


logger = getLogger(__name__)


AGENT_QUICK_RESPONSE = "I understand your request. I'm working on a detailed response for you."


def create_agent(project_name, name, agent):
    if name is None:
        return http_error(
            HTTPStatus.BAD_REQUEST,
            'Missing field',
            'Missing "name" field for agent'
        )

    if 'model_name' not in agent:
        return http_error(
            HTTPStatus.BAD_REQUEST,
            'Missing field',
            'Missing "model_name" field for agent'
        )

    model_name = agent['model_name']
    provider = agent.get('provider')
    params = agent.get('params', {})
    skills = agent.get('skills', [])

    agents_controller = AgentsController()

    try:
        existing_agent = agents_controller.get_agent(name, project_name=project_name)
    except ValueError:
        # Project must exist.
        return http_error(
            HTTPStatus.NOT_FOUND,
            'Project not found',
            f'Project with name {project_name} does not exist'
        )
    if existing_agent is not None:
        return http_error(
            HTTPStatus.CONFLICT,
            'Agent already exists',
            f'Agent with name {name} already exists. Please choose a different one.'
        )

    try:
        created_agent = agents_controller.add_agent(
            name=name,
            project_name=project_name,
            model_name=model_name,
            skills=skills,
            provider=provider,
            params=params
        )
        return created_agent.as_dict(), HTTPStatus.CREATED
    except ValueError:
        # Model or skill doesn't exist.
        return http_error(
            HTTPStatus.NOT_FOUND,
            'Resource not found',
            f'The model "{model_name}" or skills "{skills}" do not exist. Please ensure that the names are correct and try again.'
        )
    except NotImplementedError:
        # Free users trying to create agent.
        return http_error(
            HTTPStatus.UNAUTHORIZED,
            'Unavailable to free users',
            f'The model "{model_name}" or skills "{skills}" do not exist. Please ensure that the names are correct and try again.'
        )


@ns_conf.route('/<project_name>/agents')
class AgentsResource(Resource):
    @ns_conf.doc('list_agents')
    @api_endpoint_metrics('GET', '/agents')
    def get(self, project_name):
        ''' List all agents '''
        session = SessionController()
        try:
            all_agents = session.agents_controller.get_agents(project_name)
        except EntityNotExistsError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist')
        return [a.as_dict() for a in all_agents]

    @ns_conf.doc('create_agent')
    @api_endpoint_metrics('POST', '/agents')
    def post(self, project_name):
        '''Create a agent'''

        # Check for required parameters.
        if 'agent' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "agent" parameter in POST body'
            )

        agent = request.json['agent']

        name = agent.get('name')
        return create_agent(project_name, name, agent)


@ns_conf.route('/<project_name>/agents/<agent_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('agent_name', 'Name of the agent')
class AgentResource(Resource):
    @ns_conf.doc('get_agent')
    @api_endpoint_metrics('GET', '/agents/agent')
    def get(self, project_name, agent_name):
        '''Gets an agent by name'''
        session = SessionController()
        try:
            existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
            if existing_agent is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Agent not found',
                    f'Agent with name {agent_name} does not exist'
                )
            return existing_agent.as_dict()
        except ValueError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

    @ns_conf.doc('update_agent')
    @api_endpoint_metrics('PUT', '/agents/agent')
    def put(self, project_name, agent_name):
        '''Updates an agent by name, creating one if it doesn't exist'''

        # Check for required parameters.
        if 'agent' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "agent" parameter in POST body'
            )
        agents_controller = AgentsController()

        try:
            existing_agent = agents_controller.get_agent(agent_name, project_name=project_name)
        except ValueError:
            # Project must exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        agent = request.json['agent']
        name = agent.get('name', None)
        model_name = agent.get('model_name', None)
        skills_to_add = agent.get('skills_to_add', [])
        skills_to_remove = agent.get('skills_to_remove', [])
        provider = agent.get('provider')
        params = agent.get('params', None)

        # Agent must not exist with new name.
        if name is not None and name != agent_name:
            agent_with_new_name = agents_controller.get_agent(name, project_name=project_name)
            if agent_with_new_name is not None:
                return http_error(
                    HTTPStatus.CONFLICT,
                    'Agent already exists',
                    f'Agent with name {name} already exists. Please choose a different one.'
                )

        if existing_agent is None:
            # Create
            return create_agent(project_name, name, agent)

        # Update
        try:
            # Prepare the params dictionary
            if params is None:
                params = {}

            # Check if any of the skills to be added is of type 'retrieval'
            session = SessionController()
            skills_controller = session.skills_controller
            retrieval_skill_added = any(
                skills_controller.get_skill(skill_name).type == 'retrieval'
                for skill_name in skills_to_add
                if skills_controller.get_skill(skill_name) is not None
            )

            if retrieval_skill_added and 'mode' not in params:
                params['mode'] = 'retrieval'

            updated_agent = agents_controller.update_agent(
                agent_name,
                project_name=project_name,
                name=name,
                model_name=model_name,
                skills_to_add=skills_to_add,
                skills_to_remove=skills_to_remove,
                provider=provider,
                params=params
            )

            return updated_agent.as_dict()
        except ValueError as e:
            # Model or skill doesn't exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Resource not found',
                str(e)
            )

    @ns_conf.doc('delete_agent')
    @api_endpoint_metrics('DELETE', '/agents/agent')
    def delete(self, project_name, agent_name):
        '''Deletes a agent by name'''
        agents_controller = AgentsController()

        try:
            existing_agent = agents_controller.get_agent(agent_name, project_name=project_name)
            if existing_agent is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Agent not found',
                    f'Agent with name {agent_name} does not exist'
                )
        except ValueError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        agents_controller.delete_agent(agent_name, project_name=project_name)
        return '', HTTPStatus.NO_CONTENT


def _completion_event_generator(
        agent_name: str,
        messages: List[Dict],
        project_name: str) -> Iterable[str]:
    logger.info(f"Starting completion event generator for agent {agent_name}")

    def json_serialize(data):
        return f'data: {json.dumps(data)}\n\n'

    quick_response_message = {
        'role': 'assistant',
        'content': AGENT_QUICK_RESPONSE
    }
    yield json_serialize({"quick_response": True, "messages": [quick_response_message]})
    logger.info("Quick response sent")

    try:
        # Populate API key by default if not present.
        session = SessionController()
        existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
        if not existing_agent.params:
            existing_agent.params = {}
        existing_agent.params['openai_api_key'] = existing_agent.params.get('openai_api_key',
                                                                            os.getenv('OPENAI_API_KEY'))
        # Have to commit/flush here so DB isn't locked while streaming.
        db.session.commit()

        if 'mode' not in existing_agent.params and any(skill.type == 'retrieval' for skill in existing_agent.skills):
            existing_agent.params['mode'] = 'retrieval'

        completion_stream = session.agents_controller.get_completion(
            existing_agent,
            messages,
            project_name=project_name,
            tools=[],
            stream=True
        )

        for chunk in completion_stream:
            if isinstance(chunk, str) and chunk.startswith('data: '):
                # The chunk is already formatted correctly, yield it as is
                yield chunk
            elif isinstance(chunk, dict):
                if 'error' in chunk:
                    # Handle error chunks
                    logger.error(f"Error in completion stream: {chunk['error']}")
                    yield json_serialize({"error": chunk['error']})
                elif chunk.get('type') == 'context':
                    # Handle context message
                    yield json_serialize({"type": "context", "content": chunk.get('content')})
                elif chunk.get('type') == 'sql':
                    # Handle SQL query message
                    yield json_serialize({"type": "sql", "content": chunk.get('content')})
                else:
                    # Chunk should already be formatted by agent stream.
                    yield json_serialize(chunk)
            else:
                # For any other unexpected chunk types
                yield json_serialize({"output": str(chunk)})

            logger.debug(f"Streamed chunk: {str(chunk)[:100]}...")

        logger.info("Completion stream finished")

    except Exception as e:
        error_message = f"Error in completion event generator: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        yield json_serialize({"error": error_message})

    finally:
        yield json_serialize({"type": "end"})


@ns_conf.route('/<project_name>/agents/<agent_name>/completions/stream')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('agent_name', 'Name of the agent')
class AgentCompletionsStream(Resource):
    @ns_conf.doc('agent_completions_stream')
    @api_endpoint_metrics('POST', '/agents/agent/completions/stream')
    def post(self, project_name, agent_name):
        logger.info(f"Received streaming request for agent {agent_name} in project {project_name}")

        # Check for required parameters.
        if 'messages' not in request.json:
            logger.error("Missing 'messages' parameter in request body")
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "messages" parameter in POST body'
            )

        session = SessionController()
        try:
            existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
            if existing_agent is None:
                logger.error(f"Agent {agent_name} not found in project {project_name}")
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Agent not found',
                    f'Agent with name {agent_name} does not exist'
                )
        except ValueError as e:
            logger.error(f"Project {project_name} not found: {str(e)}")
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        messages = request.json['messages']

        try:
            gen = _completion_event_generator(
                agent_name,
                messages,
                project_name
            )
            logger.info(f"Starting streaming response for agent {agent_name}")
            return Response(gen, mimetype='text/event-stream')
        except Exception as e:
            logger.error(f"Error during streaming for agent {agent_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return http_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Streaming error',
                f'An error occurred during streaming: {str(e)}'
            )


@ns_conf.route('/<project_name>/agents/<agent_name>/completions')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('agent_name', 'Name of the agent')
class AgentCompletions(Resource):
    @ns_conf.doc('agent_completions')
    @api_endpoint_metrics('POST', '/agents/agent/completions')
    def post(self, project_name, agent_name):
        '''Queries an agent given a list of messages'''
        # Check for required parameters.
        if 'messages' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "messages" parameter in POST body'
            )
        agents_controller = AgentsController()

        try:
            existing_agent = agents_controller.get_agent(agent_name, project_name=project_name)
            if existing_agent is None:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Agent not found',
                    f'Agent with name {agent_name} does not exist'
                )
        except ValueError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        # Add OpenAI API key to agent params if not already present.
        if not existing_agent.params:
            existing_agent.params = {}
        existing_agent.params['openai_api_key'] = existing_agent.params.get('openai_api_key', os.getenv('OPENAI_API_KEY'))

        # set mode to `retrieval` if agent has a skill of type `retrieval` and mode is not set
        if 'mode' not in existing_agent.params and any(skill.type == 'retrieval' for skill in existing_agent.skills):
            existing_agent.params['mode'] = 'retrieval'

        messages = request.json['messages']

        completion = agents_controller.get_completion(
            existing_agent,
            messages,
            project_name=project_name,
            # Don't need to include backoffice_db related tools into this endpoint.
            # Underlying handler (e.g. Langchain) will handle default tools like mdb_read, mdb_write, etc.
            tools=[]
        )

        output_col = agents_controller.assistant_column
        model_output = completion.iloc[-1][output_col]

        response = {
            'message': {
                'content': model_output,
                'role': 'assistant'
            }
        }

        if existing_agent.params.get('return_context', False):
            context = []
            if 'context' in completion.columns:
                try:
                    last_context = completion.iloc[-1]['context']
                    if last_context:
                        context = json.loads(last_context)
                except (json.JSONDecodeError, IndexError) as e:
                    logger.error(f'Error decoding context: {e}')
                    pass  # Keeping context as an empty list in case of error

            response['message']['context'] = context

        return response
