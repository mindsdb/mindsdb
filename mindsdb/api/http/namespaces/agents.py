from http import HTTPStatus
from typing import Dict, Iterable, List
import json
import os

from flask import request, Response
from flask_restx import Resource
from langfuse import Langfuse

from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.agents.langfuse_callback_handler import get_metadata, get_tags, get_tool_usage, get_skills
from mindsdb.interfaces.storage import db

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities.log import getLogger

logger = getLogger(__name__)


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
            f'The {model_name} or skills "{skills}" do not exist. Please ensure that the names are correct and try again.'
        )
    except NotImplementedError:
        # Free users trying to create agent.
        return http_error(
            HTTPStatus.UNAUTHORIZED,
            'Unavailable to free users',
            f'The {model_name} or skills "{skills}" do not exist. Please ensure that the names are correct and try again.'
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
        except ValueError:
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
        trace_id: str,
        observation_id: str,
        project_name: str,
        run_completion_span,
        api_trace) -> Iterable[str]:
    # Populate API key by default if not present.
    session = SessionController()
    existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
    if not existing_agent.params:
        existing_agent.params = {}
    existing_agent.params['openai_api_key'] = existing_agent.params.get('openai_api_key', os.getenv('OPENAI_API_KEY'))
    # Have to commit/flush here so DB isn't locked while streaming.
    db.session.commit()

    completion_stream = session.agents_controller.get_completion(
        existing_agent,
        messages,
        trace_id=trace_id,
        observation_id=observation_id,
        project_name=project_name,
        tools=[],
        stream=True
    )
    last_output = None
    for chunk in completion_stream:
        chunk_obj = {}
        if 'output' in chunk:
            # Langchain final output.
            chunk_obj['output'] = chunk['output']
        if 'messages' in chunk:
            # Langchain messages in output/actions.
            chunk_obj['messages'] = [{'content': m.content} for m in chunk['messages']]
        if 'actions' in chunk:
            # Langchain actions.
            chunk_obj['actions'] = [{
                'tool': a.tool,
                'tool_input': a.tool_input,
                'log': a.log
            } for a in chunk['actions']]
        if 'steps' in chunk:
            # Langchain steps (similar to actions).
            chunk_obj['steps'] = [{'observation': s.observation} for s in chunk['steps']]
        chunk_str = json.dumps(chunk_obj)
        # Stream parsed & formatted Langchain streaming chunk.
        yield 'data: {}\n\n'.format(chunk_str)
        if 'output' in chunk:
            last_output = chunk_obj
    if run_completion_span is not None and api_trace is not None:
        run_completion_span.end(output=last_output)
        api_trace.update(output=last_output)


@ns_conf.route('/<project_name>/agents/<agent_name>/completions/stream')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('agent_name', 'Name of the agent')
class AgentCompletionsStream(Resource):
    @ns_conf.doc('agent_completions_stream')
    @api_endpoint_metrics('POST', '/agents/agent/completions/stream')
    def post(self, project_name, agent_name):
        # Check for required parameters.
        if 'messages' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "messages" parameter in POST body'
            )
        session = SessionController()
        try:
            existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
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

        trace_id = None
        observation_id = None
        api_trace = None
        run_completion_span = None
        messages = request.json['messages']
        # Trace Agent completions using Langfuse if configured.
        if os.getenv('LANGFUSE_PUBLIC_KEY') is not None:
            langfuse = Langfuse(
                public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
                secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
                host=os.getenv('LANGFUSE_HOST')
            )
            api_trace = langfuse.trace(
                name='api-completion',
                input=messages,
                tags=[os.getenv('FLASK_ENV', 'unknown')]
            )
            run_completion_span = api_trace.span(name='run-completion', input=messages)
            trace_id = api_trace.id
            observation_id = run_completion_span.id

        gen = _completion_event_generator(
            agent_name,
            messages,
            trace_id,
            observation_id,
            project_name,
            run_completion_span,
            api_trace
        )
        return Response(gen, mimetype='text/event-stream')


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

        trace_id = None
        observation_id = None
        api_trace = None
        run_completion_span = None
        messages = request.json['messages']
        # Trace Agent completions using Langfuse if configured.
        if os.getenv('LANGFUSE_PUBLIC_KEY') is not None:

            # todo we need to fix this as this assumes that the model is always langchain
            # since decoupling the model from langchain, we need to find a way to get the model name
            # this breaks retrieval agents

            # get model details
            session = SessionController()
            model_name_no_version, version = db.Predictor.get_name_and_version(existing_agent.model_name)
            agent_model = session.model_controller.get_model(model_name_no_version, version=version,
                                                             project_name=project_name)  # noqa
            model_using = agent_model.get('problem_definition', {}).get('using', {})

            # metadata retrieval
            trace_metadata = get_metadata(model_using)
            trace_metadata['skills'] = get_skills(existing_agent)
            trace_tags = get_tags(trace_metadata)

            langfuse = Langfuse(
                public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
                secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
                host=os.getenv('LANGFUSE_HOST')
            )
            api_trace = langfuse.trace(
                name='api-completion',
                input=messages,
                tags=trace_tags,
                metadata=trace_metadata
            )
            run_completion_span = api_trace.span(name='run-completion', input=messages)
            trace_id = api_trace.id
            observation_id = run_completion_span.id

        completion = agents_controller.get_completion(
            existing_agent,
            messages,
            trace_id=trace_id,
            observation_id=observation_id,
            project_name=project_name,
            # Don't need to include backoffice_db related tools into this endpoint.
            # Underlying handler (e.g. Langchain) will handle default tools like mdb_read, mdb_write, etc.
            tools=[]
        )

        output_col = agents_controller.assistant_column
        model_output = completion.iloc[-1][output_col]
        if run_completion_span is not None and api_trace is not None:
            run_completion_span.end(output=model_output)
            api_trace.update(output=model_output)

            # update metadata with tool usage
            trace = langfuse.get_trace(trace_id)
            trace_metadata['tool_usage'] = get_tool_usage(trace)
            api_trace.update(metadata=trace_metadata)

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
