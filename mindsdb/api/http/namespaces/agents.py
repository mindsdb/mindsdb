from http import HTTPStatus
import os

from flask import request
from flask_restx import Resource
from langfuse import Langfuse

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.storage import db


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
    params = agent.get('params', {})
    skills = agent.get('skills', [])

    session = SessionController()

    try:
        existing_agent = session.agents_controller.get_agent(name, project_name=project_name)
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
        created_agent = session.agents_controller.add_agent(
            name,
            project_name,
            model_name,
            skills,
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
        '''Gets a agent by name'''
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
        '''Updates a agent by name, creating one if it doesn't exist'''

        # Check for required parameters.
        if 'agent' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "agent" parameter in POST body'
            )
        session = SessionController()

        try:
            existing_agent = session.agents_controller.get_agent(agent_name, project_name=project_name)
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
        params = agent.get('params', None)

        # Model needs to exist.
        if model_name is not None:
            session_controller = SessionController()
            model_name_no_version, version = db.Predictor.get_name_and_version(model_name)
            try:
                session_controller.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model_name} not found'
                )

        # Agent must not exist with new name.
        if name is not None and name != agent_name:
            agent_with_new_name = session.agents_controller.get_agent(name, project_name=project_name)
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
            updated_agent = session.agents_controller.update_agent(
                agent_name,
                project_name=project_name,
                name=name,
                model_name=model_name,
                skills_to_add=skills_to_add,
                skills_to_remove=skills_to_remove,
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

        session.agents_controller.delete_agent(agent_name, project_name=project_name)
        return '', HTTPStatus.NO_CONTENT


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

        # Model needs to exist.
        model_name_no_version, version = db.Predictor.get_name_and_version(existing_agent.model_name)
        try:
            agent_model = session.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
            agent_model_record = db.Predictor.query.get(agent_model['id'])
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {existing_agent.model_name} not found'
            )

        trace_id = None
        observation_id = None
        run_completion_span = None
        # Trace Agent completions using Langfuse if configured.
        if os.getenv('LANGFUSE_PUBLIC_KEY') is not None:
            langfuse = Langfuse(
                public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
                secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
                host=os.getenv('LANGFUSE_HOST')
            )
            api_trace = langfuse.trace(name='api-completion')
            run_completion_span = api_trace.span(name='run-completion')
            trace_id = api_trace.id
            observation_id = run_completion_span.id

        completion = session.agents_controller.get_completion(
            existing_agent,
            request.json['messages'],
            trace_id=trace_id,
            observation_id=observation_id,
            project_name=project_name,
            # Don't need to include backoffice_db related tools into this endpoint.
            # Underlying handler (e.g. Langchain) will handle default tools like mdb_read, mdb_write, etc.
            tools=[]
        )

        if run_completion_span is not None:
            run_completion_span.end()

        output_col = agent_model_record.to_predict[0]
        model_output = completion.iloc[-1][output_col]
        return {
            'message': {
                'content': model_output,
                'role': 'assistant'
            }
        }
