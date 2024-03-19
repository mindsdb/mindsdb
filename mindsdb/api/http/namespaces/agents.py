from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.interfaces.agents.agents_controller import AgentsController
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
            name,
            project_name,
            model_name,
            skills,
            params=params
        )
        return created_agent.as_dict(), HTTPStatus.CREATED
    except ValueError as e:
        # Model or skill doesn't exist.
        return http_error(
            HTTPStatus.NOT_FOUND,
            'Resource not found',
            str(e)
        )
    except NotImplementedError as e:
        # Free users trying to create agent.
        return http_error(
            HTTPStatus.UNAUTHORIZED,
            'Unavailable to free users',
            str(e)
        )


@ns_conf.route('/<project_name>/agents')
class AgentsResource(Resource):
    @ns_conf.doc('list_agents')
    @api_endpoint_metrics('GET', '/agents')
    def get(self, project_name):
        ''' List all agents '''
        agents_controller = AgentsController()
        try:
            all_agents = agents_controller.get_agents(project_name)
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
        agents_controller = AgentsController()
        try:
            existing_agent = agents_controller.get_agent(agent_name, project_name=project_name)
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

        # Model needs to exist.
        session_controller = SessionController()
        model_name_no_version, version = db.Predictor.get_name_and_version(existing_agent.model_name)
        try:
            agent_model = session_controller.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
            agent_model_record = db.Predictor.query.get(agent_model['id'])
        except PredictorRecordNotFound:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Model not found',
                f'Model with name {existing_agent.model_name} not found'
            )

        predict_params = {
            # Don't need to include backoffice_db related tools into this endpoint.
            # Underlying handler (e.g. Langchain) will handle default tools like mdb_read, mdb_write, etc.
            'tools': [],
            'skills': [s for s in existing_agent.skills],
        }
        project_datanode = session_controller.datahub.get(project_name)
        predictions = project_datanode.predict(
            model_name=existing_agent.model_name,
            data=request.json['messages'],
            params=predict_params
        )

        output_col = agent_model_record.to_predict[0]
        model_output = predictions.iloc[-1][output_col]
        return {
            'message': {
                'content': model_output,
                'role': 'assistant'
            }
        }
