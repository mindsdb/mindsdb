from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.http.utils import http_error
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.utilities.exception import EntityNotExistsError


def create_skill(project_name, skill):
    skills_controller = SkillsController()
    for required_field in ['name', 'type', 'params']:
        if required_field not in skill:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing field',
                'Missing "{}" field for skill'.format(required_field)
            )
    name = skill['name']
    type = skill['type']
    params = skill['params']

    try:
        existing_skill = skills_controller.get_skill(name, project_name)
    except ValueError:
        # Project needs to exist
        return http_error(
            HTTPStatus.NOT_FOUND,
            'Project not found',
            f'Project with name {project_name} does not exist'
        )

    if existing_skill is not None:
        return http_error(
            HTTPStatus.CONFLICT,
            'Skill already exists',
            f'Skill with name {name} already exists. Please use a different name'
        )

    new_skill = skills_controller.add_skill(name, project_name, type, params)
    return new_skill.as_dict(), HTTPStatus.CREATED


@ns_conf.route('/<project_name>/skills')
class SkillsResource(Resource):
    @ns_conf.doc('list_skills')
    @api_endpoint_metrics('GET', '/skills')
    def get(self, project_name):
        ''' List all skills'''
        skills_controller = SkillsController()
        try:
            all_skills = skills_controller.get_skills(project_name)
        except EntityNotExistsError:
            # Project needs to exist.
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )
        return [skill.as_dict() for skill in all_skills]

    @ns_conf.doc('create_skill')
    @api_endpoint_metrics('POST', '/skills')
    def post(self, project_name):
        '''Create a skill'''

        # Check required request format.
        if 'skill' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "skill" parameter in POST body'
            )
        skill = request.json['skill']
        return create_skill(project_name, skill)


@ns_conf.route('/<project_name>/skills/<skill_name>')
@ns_conf.param('project_name', 'Name of the project')
@ns_conf.param('skill_name', 'Name of the skill')
class SkillResource(Resource):
    @ns_conf.doc('get_skill')
    @api_endpoint_metrics('GET', '/skills/skill')
    def get(self, project_name, skill_name):
        '''Gets a skill by name'''
        skills_controller = SkillsController()
        try:
            existing_skill = skills_controller.get_skill(skill_name, project_name)
        except EntityNotExistsError:
            # Project needs to exist
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        if existing_skill is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Skill not found',
                f'Skill with name {skill_name} not found.'
            )
        return existing_skill.as_dict()

    @ns_conf.doc('update_skill')
    @api_endpoint_metrics('PUT', '/skills/skill')
    def put(self, project_name, skill_name):
        '''Updates a skill by name, creating one if it doesn't exist'''
        skills_controller = SkillsController()

        # Check required request format.
        if 'skill' not in request.json:
            return http_error(
                HTTPStatus.BAD_REQUEST,
                'Missing parameter',
                'Must provide "skill" parameter in POST body'
            )

        try:
            existing_skill = skills_controller.get_skill(skill_name, project_name)
        except EntityNotExistsError:
            # Project needs to exist
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        skill = request.json['skill']
        if existing_skill is None:
            # Use same name as provided in URL.
            skill['name'] = skill_name
            return create_skill(project_name, skill)

        new_name = skill.get('name', None)
        new_type = skill.get('type', None)
        new_params = skill.get('params', None)
        updated_skill = skills_controller.update_skill(
            skill_name,
            new_name=new_name,
            project_name=project_name,
            type=new_type,
            params=new_params)
        return updated_skill.as_dict()

    @ns_conf.doc('delete_skill')
    @api_endpoint_metrics('DELETE', '/skills/skill')
    def delete(self, project_name, skill_name):
        '''Deletes a skill by name'''
        skills_controller = SkillsController()
        try:
            existing_skill = skills_controller.get_skill(skill_name, project_name)
        except EntityNotExistsError:
            # Project needs to exist
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Project not found',
                f'Project with name {project_name} does not exist'
            )

        if existing_skill is None:
            return http_error(
                HTTPStatus.NOT_FOUND,
                'Skill not found',
                f'Skill with name {skill_name} not found.'
            )
        skills_controller.delete_skill(skill_name, project_name)
        return '', HTTPStatus.NO_CONTENT
