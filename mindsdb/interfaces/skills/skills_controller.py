import datetime
from typing import Dict, List

from sqlalchemy import null
from sqlalchemy.orm.attributes import flag_modified

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController


class SkillsController:
    '''Handles CRUD operations at the database level for Skills'''

    def __init__(self, project_controller: ProjectController = None):
        if project_controller is None:
            project_controller = ProjectController()
        self.project_controller = project_controller

    def get_skill(self, skill_name: str, project_name: str = 'mindsdb') -> db.Skills:
        '''
        Gets a skill by name. Skills are expected to have unique names.

        Parameters:
            skill_name (str): The name of the skill
            project_name (str): The name of the containing project

        Returns:
            skill (db.Skills): The database skill object

        Raises:
            ValueError: If `project_name` does not exist
        '''

        project = self.project_controller.get(name=project_name)
        return db.Skills.query.filter(
            db.Skills.name == skill_name,
            db.Skills.project_id == project.id,
            db.Skills.deleted_at == null()
        ).first()

    def get_skills(self, project_name: str) -> List[dict]:
        '''
        Gets all skills in a project.

        Parameters:
            project_name (str): The name of the containing project

        Returns:
            all_skills (List[db.Skills]): List of database skill object

        Raises:
            ValueError: If `project_name` does not exist
        '''

        project_controller = ProjectController()
        projects = project_controller.get_list()
        if project_name is not None:
            projects = list([p for p in projects if p.name == project_name])
        project_ids = list([p.id for p in projects])

        query = (
            db.session.query(db.Skills)
            .filter(
                db.Skills.project_id.in_(project_ids),
                db.Skills.deleted_at == null()
            )
        )

        return query.all()

    def add_skill(
            self,
            name: str,
            project_name: str,
            type: str,
            params: Dict[str, str] = {}) -> db.Skills:
        '''
        Adds a skill to the database.

        Parameters:
            name (str): The name of the new skill
            project_name (str): The containing project
            type (str): The type of the skill (e.g. Knowledge Base)
            params: (Dict[str, str]): Parameters associated with the skill

        Returns:
            bot (db.Skills): The created skill

        Raises:
            ValueError: If `project_name` does not exist or skill already exists
        '''
        if project_name is None:
            project_name = 'mindsdb'
        project = self.project_controller.get(name=project_name)

        skill = self.get_skill(name, project_name)

        if skill is not None:
            raise ValueError(f'Skill with name already exists: {name}')

        new_skill = db.Skills(
            name=name,
            project_id=project.id,
            type=type,
            params=params,
        )
        db.session.add(new_skill)
        db.session.commit()

        return new_skill

    def update_skill(
            self,
            skill_name: str,
            new_name: str = None,
            project_name: str = 'mindsdb',
            type: str = None,
            params: Dict[str, str] = None):
        '''
        Updates an existing skill in the database.

        Parameters:
            skill_name (str): The name of the new skill, or existing skill to update
            new_name (str): Updated name of the skill
            project_name (str): The containing project
            type (str): The type of the skill (e.g. Knowledge Base)
            params: (Dict[str, str]): Parameters associated with the skill

        Returns:
            bot (db.Skills): The updated skill

        Raises:
            ValueError: If `project_name` does not exist or skill doesn't exist
        '''

        existing_skill = self.get_skill(skill_name, project_name)
        if existing_skill is None:
            raise ValueError(f'Skill with name not found: {skill_name}')

        if new_name is not None:
            existing_skill.name = new_name
        if type is not None:
            existing_skill.type = type
        if params is not None:
            # Merge params on update
            existing_params = {} if not existing_skill.params else existing_skill.params
            existing_params.update(params)
            # Remove None values entirely.
            params = {k: v for k, v in existing_params.items() if v is not None}
            existing_skill.params = params
            # Some versions of SQL Alchemy won't handle JSON updates correctly without this.
            # See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.attributes.flag_modified
            flag_modified(existing_skill, 'params')

        db.session.commit()

        return existing_skill

    def delete_skill(self, skill_name: str, project_name: str = 'mindsdb'):
        '''
        Deletes a skill by name.

        Parameters:
            skill_name (str): The name of the skill to delete
            project_name (str): The name of the containing project

        Raises:
            ValueError: If `project_name` does not exist or skill doesn't exist
        '''

        skill = self.get_skill(skill_name, project_name)
        if skill is None:
            raise ValueError(f"Skill with name doesn't exist: {skill_name}")
        skill.deleted_at = datetime.datetime.now()
        db.session.commit()
