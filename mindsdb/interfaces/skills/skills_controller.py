import datetime
from typing import Dict, List, Optional

from sqlalchemy import null, func
from sqlalchemy.orm.attributes import flag_modified

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.config import config
from mindsdb.utilities import log


logger = log.getLogger(__name__)

default_project = config.get("default_project")


class SkillsController:
    """Handles CRUD operations at the database level for Skills"""

    def __init__(self, project_controller: ProjectController = None):
        if project_controller is None:
            project_controller = ProjectController()
        self.project_controller = project_controller

    def get_skill(
        self, skill_name: str, project_name: str = default_project, strict_case: bool = False
    ) -> Optional[db.Skills]:
        """
        Gets a skill by name. Skills are expected to have unique names.

        Parameters:
            skill_name (str): The name of the skill
            project_name (str): The name of the containing project
            strict_case (bool): If True, the skill name is case-sensitive. Defaults to False.

        Returns:
            skill (Optional[db.Skills]): The database skill object

        Raises:
            ValueError: If `project_name` does not exist
        """

        project = self.project_controller.get(name=project_name)
        query = db.Skills.query.filter(
            db.Skills.project_id == project.id,
            db.Skills.deleted_at == null(),
        )
        if strict_case:
            query = query.filter(db.Skills.name == skill_name)
        else:
            query = query.filter(func.lower(db.Skills.name) == func.lower(skill_name))

        return query.first()

    def get_skills(self, project_name: Optional[str]) -> List[dict]:
        """
        Gets all skills in a project.

        Parameters:
            project_name (Optional[str]): The name of the containing project

        Returns:
            all_skills (List[db.Skills]): List of database skill object

        Raises:
            ValueError: If `project_name` does not exist
        """

        if project_name is None:
            projects = self.project_controller.get_list()
            project_ids = list([p.id for p in projects])
        else:
            project = self.project_controller.get(name=project_name)
            project_ids = [project.id]

        query = db.session.query(db.Skills).filter(
            db.Skills.project_id.in_(project_ids), db.Skills.deleted_at == null()
        )

        return query.all()

    def add_skill(self, name: str, project_name: str, type: str, params: Dict[str, str] = {}) -> db.Skills:
        """
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
        """
        if project_name is None:
            project_name = default_project
        project = self.project_controller.get(name=project_name)

        if not name.islower():
            raise ValueError(f"The name must be in lower case: {name}")

        skill = self.get_skill(name, project_name)

        if skill is not None:
            raise ValueError(f"Skill with name already exists: {name}")

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
        project_name: str = default_project,
        type: str = None,
        params: Dict[str, str] = None,
    ):
        """
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
        """

        existing_skill = self.get_skill(skill_name, project_name)
        if existing_skill is None:
            raise ValueError(f"Skill with name not found: {skill_name}")
        if isinstance(existing_skill.params, dict) and existing_skill.params.get("is_demo") is True:
            raise ValueError("It is forbidden to change properties of the demo object")

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
            flag_modified(existing_skill, "params")

        db.session.commit()

        return existing_skill

    def delete_skill(self, skill_name: str, project_name: str = default_project, strict_case: bool = False):
        """
        Deletes a skill by name.

        Parameters:
            skill_name (str): The name of the skill to delete
            project_name (str): The name of the containing project
            strict_case (bool): If true, then skill_name is case sensitive

        Raises:
            ValueError: If `project_name` does not exist or skill doesn't exist
        """

        skill = self.get_skill(skill_name, project_name, strict_case)
        if skill is None:
            raise ValueError(f"Skill with name doesn't exist: {skill_name}")
        if isinstance(skill.params, dict) and skill.params.get("is_demo") is True:
            raise ValueError("Unable to delete demo object")
        skill.deleted_at = datetime.datetime.now()
        db.session.commit()
