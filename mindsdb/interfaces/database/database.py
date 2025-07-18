from typing import Optional
from collections import OrderedDict

from mindsdb.interfaces.database.projects import ProjectController
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.config import config
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.database.log import LogDBController


class DatabaseController:
    def __init__(self):
        from mindsdb.interfaces.database.integrations import integration_controller

        self.integration_controller = integration_controller
        self.project_controller = ProjectController()

        self.logs_db_controller = LogDBController()
        self.information_schema_controller = None

    def delete(self, name: str, strict_case: bool = False) -> None:
        """Delete a database (project or integration) by name.

        Args:
            name (str): The name of the database to delete.
            strict_case (bool, optional): If True, the database name is case-sensitive. Defaults to False.

        Raises:
            EntityNotExistsError: If the database does not exist.
            Exception: If the database cannot be deleted.

        Returns:
            None
        """
        databases = self.get_dict()
        if name.lower() not in databases:
            raise EntityNotExistsError("Database does not exists", name)
        db_type = databases[name.lower()]["type"]
        if db_type == "project":
            project = self.get_project(name, strict_case)
            project.delete()
            return
        elif db_type == "data":
            self.integration_controller.delete(name, strict_case)
            return
        else:
            raise Exception(f"Database with type '{db_type}' cannot be deleted")

    @profiler.profile()
    def get_list(self, filter_type: Optional[str] = None, with_secrets: Optional[bool] = True):
        projects = self.project_controller.get_list()
        integrations = self.integration_controller.get_all(show_secrets=with_secrets)
        result = [
            {
                "name": "information_schema",
                "type": "system",
                "id": None,
                "engine": None,
                "visible": True,
                "deletable": False,
            },
            {"name": "log", "type": "system", "id": None, "engine": None, "visible": True, "deletable": False},
        ]
        for x in projects:
            result.append(
                {
                    "name": x.name,
                    "type": "project",
                    "id": x.id,
                    "engine": None,
                    "visible": True,
                    "deletable": x.name.lower() != config.get("default_project"),
                }
            )
        for key, value in integrations.items():
            db_type = value.get("type", "data")
            if db_type != "ml":
                result.append(
                    {
                        "name": key,
                        "type": value.get("type", "data"),
                        "id": value.get("id"),
                        "engine": value.get("engine"),
                        "class_type": value.get("class_type"),
                        "connection_data": value.get("connection_data"),
                        "visible": True,
                        "deletable": value.get("permanent", False) is False,
                    }
                )

        if filter_type is not None:
            result = [x for x in result if x["type"] == filter_type]

        return result

    def get_dict(self, filter_type: Optional[str] = None, lowercase: bool = True):
        return OrderedDict(
            (x["name"].lower() if lowercase else x["name"], {"type": x["type"], "engine": x["engine"], "id": x["id"]})
            for x in self.get_list(filter_type=filter_type)
        )

    def get_integration(self, integration_id):
        # get integration by id

        # TODO get directly from db?
        for rec in self.get_list():
            if rec["id"] == integration_id and rec["type"] == "data":
                return {"name": rec["name"], "type": rec["type"], "engine": rec["engine"], "id": rec["id"]}

    def exists(self, db_name: str) -> bool:
        return db_name.lower() in self.get_dict()

    def get_project(self, name: str, strict_case: bool = False):
        """Get a project by name.

        Args:
            name (str): The name of the project to retrieve.
            strict_case (bool, optional): If True, the project name is case-sensitive. Defaults to False.

        Returns:
            Project: The project instance matching the given name.
        """
        return self.project_controller.get(name=name, strict_case=strict_case)

    def get_system_db(self, name: str):
        if name == "log":
            return self.logs_db_controller
        elif name == "information_schema":
            from mindsdb.api.executor.controllers.session_controller import SessionController

            session = SessionController()
            return session.datahub
        else:
            raise Exception(f"Database '{name}' does not exists")

    def update(self, name: str, data: dict, strict_case: bool = False):
        """
        Updates the database with the given name using the provided data.

        Parameters:
            name (str): The name of the database to update.
            data (dict): The data to update the database with.
            strict_case (bool): if True, then name is case-sesitive

        Raises:
            EntityNotExistsError: If the database does not exist.
        """
        databases = self.get_dict(lowercase=(not strict_case))
        if not strict_case:
            name = name.lower()
        if name not in databases:
            raise EntityNotExistsError("Database does not exist.", name)

        db_type = databases[name]["type"]
        if db_type == "project":
            # Only the name of the project can be updated.
            if {"name"} != set(data):
                raise ValueError("Only the 'name' field can be updated for projects.")
            if not data["name"].islower():
                raise ValueError("New name must be in lower case.")
            self.project_controller.update(name=name, new_name=str(data["name"]))
            return

        elif db_type == "data":
            # Only the parameters (connection data) of the integration can be updated.
            if {"parameters"} != set(data):
                raise ValueError("Only the 'parameters' field can be updated for integrations.")
            self.integration_controller.modify(name, data["parameters"])
            return

        else:
            raise ValueError(f"Database with type '{db_type}' cannot be updated")
