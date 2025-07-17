import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log
from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class DockerHubRepoImagesSummaryTable(APITable):
    """The DockerHub Repo Images Summary Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.docker.com/docker-hub/api/latest/#tag/images" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            repo images summary matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'repo_images_summary',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'namespace':
                if op == '=':
                    search_params["namespace"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for namespace column.")
            elif arg1 == 'repository':
                if op == '=':
                    search_params["repository"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for repository column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("namespace" in search_params) and ("repository" in search_params)

        if not filter_flag:
            raise NotImplementedError("Both namespace and repository columns have to be present in WHERE clause.")

        repo_images_summary_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.docker_client.get_images_summary(search_params["namespace"], search_params["repository"])

        self.check_res(res=response)

        content = response["content"]

        repo_images_summary_df = pd.json_normalize({"active_from": content["active_from"], "total": content["statistics"]["total"], "active": content["statistics"]["active"], "inactive": content["statistics"]["inactive"]})

        select_statement_executor = SELECTQueryExecutor(
            repo_images_summary_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        repo_images_summary_df = select_statement_executor.execute_query()

        return repo_images_summary_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "active_from",
            "total",
            "active",
            "inactive"
        ]


class DockerHubOrgSettingsTable(APITable):
    """The DockerHub Repo Org Settings Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://hub.docker.com/v2/orgs/{name}/settings" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            org settings matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'org_settings',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'organization':
                if op == '=':
                    search_params["organization"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for organization column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        if "organization" not in search_params:
            raise NotImplementedError("organization column has to be present in where clause.")

        organization_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.docker_client.get_org_settings(search_params["organization"])

        self.check_res(res=response)

        content = response["content"]

        organization_df = pd.json_normalize({"restricted_images_enabled": content["restricted_images"]["enabled"], "restricted_images_allow_official_images": content["restricted_images"]["allow_official_images"], "restricted_images_allow_verified_publishers": content["restricted_images"]["allow_verified_publishers"]})

        select_statement_executor = SELECTQueryExecutor(
            organization_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        organization_df = select_statement_executor.execute_query()

        return organization_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "restricted_images_enabled",
            "restricted_images_allow_official_images",
            "restricted_images_allow_verified_publishers"
        ]


class DockerHubRepoImagesTable(APITable):
    """The DockerHub Repo Images Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://hub.docker.com/v2/namespaces/{namespace}/repositories/{repository}/images" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Repo Images matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'repo_images',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'namespace':
                if op == '=':
                    search_params["namespace"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for namespace column.")
            elif arg1 == 'repository':
                if op == '=':
                    search_params["repository"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for repository column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("namespace" in search_params) and ("repository" in search_params)

        if not filter_flag:
            raise NotImplementedError("namespace and repository column has to be present in where clause.")

        repo_images_summary_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.docker_client.get_repo_images(search_params["namespace"], search_params["repository"])

        self.check_res(res=response)

        content = response["content"]

        repo_images_summary_df = pd.json_normalize(content["results"])

        select_statement_executor = SELECTQueryExecutor(
            repo_images_summary_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        repo_images_summary_df = select_statement_executor.execute_query()

        return repo_images_summary_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return ["namespace",
                "repository",
                "digest",
                "tags",
                "last_pushed",
                "last_pulled",
                "status"
                ]


class DockerHubRepoTagTable(APITable):
    """The DockerHub Repo Tag Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.docker.com/docker-hub/api/latest/#tag/images" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Repo Tag matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'repo_tag_details',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'namespace':
                if op == '=':
                    search_params["namespace"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for namespace column.")
            elif arg1 == 'repository':
                if op == '=':
                    search_params["repository"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for repository column.")
            elif arg1 == 'tag':
                if op == '=':
                    search_params["tag"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for tag column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("namespace" in search_params) and ("repository" in search_params) and ("tag" in search_params)

        if not filter_flag:
            raise NotImplementedError("namespace, repository and tag column has to be present in where clause.")

        repo_tag_summary_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.docker_client.get_repo_tag(search_params["namespace"], search_params["repository"], search_params["tag"])

        self.check_res(res=response)

        content = response["content"]

        repo_tag_summary_df = pd.json_normalize({"creator": content["creator"],
                                                 "id": content["id"],
                                                 "images": content["images"],
                                                 "last_updated": content["last_updated"],
                                                 "last_updater": content["last_updater"],
                                                 "last_updater_username": content["last_updater_username"],
                                                 "name": content["name"],
                                                 "repository": content["repository"],
                                                 "full_size": content["full_size"],
                                                 "v2": content["v2"],
                                                 "tag_status": content["tag_status"],
                                                 "tag_last_pulled": content["tag_last_pulled"],
                                                 "tag_last_pushed": content["tag_last_pushed"],
                                                 "media_type": content["media_type"],
                                                 "content_type": content["media_type"]
                                                 })

        select_statement_executor = SELECTQueryExecutor(
            repo_tag_summary_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        repo_tag_summary_df = select_statement_executor.execute_query()

        return repo_tag_summary_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return ["creator",
                "id",
                "images",
                "last_updated",
                "last_updater",
                "last_updater_username",
                "name",
                "repository",
                "full_size",
                "v2",
                "tag_status",
                "tag_last_pulled",
                "tag_last_pushed",
                "media_type",
                "content_type"
                ]


class DockerHubRepoTagsTable(APITable):
    """The DockerHub Repo Tags Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://hub.docker.com/v2/namespaces/{namespace}/repositories/{repository}/tags" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Repo Tag matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'repo_tags',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'namespace':
                if op == '=':
                    search_params["namespace"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for namespace column.")
            elif arg1 == 'repository':
                if op == '=':
                    search_params["repository"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for repository column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("namespace" in search_params) and ("repository" in search_params)

        if not filter_flag:
            raise NotImplementedError("namespace and repository column has to be present in where clause.")

        repo_tags_summary_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.docker_client.get_repo_tags(search_params["namespace"], search_params["repository"])

        self.check_res(res=response)

        content = response["content"]

        repo_tags_summary_df = pd.json_normalize(content["results"])

        select_statement_executor = SELECTQueryExecutor(
            repo_tags_summary_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        repo_tags_summary_df = select_statement_executor.execute_query()

        return repo_tags_summary_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return ["creator",
                "id",
                "images",
                "last_updated",
                "last_updater",
                "last_updater_username",
                "name",
                "repository",
                "full_size",
                "v2",
                "tag_status",
                "tag_last_pulled",
                "tag_last_pushed",
                "media_type",
                "content_type"
                ]
