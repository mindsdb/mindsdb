from typing import List

import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


class CustomAPITable(APITable):

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        return [item for item in self.columns if item not in ignore]

    def select(self, query: ast.Select) -> pd.DataFrame:
        raise NotImplementedError()

    def parse_select(self, query: ast.Select, table_name: str):
        select_statement_parser = SELECTQueryParser(query, table_name, self.get_columns())
        self.selected_columns, self.where_conditions, self.order_by_conditions, self.result_limit = select_statement_parser.parse_query()

    def get_package_name(self, query: ast.Select):
        params = conditions_to_filter(query.where)
        if "package" not in params:
            raise Exception("Where condition does not have 'package' selector")
        return params["package"]

    def apply_query_params(self, df, query):
        select_statement_parser = SELECTQueryParser(query, self.name, self.get_columns())
        selected_columns, _, order_by_conditions, result_limit = select_statement_parser.parse_query()
        select_statement_executor = SELECTQueryExecutor(df, selected_columns, [], order_by_conditions)
        return select_statement_executor.execute_query()


class NPMMetadataTable(CustomAPITable):
    name: str = "metadata"
    columns: List[str] = [
        "name",
        "scope",
        "version",
        "description",
        "author_name",
        "author_email",
        "publisher_username",
        "publisher_email",
        "repository_url",
        "license",
        "num_releases",
        "num_downloads",
        "num_stars",
        "score",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["name", "scope", "version", "description", "author", "publisher", "repository", "license", "releases"]
        )
        metadata["author_name"] = metadata["author"].get("name")
        metadata["author_email"] = metadata["author"].get("email")
        del metadata["author"]
        metadata["publisher_username"] = metadata["publisher"].get("username")
        metadata["publisher_email"] = metadata["publisher"].get("email")
        del metadata["publisher"]
        metadata["repository"] = metadata["repository"].get("url")
        metadata["num_releases"] = sum([x["count"] for x in metadata["releases"]])
        del metadata["releases"]
        npm_data = connection.get_cols_in(
            ["collected", "npm"],
            ["downloads", "starsCount"]
        )
        npm_data["num_downloads"] = sum([x["count"] for x in npm_data["downloads"]])
        del npm_data["downloads"]
        npm_data["num_stars"] = npm_data["starsCount"]
        del npm_data["starsCount"]
        score = connection.get_cols_in(
            ["score"],
            ["final"]
        )
        df = pd.DataFrame.from_records([{**metadata, **npm_data, "score": score["final"]}])
        return self.apply_query_params(df, query)


class NPMMaintainersTable(CustomAPITable):
    name: str = "maintainers"
    columns: List[str] = [
        "username",
        "email"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["maintainers"]
        )
        records = [{col: x[col] for col in self.columns} for x in metadata.get("maintainers", [])]
        df = pd.DataFrame.from_records(records)
        return self.apply_query_params(df, query)


class NPMKeywordsTable(CustomAPITable):
    name: str = "keywords"
    columns: List[str] = [
        "keyword"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["keywords"]
        )
        records = [{"keyword": keyword} for keyword in metadata["keywords"]]
        df = pd.DataFrame.from_records(records)
        return self.apply_query_params(df, query)


class NPMDependenciesTable(CustomAPITable):
    name: str = "dependencies"
    columns: List[str] = [
        "dependency",
        "version"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["dependencies"]
        )
        records = [{"dependency": d, "version": v} for d, v in metadata["dependencies"].items()]
        df = pd.DataFrame.from_records(records)
        return self.apply_query_params(df, query)


class NPMDevDependenciesTable(CustomAPITable):
    name: str = "dev_dependencies"
    columns: List[str] = [
        "dev_dependency",
        "version"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["devDependencies"]
        )
        records = [{"dev_dependency": d, "version": v} for d, v in metadata["devDependencies"].items()]
        df = pd.DataFrame.from_records(records)
        return self.apply_query_params(df, query)


class NPMOptionalDependenciesTable(CustomAPITable):
    name: str = "optional_dependencies"
    columns: List[str] = [
        "optional_dependency",
        "version"
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        metadata = connection.get_cols_in(
            ["collected", "metadata"],
            ["optionalDependencies"]
        )
        records = [{"optional_dependency": d, "version": v} for d, v in metadata["optionalDependencies"].items()]
        df =  pd.DataFrame.from_records(records)
        return self.apply_query_params(df, query)


class NPMGithubStatsTable(CustomAPITable):
    name: str = "github_stats"
    columns: List[str] = [
        "homepage",
        "num_stars",
        "num_forks",
        "num_subscribers",
        "num_issues",
        "num_open_issues",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        package_name = self.get_package_name(query)
        connection = self.handler.connection(package_name)
        github_data = connection.get_cols_in(
            ["collected", "github"],
            ["homepage", "starsCount", "forksCount", "subscribersCount", "issues"]
        )
        github_data["num_stars"] = github_data["starsCount"]
        del github_data["starsCount"]
        github_data["num_forks"] = github_data["forksCount"]
        del github_data["forksCount"]
        github_data["num_subscribers"] = github_data["subscribersCount"]
        del github_data["subscribersCount"]
        github_data["num_issues"] = github_data["issues"].get("count", 0)
        github_data["num_open_issues"] = github_data["issues"].get("openCount", 0)
        del github_data["issues"]
        df = pd.DataFrame.from_records([github_data])
        return self.apply_query_params(df, query)
