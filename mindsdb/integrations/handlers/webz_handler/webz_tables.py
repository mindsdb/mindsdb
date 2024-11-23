from typing import List

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class WebzBaseAPITable(APITable):

    ENDPOINT = None
    OUTPUT_COLUMNS = []
    SORTABLE_COLUMNS = []
    TABLE_NAME = None

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the API and returns it as a pandas DataFrame

        Returns dataframe representing the API results.

        Args:
            query (ast.Select): SQL SELECT query

        """
        conditions = extract_comparison_conditions(query.where)
        params = {}

        for op, arg1, arg2 in conditions:
            if op != "=":
                raise NotImplementedError(f"Unsupported Operator: {op}")
            elif arg1 == "query":
                params["q"] = arg2
            else:
                raise NotImplementedError(f"Unknown clause: {arg1}")

        if query.order_by:
            if len(query.order_by) > 1:
                raise ValueError("Unsupported to order by multiple fields")
            order_item = query.order_by[0]
            sort_column = ".".join(order_item.field.parts[1:])
            # make sure that column is sortable
            if sort_column not in type(self).SORTABLE_COLUMNS:
                raise ValueError(f"Order by unknown column {sort_column}")
            params.update({"sort": sort_column, "order": order_item.direction.lower()})

        if query.limit is not None:
            params["size"] = query.limit.value
        result = self.handler.call_webz_api(
            method_name=type(self).TABLE_NAME, params=params
        )

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError(f"Unknown query target {type(target)}")

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            return pd.DataFrame([], columns=columns)

        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # filter by columns
        result = result[columns]

        # Rename columns
        for target in query.targets:
            if target.alias:
                result.rename(
                    columns={target.parts[-1]: str(target.alias)}, inplace=True
                )
        return result

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
            List of columns

        """
        return [column.replace(".", "__") for column in type(self).OUTPUT_COLUMNS]


class WebzPostsTable(WebzBaseAPITable):
    """To interact with structured posts data from news articles, blog posts and online discussions
    provided through the Webz.IO API.

    """

    ENDPOINT = "filterWebContent"
    OUTPUT_COLUMNS = [
        "thread.uuid",
        "thread.url",
        "thread.site_full",
        "thread.site",
        "thread.site_section",
        "thread.section_title",
        "thread.title",
        "thread.title_full",
        "thread.published",
        "thread.replies_count",
        "thread.participants_count",
        "thread.site_type",
        "thread.main_image",
        "thread.country",
        "thread.site_categories",
        "thread.social.facebook.likes",
        "thread.social.facebook.shares",
        "thread.social.facebook.comments",
        "thread.social.gplus.shares",
        "thread.social.pinterest.shares",
        "thread.social.linkedin.shares",
        "thread.social.stumbledupon.shares",
        "thread.social.vk.shares",
        "thread.performance_score",
        "thread.domain_rank",
        "thread.domain_rank_updated",
        "thread.reach.per_million",
        "thread.reach.page_views",
        "thread.reach.updated",
        "uuid",
        "url",
        "ord_in_thread",
        "parent_url",
        "author",
        "published",
        "title",
        "text",
        "language",
        "external_links",
        "external_images",
        "rating",
        "entities.persons",
        "entities.organizations",
        "entities.locations",
        "crawled",
    ]
    SORTABLE_COLUMNS = [
        "crawled",
        "relevancy",
        "social.facebook.likes",
        "social.facebook.shares",
        "social.facebook.comments",
        "social.gplus.shares",
        "social.pinterest.shares",
        "social.linkedin.shares",
        "social.stumbledupon.shares",
        "social.vk.shares",
        "replies_count",
        "participants_count",
        "performance_score",
        "published",
        "thread.published",
        "domain_rank",
        "ord_in_thread",
        "rating",
    ]
    TABLE_NAME = "posts"


class WebzReviewsTable(WebzBaseAPITable):
    """To interact with structured reviews data from hundreds of review sites,
    provided through the Webz.IO API.

    """

    ENDPOINT = "reviewFilter"
    OUTPUT_COLUMNS = [
        "item.uuid",
        "item.url",
        "item.site_full",
        "item.site",
        "item.site_section",
        "item.section_title",
        "item.title",
        "item.title_full",
        "item.published",
        "item.reviews_count",
        "item.reviewers_count",
        "item.main_image",
        "item.country",
        "item.site_categories",
        "item.domain_rank",
        "item.domain_rank_updated",
        "uuid",
        "url",
        "ord_in_thread",
        "author",
        "published",
        "title",
        "text",
        "language",
        "external_links",
        "rating",
        "crawled",
    ]
    SORTABLE_COLUMNS = [
        "crawled",
        "relevancy",
        "reviews_count",
        "reviewers_count",
        "spam_score",
        "domain_rank",
        "ord_in_thread",
        "rating",
    ]
    TABLE_NAME = "reviews"
