import pandas as pd
import requests
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class BaseResultsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Selects data from the results table and returns it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """
        base_url = self.handler.connect()

        params = {'access_key': self.handler.access_key}
        conditions = extract_comparison_conditions(query.where)
        params.update({condition[1]: condition[2] for condition in conditions if condition[0] == '='})

        if 'query' not in params:
            raise ValueError('Query is missing in the SQL query')
        if 'type' not in params and hasattr(self, 'default_type'):
            params['type'] = self.default_type
        try:
            api_response = requests.get(base_url, params=params)
            api_response.raise_for_status()  # raises HTTPError for bad responses
            api_result = api_response.json()
        except requests.exceptions.HTTPError as e:
            raise SystemError(f"HTTP error occurred: {e.response.status_code} - {e.response.reason}")
        except requests.exceptions.ConnectionError as e:
            raise SystemError(f"Connection error occurred: {str(e)}")
        except requests.exceptions.Timeout as e:
            raise SystemError(f"Request timeout: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise SystemError(f"Request exception occurred: {str(e)}")
        except ValueError as e:
            raise SystemError(f"Failed to parse JSON response: {str(e)}")

        results = api_result.get(self.results_key, [])
        processed_results = [self.extract_data(result) for result in results]

        if len(processed_results) == 0:
            columns = self.get_columns()
            empty_data = {col: ["No results found"] for col in columns}
            return pd.DataFrame(empty_data, columns=columns)

        result_df = pd.DataFrame(processed_results)
        result_df = self.filter_columns(result_df, query)
        return result_df

    def extract_data(self, data):
        """
        Extracts the required data from the result.

        Args:
            data (dict): The result data.

        Returns:
            dict: A dictionary containing the extracted data.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):
        """
        Filters the columns of the result DataFrame.

        Args:
            result (pandas.DataFrame): The result DataFrame.
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the filtered data.
        """
        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.parts[-1])
                else:
                    raise NotImplementedError
        else:
            columns = self.get_columns()

        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            result = result[columns]

        if query is not None and query.limit is not None:
            result = result.head(query.limit.value)

        return result


class OrganicResultsTable(BaseResultsTable):
    results_key = 'organic_results'

    def extract_data(self, organic):
        return {
            'position': organic.get('position'),
            'title': organic.get('title'),
            'url': organic.get('url'),
            'domain': organic.get('domain'),
            'displayed_url': organic.get('displayed_url'),
            'snippet': organic.get('snippet'),
            'cached_page_url': organic.get('cached_page_url'),
            'related_pages_url': organic.get('related_pages_url'),
            'prerender': organic.get('prerender'),
            'sitelinks': self._extract_sitelinks(organic.get('sitelinks')),
            'rich_snippet': self._extract_rich_snippet(organic.get('rich_snippet'))
        }

    def _extract_sitelinks(self, sitelinks):
        if not sitelinks:
            return None
        return {
            'inline': [{'title': link['title'], 'url': link['url']} for link in sitelinks.get('inline', [])],
            'expanded': [{'title': link['title'], 'url': link['url']} for link in sitelinks.get('expanded', [])]
        }

    def _extract_rich_snippet(self, rich_snippet):
        if not rich_snippet:
            return None
        snippet_type = 'top' if 'top' in rich_snippet else 'bottom'
        return {
            'detected_extensions': rich_snippet.get(snippet_type, {}).get('detected_extensions', []),
            'extensions': rich_snippet.get(snippet_type, {}).get('extensions', [])
        }

    def get_columns(self):
        return [
            'position',
            'title',
            'url',
            'domain',
            'displayed_url',
            'snippet',
            'cached_page_url',
            'related_pages_url',
            'prerender',
            'sitelinks',
            'rich_snippet'
        ]


class ImageResultsTable(BaseResultsTable):
    results_key = 'image_results'
    default_type = 'images'

    def extract_data(self, image):
        return {
            'position': image.get('position'),
            'title': image.get('title'),
            'width': image.get('width'),
            'height': image.get('height'),
            'image_url': image.get('image_url'),
            'type': image.get('type'),
            'url': image.get('url'),
            'source': image.get('source')
        }

    def get_columns(self):
        return [
            'position',
            'title',
            'width',
            'height',
            'image_url',
            'type',
            'url',
            'source'
        ]


class VideoResultsTable(BaseResultsTable):
    results_key = 'video_results'
    default_type = 'videos'

    def extract_data(self, video):
        return {
            'position': video.get('position'),
            'title': video.get('title'),
            'url': video.get('url'),
            'displayed_url': video.get('displayed_url'),
            'uploaded': video.get('uploaded'),
            'snippet': video.get('snippet'),
            'length': video.get('length')
        }

    def get_columns(self):
        return [
            'position',
            'title',
            'url',
            'displayed_url',
            'uploaded',
            'snippet',
            'length'
        ]


class NewsResultsTable(BaseResultsTable):
    results_key = 'news_results'
    default_type = 'news'

    def extract_data(self, news):
        return {
            'position': news.get('position'),
            'title': news.get('title'),
            'url': news.get('url'),
            'source_name': news.get('source_name'),
            'uploaded': news.get('uploaded'),
            'uploaded_utc': news.get('uploaded_utc'),
            'snippet': news.get('snippet'),
            'thumbnail_url': news.get('thumbnail_url')
        }

    def get_columns(self):
        return [
            'position',
            'title',
            'url',
            'source_name',
            'uploaded',
            'uploaded_utc',
            'snippet',
            'thumbnail_url'
        ]


class ShoppingResultsTable(BaseResultsTable):
    results_key = 'shopping_results'
    default_type = 'shopping'

    def extract_data(self, shopping):
        return {
            'position': shopping.get('position'),
            'title': shopping.get('title'),
            'url': shopping.get('url')
        }

    def get_columns(self):
        return [
            'position',
            'title',
            'url'
        ]
