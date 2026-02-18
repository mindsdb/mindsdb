from mindsdb_sql_parser import ast
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class CatalogSearchTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """The select method implements the mappings from the ast.Select and calls the actual API through the call_mendeley_api.
        Firstly, it is used to put the parameters specified in the query in a dictionary, which is then used when calling the method call_mendeley_api.
        If no conditions are specified, an error is raised since the search cannot be conducted.

        Args:
            query (ast.Select): query used to specify the wanted results
        Returns:
            result (DataFrame): the result of the query
        """

        conditions = extract_comparison_conditions(query.where)

        params = {}

        # Since there are three different types of search, and each of them takes different parameters, we use the parameters that lead
        # to the most specific results. For example, in the case of the user specifying the title and the doi of a document, priority is given to
        # the doi.

        if query.limit is not None:
            params['limit'] = query.limit.value
        else:
            params['limit'] = 30

        for op, arg1, arg2 in conditions:

            if arg1 in ['arxiv', 'doi', 'isbn', 'issn', 'pmid', 'scopus', 'filehash']:

                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2

                result = self.handler.call_mendeley_api(
                    method_name='identifier_search',
                    params=params)

                break

            elif arg1 == 'id':
                if op != '=':
                    raise NotImplementedError
                params['id'] = arg2

                result = self.handler.call_mendeley_api(
                    method_name='get',
                    params=params)

                break

            elif "title" or "author" or "source" or "abstract" or "min_year" or "max_year" or "open_access" or "view" in conditions:

                if arg1 == 'title':
                    if op != '=':
                        raise NotImplementedError
                    params['title'] = arg2

                elif arg1 == 'author':
                    if op != '=':
                        raise NotImplementedError
                    params['author'] = arg2

                elif arg1 == 'source':
                    if op != '=':
                        raise NotImplementedError
                    params['source'] = arg2

                elif arg1 == 'abstract':
                    if op != '=':
                        raise NotImplementedError
                    params['abstract'] = arg2

                elif arg1 == 'min_year':
                    params['min_year'] = arg2

                elif arg1 == 'max_year':
                    params['max_year'] = arg2

                elif arg1 == 'open_access':
                    if op != '=':
                        raise NotImplementedError
                    params['open_access'] = arg2

                result = self.handler.call_mendeley_api(
                    method_name='advanced_search',
                    params=params)

        if conditions == []:
            raise ValueError('Please give input for the search to be conducted.')

        columns = []

        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        return result[columns]

    def get_columns(self):
        """ get_columns method returns the columns returned by the API"""
        return [

            'title',
            'type',
            'source',
            'year',
            'pmid',
            'sgr',
            'issn',
            'scopus',
            'doi',
            'pui',
            'authors',
            'keywords',
            'link',
            'id'
        ]
