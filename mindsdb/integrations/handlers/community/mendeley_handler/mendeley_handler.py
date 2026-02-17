from mindsdb_sql_parser import parse_sql
import pandas as pd
from mendeley import Mendeley
from mindsdb.integrations.libs.api_handler import APIHandler
from mendeley.session import MendeleySession
from mindsdb.integrations.handlers.mendeley_handler.mendeley_tables import CatalogSearchTable
from mindsdb.utilities import log
from typing import Dict
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse
)

logger = log.getLogger(__name__)


class MendeleyHandler(APIHandler):

    def __init__(self, name, **kwargs):
        """ constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)

        self.connection_args = kwargs.get('connection_data', {})

        self.client_id = self.connection_args.get('client_id', None)
        self.client_secret = self.connection_args.get('client_secret', None)
        self.session = self.connect()

        self.session = None
        self.is_connected = False

        catalog_search_data = CatalogSearchTable(self)
        self.catalog_search_data = catalog_search_data
        self._register_table('catalog_search_data', catalog_search_data)

    def connect(self) -> MendeleySession:
        """ The connect method sets up the connection required by the handler.
        In order establish a connection with Mendeley API one needs the client id and client secret that are
        created after registering the application at https://dev.mendeley.com/myapps.html . More information on the matter
        can be found at https://dev.mendeley.com/reference/topics/application_registration.html .
        In order to have access to Mendeley data we use "session".

        Returns:
        HandlerStatusResponse """

        if self.is_connected:
            return self.session

        mendeley = Mendeley(self.client_id, self.client_secret)
        auth = mendeley.start_client_credentials_flow()
        self.session = auth.authenticate()

        self.is_connected = True
        return self.session

    def check_connection(self) -> StatusResponse:
        """ The check_connection method checks the connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Mendeley: {e}!')
            response.error_message = str(e)

        self.is_connected = response.success
        return response

    def native_query(self, query_string: str):
        """The native_query method receives raw query and acts upon it.
            Args:
                query_string (str): query in native format
            Returns:
                HandlerResponse
        """
        ast = parse_sql(query_string)
        return self.query(ast)

    def get_authors(self, data):
        """The get_authors method receives the data - a specific document returned by the API, gets the names of the authors
            and combines them in a string, so as to allow the use of DataFrame.
            Args:
                data (CatalogDocument): document returned by API
            Returns:
                authors string
        """
        authors = ""
        sum = 0
        if data.authors is not None:
            for x in data.authors:
                if sum + 1 == len(data.authors) and x.first_name is not None and x.last_name is not None:
                    authors = authors + x.first_name + " " + x.last_name
                else:
                    if x.first_name is not None and x.last_name is not None:
                        authors = authors + x.first_name + " " + x.last_name + ", "
                        sum = sum + 1
        return authors

    def get_keywords(self, data):
        """The get_keywords method receives the data-a specific document returned by the API, gets the specified keywords
            and combines them in a string, so as to allow the use of DataFrame.
            Args:
                data (CatalogDocument) : document returned by the API
            Returns:
                keywords string
        """
        keywords = ""
        sum = 0
        if data.keywords is not None:
            for x in data.keywords:
                if sum + 1 == len(data.keywords):
                    keywords = keywords + x + " "
                else:
                    if x is not None:
                        keywords = keywords + x + ", "
                        sum = sum + 1
        return keywords

    def create_dict(self, data):
        """The create_dict method receives the data-a specific document returned by the API, gets the resources-fields of the document,
          as specified in Mendley documentation, and puts them in a dictionary.

            Args:
                data (CatalogDocument) : document returned by API
            Returns:
                dict dictionary
        """
        dict = {}
        dict["title"] = data.title
        dict["type"] = data.type
        dict["source"] = data.source
        dict["year"] = data.year
        if data.identifiers is not None:
            dict["pmid"] = data.identifiers.get("pmid")
            dict["sgr"] = data.identifiers.get("sgr")
            dict["issn"] = data.identifiers.get("issn")
            dict["scopus"] = data.identifiers.get("scopus")
            dict["doi"] = data.identifiers.get("doi")
            dict["pui"] = data.identifiers.get("pui")
        dict["authors"] = self.get_authors(data)
        if data.keywords is not None:
            dict["keywords"] = self.get_keywords(data)
        else:
            dict["keywords"] = None
        dict["link"] = data.link
        dict["id"] = data.id
        return dict

    def call_mendeley_api(self, method_name: str, params: Dict) -> pd.DataFrame:
        """The method call_mendeley_api is used to communicate with Mendeley. Depending on the method used there are three different types
        of search conducted.
        The advanced_search results in a CatalogSearch resource, which, depending on the parameters used, could either be a number of different documents (CatalogDocument),
        a single one or none.
        The by_identifier search is more specific in nature and can result either in one or no CatalogDocuments.
        The get search has the same results as the by_identifier.
        If the method specified does not exist, an NotImplementedError is raised.
        Args:
            method_name (str) : name of method
            params (Dict): Dictionary containing the parameters used in the search
        Returns:
            DataFrame
        """

        self.session = self.connect()

        if method_name == 'advanced_search':
            search_params = {
                'title': params.get("title"),
                'author': params.get("author"),
                'source': params.get("source"),
                'abstract': params.get("abstract"),
                'min_year': params.get("min_year"),
                'max_year': params.get("max_year"),
                'open_access': params.get("open_access")
            }
            data = self.session.catalog.advanced_search(**search_params)
            sum = 0
            df = pd.DataFrame()
            for x in data.list(page_size=params["limit"]).items:
                if sum == 0:
                    df = pd.DataFrame(self.create_dict(x), index=[0])
                    sum += 1
                else:
                    df = df.append(self.create_dict(x), ignore_index=True)
                    sum += 1
            if df.empty:
                raise NotImplementedError(('Insufficient or wrong input given'))
            else:
                return df

        elif method_name == 'identifier_search':
            search_params = {
                'arxiv': params.get("arxiv"),
                'doi': params.get("doi"),
                'isbn': params.get("isbn"),
                'issn': params.get("issn"),
                'pmid': params.get("pmid"),
                'scopus': params.get("scopus"),
                'filehash': params.get("filehash")
            }
            data = self.session.catalog.by_identifier(**search_params)
            df = pd.DataFrame(self.create_dict(data), index=[0])
            return df

        elif method_name == 'get':
            data = self.session.catalog.get(params.get("id"))
            df = pd.DataFrame(self.create_dict(data), index=[0])
            return df

        raise NotImplementedError('Method name {} not supported by Mendeley API Handler'.format(method_name))
