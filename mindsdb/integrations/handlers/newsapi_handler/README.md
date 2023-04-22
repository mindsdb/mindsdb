# NuoDB Handler

This is the implementation of the NuoDB handler for MindsDB.

## NuoDB

importast

fromcollectionsimportOrderedDict

fromtypingimportAny

importpandasaspd

fromrequestsimportResponse

frommindsdb.api.mysql.mysql_proxy.libs.constants.response_typeimportRESPONSE_TYPE

frommindsdb.integrations.libs.api_handlerimportAPIHandler, APITable, FuncParser

frommindsdb.integrations.libs.baseimportDatabaseHandler

frommindsdb.integrations.libs.responseimportHandlerResponse, HandlerStatusResponse

frommindsdb.integrations.utilities.sql_utilsimportextract_comparison_conditions

fromnewsapiimportNewsApiClient, NewsAPIException

frommindsdb_sql.parser.ast.baseimportASTNode

frommindsdb.microservices_grpc.db.common_pb2importStatusResponse

importurllib

frommindsdb.utilities.configimportConfig

importos

frommindsdb.integrations.libs.constimportHANDLER_CONNECTION_ARG_TYPEasARG_TYPE

classNewsAPIArticleTable(APITable):

def__init__(self, handler):

super().__init__(handler)

defselect(self, query: ast.Select) -> pd.DataFrame:

conditions=extract_comparison_conditions(query.where)

params= {}

forop, arg1, arg2inconditions:

ifarg1=='query':

params['query'] =urllib.parse.quote_plus(arg2)

elifarg1=='sources':

iflen(arg2.split(",")) >20:

raiseValueError("The number of items it sources should be 20 or less")

else:

params[arg1] =arg2

elifarg1=='publishedAt':

ifop=='Gt'orop=='GtE':

params['from'] =arg2

ifop=='Lt'orop=='LtE':

params['to'] =arg2

elifop=='Eq':

params['from'] =arg2

params['to'] =arg2

else:

params[arg1] =arg2

if query.limit:

if query.limit.value >100:

params['page_size'], params['page'] =divmod(query.limit.value, 100)

params['page_size'] = query.limit.value

params['page'] =1

else:

params['page_size'] =100

params['page'] =1

if query.order_by:

iflen(query.order_by) ==1:

if query.order_by[0] notin ['query', 'publishedAt']:

raiseNotImplementedError("Not supported ordering by this field")

params['sortBy'] == query.order_by[0]

else:

raiseValueError("Multiple order by condition is not supported by the API")

result=self.handler.call_application_api(params=params)

selected_columns= []

fortargetin query.targets:

ifisinstance(target, ast.Star):

selected_columns=self.get_columns()

break

elifisinstance(target, ast.Identifier):

selected_columns.append(target.parts[-1])

else:

raiseValueError(f"Unknown query target {type(target)}")

returnresult[selected_columns]

defget_columns(self) -> list:

return [

'author',

'title',

'description',

'url',

'urlToImage',

'publishedAt',

'content',

'source_id',

'source_name',

'query',

'searchIn',

'domains',

'excludedDomains',

    ]

classNewsAPIHandler(DatabaseHandler):

def__init__(self, name: str, **kwargs):

super().__init__(name)

print(f"TEST ---- {name}")

self.api=None

self._tables= {}

args= kwargs.get('connection_data', {})

self.connection_args= {}

handler_config=Config().get('newsAPI_handler', {})

forkin ['api_key']:

ifkinargs:

self.connection_args[k] =args[k]

eliff'NEWSAPI_{k.upper()}'inos.environ:

self.connection_args[k] =os.environ[f'NEWSAPI_{k.upper()}']

elifkinhandler_config:

self.connection_args[k] =handler_config[k]

self.is_connected=False

article=NewsAPIArticleTable(self)

self._register_table('article', article)

def__del__(self):

ifself.is_connectedisTrue:

self.disconnect()

defdisconnect(self):

"""

    Close any existing connections.

    """

ifself.is_connectedisFalse:

return

self.is_connected=False

returnself.is_connected

defcreate_connection(self):

returnNewsApiClient(**self.connection_args)

def_register_table(self, table_name: str, table_class: Any):

self._tables[table_name] = table_class

defconnect(self) -> HandlerStatusResponse:

ifself.is_connectedisTrue:

returnself.api

self.api=self.create_connection()

self.is_connected=True

returnHandlerStatusResponse(success=True)

defcheck_connection(self) -> HandlerStatusResponse:

response=HandlerStatusResponse(False)

try:

self.connect()

self.api.get_top_headlines()

response.success=True

except NewsAPIException ase:

response.error_message=e.message

returnresponse

defnative_query(self, query: Any) -> HandlerResponse:

returnsuper().native_query(query)

defcall_application_api(self, method_name:str=None, params:dict=None) -> pd.DataFrame:

# This will implement api base on the native query

# By processing native query to convert it to api callable parameters

pages= params['pages']

data= []

forpageinrange(pages):

    params['pages'] =page

result=self.api.get_everything(**params)

articles=result['articles']

articles['source_id'] =articles['source']['id']

articles['source_name'] =articles['source']['name']

delarticles['source']

articles['query'] = params['query']

articles['searchIn'] = params['qintitle']

articles['domains'] = params['domains']

articles['excludedDomains'] = params['exclude_domains']

data.extend(articles)

returnpd.DataFrame(data=data)

connection_args=OrderedDict(

    api_key={

'type': ARG_TYPE.STR,

'description': 'The API key for the Airtable API.'

    }

)

connection_args_example=OrderedDict(

    api_key='knlsndlknslk'

)

NuoDB delivers consistent, resilient distributed SQL for your mission critical applications, so you can deploy on premises, in public or private clouds, in hybrid environments, or across clouds.
NuoDB is the distributed SQL database designed to meet the rapidly evolving demands of today’s enterprises, scale on demand, eliminate downtime and reduce total cost of ownership. All while maintaining SQL compatibility.

## Implementation

This handler was implemented using the JDBC driver provided by NuoDB. To establish connection with the database, `JayDeBeApi` library is used. The `JayDeBeApi` module allows you to connect from Python code to databases using Java JDBC. It provides a Python DB-API v2.0 to that database.

The required arguments to establish a connection are,

* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `user`: The username to authenticate with the NuoDB server.
* `password`: The password to authenticate the user with the NuoDB server.
* `is_direct`: This argument indicates whether a direct connection to the TE is to be attempted.

Other optional arguments are,

* `schema`: The schema name to use when connecting with the NuoDB.
* `jar_location`: The location of the jar files which contain the JDBC class. This need not be specified if the required classes are already added to the CLASSPATH variable.
* `driver_args`: The extra arguments which can be specified to the driver. Specify this in the format: "arg1=value1,arg2=value2.
  More information on the supported paramters can be found at: https://doc.nuodb.com/nuodb/latest/deployment-models/physical-or-vmware-environments-with-nuodb-admin/reference-information/connection-properties/

## Usage

In order to make use of this handler and connect to Apache Derby in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE nuo_datasource
WITH engine='nuo_jdbc',
parameters={
    "host": "localhost",
    "port": "48006",
    "database": "test",
    "schema": "hockey",
    "user": "dba",
    "password": "goalie",
    "is_direct": "true",
};
```

Now, you can use this established connection to query your database as follows,

```sql
SELECT * FROM nuo_datasource.PLAYERS;
```
