from flask_restx import Namespace, Resource
import mysql.connector


ns_conf = Namespace('nlp', description='The NLP API of mindsdb')


@ns_conf.route('/<query>')
@ns_conf.param('query', 'The natural language query')
@ns_conf.response(404, 'query failed')
class PredictorList(Resource):
    @ns_conf.doc('list_predictors')
    def get(self, query):
        '''Turn a natural language query into a sql query and deliver the response'''
        # For more info on ITG see https://github.com/mindsdb/ITG/tree/staging/itg
        try:
            from itg import itg
            # @TODO here and/or when this ns is started register all the user db schemas + mindsdb's db schema
            # itg.register((dataframe, 'db_name'))

            sql = itg(query).query
            print(f'The NLP API got the natural language query "{query}" and will execute the sql query: {sql}')

            con = mysql.connector.connect(
                host='localhost',
                port='47335',
                user='mindsdb',
                password='',
                database='mindsdb'
            )

            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute(query)
            res = cur.fetchall()
            status = 200
        except Exception as e:
            res = {'error': str(e), 'sql_query': sql}
            status = 400
        con.commit()
        con.close()

        return res, status
