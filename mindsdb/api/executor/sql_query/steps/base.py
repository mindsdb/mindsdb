
class BaseStepCall:
    bind = None

    def __init__(self, sql_query):
        self.steps_data = sql_query.steps_data

        self.sql_query = sql_query
        self.context = sql_query.context
        self.session = sql_query.session

        # context
        # self.database
        # self.row_id
        # self.predictor_metadata
        # self.query_str

        # self.session.predictor_cache

        # session
        # self.datahub

        # get/set
        # self.columns_list

    def set_columns_list(self, columns_list):
        self.sql_query.columns_list = columns_list

    def get_columns_list(self):
        return self.sql_query.columns_list

    def call(self, step):
        raise NotImplementedError
