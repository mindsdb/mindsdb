
class BaseStepCall:
    bind = None

    def __init__(self, sql_query, steps_data=None):
        if steps_data is None:
            steps_data = sql_query.steps_data
        self.steps_data = steps_data

        self.sql_query = sql_query
        self.context = sql_query.context
        self.session = sql_query.session

    def set_columns_list(self, columns_list):
        self.sql_query.columns_list = columns_list

    def get_columns_list(self):
        return self.sql_query.columns_list

    def call(self, step):
        raise NotImplementedError
