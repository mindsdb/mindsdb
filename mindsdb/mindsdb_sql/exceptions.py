class MindsdbSQLException(Exception):
    pass


class ParsingException(MindsdbSQLException):
    pass


class PlanningException(MindsdbSQLException):
    pass
