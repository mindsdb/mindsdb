from mindsdb_sql.planner.steps import MultipleSteps

from mindsdb.api.mysql.mysql_proxy.utilities import ErNotSupportedYet

from .base import BaseStepCall


class MultipleStepsCall(BaseStepCall):

    bind = MultipleSteps

    def call(self, step):

        if step.reduce != 'union':
            raise ErNotSupportedYet(f"Only MultipleSteps with type = 'union' is supported. Got '{step.type}'")
        data = None
        for substep in step.steps:
            subdata = self.sql_query.execute_step(substep)
            if data is None:
                data = subdata
            else:
                data.add_records(subdata.get_records())

        return data
