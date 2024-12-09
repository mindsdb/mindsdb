from mindsdb.api.executor.planner.steps import MultipleSteps

from mindsdb.api.executor.exceptions import NotSupportedYet

from .base import BaseStepCall


class MultipleStepsCall(BaseStepCall):

    bind = MultipleSteps

    def call(self, step):

        if step.reduce != 'union':
            raise NotSupportedYet(f"Only MultipleSteps with type = 'union' is supported. Got '{step.type}'")
        data = None
        for substep in step.steps:
            subdata = self.sql_query.execute_step(substep)
            if data is None:
                data = subdata
            else:
                data.add_from_result_set(subdata)

        return data
