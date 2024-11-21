import hashlib

from mindsdb.api.executor.planner.steps import UnionStep

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import WrongArgumentError

from .base import BaseStepCall


class UnionStepCall(BaseStepCall):

    bind = UnionStep

    def call(self, step):
        left_result = self.steps_data[step.left.step_num]
        right_result = self.steps_data[step.right.step_num]

        # count of columns have to match
        if len(left_result.columns) != len(right_result.columns):
            raise WrongArgumentError(
                f'UNION columns count mismatch: {len(left_result.columns)} != {len(right_result.columns)} ')

        # types have to match
        # TODO: return checking type later
        # for i, left_col in enumerate(left_result.columns):
        #     right_col = right_result.columns[i]
        #     type1, type2 = left_col.type, right_col.type
        #     if type1 is not None and type2 is not None:
        #         if type1 != type2:
        #             raise ErSqlWrongArguments(f'UNION types mismatch: {type1} != {type2}')

        data = ResultSet()
        for col in left_result.columns:
            data.add_column(col)

        records_hashes = []
        results = []
        for row in left_result.to_lists():
            if step.unique:
                checksum = hashlib.sha256(str(row).encode()).hexdigest()
                if checksum in records_hashes:
                    continue
                records_hashes.append(checksum)
            results.append(list(row))

        for row in right_result.to_lists():
            if step.unique:
                checksum = hashlib.sha256(str(row).encode()).hexdigest()
                if checksum in records_hashes:
                    continue
                records_hashes.append(checksum)
            results.append(list(row))
        data.add_raw_values(results)

        return data
