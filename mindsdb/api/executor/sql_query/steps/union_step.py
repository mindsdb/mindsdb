from mindsdb.api.executor.planner.steps import UnionStep

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import WrongArgumentError
from mindsdb.api.executor.utilities.sql import query_df_with_type_infer_fallback
import numpy as np

from .base import BaseStepCall


class UnionStepCall(BaseStepCall):
    bind = UnionStep

    def call(self, step):
        left_result = self.steps_data[step.left.step_num]
        right_result = self.steps_data[step.right.step_num]

        # count of columns have to match
        if len(left_result.columns) != len(right_result.columns):
            raise WrongArgumentError(
                f"UNION columns count mismatch: {len(left_result.columns)} != {len(right_result.columns)} "
            )

        # types have to match
        # TODO: return checking type later
        # for i, left_col in enumerate(left_result.columns):
        #     right_col = right_result.columns[i]
        #     type1, type2 = left_col.type, right_col.type
        #     if type1 is not None and type2 is not None:
        #         if type1 != type2:
        #             raise ErSqlWrongArguments(f'UNION types mismatch: {type1} != {type2}')

        table_a, names = left_result.to_df_cols()
        table_b, _ = right_result.to_df_cols()

        if step.operation.lower() == "intersect":
            op = "INTERSECT"
        else:
            op = "UNION"

        if step.unique is not True:
            op += " ALL"

        query = f"""
            SELECT * FROM table_a
            {op}
            SELECT * FROM table_b
        """

        resp_df, _description = query_df_with_type_infer_fallback(query, {"table_a": table_a, "table_b": table_b})
        resp_df.replace({np.nan: None}, inplace=True)

        return ResultSet.from_df_cols(df=resp_df, columns_dict=names)
