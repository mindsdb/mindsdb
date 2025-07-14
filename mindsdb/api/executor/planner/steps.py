from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner.step_result import Result


class PlanStep:
    def __init__(self, step_num=None):
        self.step_num = step_num

    @property
    def result(self):
        if self.step_num is None:
            raise PlanningException(
                f"Can't reference a step with no assigned step number. Tried to reference: {type(self)}"
            )
        return Result(self.step_num)

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        for k in vars(self):
            # skip result comparison
            if k == "result_data":
                continue

            if getattr(self, k) != getattr(other, k):
                return False

        return True

    def __repr__(self):
        attrs_dict = vars(self)
        attrs_str = ", ".join([f"{k}={str(v)}" for k, v in attrs_dict.items()])
        return f"{self.__class__.__name__}({attrs_str})"

    def set_result(self, result):
        self.result_data = result


class ProjectStep(PlanStep):
    """Selects columns from a dataframe"""

    def __init__(self, columns, dataframe, ignore_doubles=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.dataframe = dataframe
        self.ignore_doubles = ignore_doubles


# TODO remove
class FilterStep(PlanStep):
    """Filters some dataframe according to a query"""

    def __init__(self, dataframe, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.query = query


# TODO remove
class GroupByStep(PlanStep):
    """Groups output by columns and computes aggregation functions"""

    def __init__(self, dataframe, columns, targets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.columns = columns
        self.targets = targets


class JoinStep(PlanStep):
    """Joins two dataframes, producing a new dataframe"""

    def __init__(self, left, right, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.query = query


class UnionStep(PlanStep):
    """Union of two dataframes, producing a new dataframe"""

    def __init__(self, left, right, unique, operation="union", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.unique = unique
        self.operation = operation


# TODO remove
class OrderByStep(PlanStep):
    """Applies sorting to a dataframe"""

    def __init__(self, dataframe, order_by, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.order_by = order_by


class LimitOffsetStep(PlanStep):
    """Applies limit and offset to a dataframe"""

    def __init__(self, dataframe, limit=None, offset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.limit = limit
        self.offset = offset


class FetchDataframeStep(PlanStep):
    """Fetches a dataframe from external integration"""

    def __init__(self, integration, query=None, raw_query=None, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.integration = integration
        self.query = query
        self.raw_query = raw_query
        self.params = params


class FetchDataframeStepPartition(FetchDataframeStep):
    """Fetches a dataframe from external integration in partitions"""

    def __init__(self, steps=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if steps is None:
            steps = []
        self.steps = steps


class ApplyPredictorStep(PlanStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions"""

    def __init__(
        self,
        namespace,
        predictor,
        dataframe,
        params: dict = None,
        row_dict: dict = None,
        columns_map: dict = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor
        self.dataframe = dataframe
        self.params = params

        # columns to add to input data, struct: {column name: value}
        self.row_dict = row_dict

        # rename columns in input data, struct: {a str: b Identifier}
        #  renames b to a
        self.columns_map = columns_map


class ApplyTimeseriesPredictorStep(ApplyPredictorStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions.
    Accepts an additional parameter output_time_filter that specifies for which dates the predictions should be returned
    """

    def __init__(self, *args, output_time_filter=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_time_filter = output_time_filter


class ApplyPredictorRowStep(PlanStep):
    """Applies a mindsdb predictor to one row of values and returns a dataframe of one row, the predictor."""

    def __init__(self, namespace, predictor, row_dict, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor
        self.row_dict = row_dict
        self.params = params


class GetPredictorColumns(PlanStep):
    """Returns an empty dataframe of shape and columns like predictor results."""

    def __init__(self, namespace, predictor, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor


class GetTableColumns(PlanStep):
    """Returns an empty dataframe of shape and columns like select from table."""

    def __init__(self, namespace, table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.table = table


class MapReduceStep(PlanStep):
    """Applies a step for each value in a list, and then reduces results to a single dataframe"""

    def __init__(self, values, step, reduce="union", partition=None, *args, **kwargs):
        """
        :param values: input step data
        :param step: step to be applied
        :param reduce: type of reduce to be applied
        :param partition: type of partition to be applied
         - <number> - split data by chunks with equal size
         - None - every record is variables to fill
        """
        super().__init__(*args, **kwargs)
        self.values = values
        self.step = step
        self.reduce = reduce
        self.partition = partition


class MultipleSteps(PlanStep):
    def __init__(self, steps, reduce=None, *args, **kwargs):
        """Runs multiple steps and reduces results to a single dataframe"""
        super().__init__(*args, **kwargs)
        self.steps = steps
        self.reduce = reduce


class SaveToTable(PlanStep):
    def __init__(self, table, dataframe, is_replace=False, params=None, *args, **kwargs):
        """
        Creates table if not exists and fills it with content of dataframe
        is_replace - to drop table beforehand
        """
        super().__init__(*args, **kwargs)
        self.table = table
        self.dataframe = dataframe
        self.is_replace = is_replace
        if params is None:
            params = {}
        self.params = params


class InsertToTable(PlanStep):
    def __init__(self, table, dataframe=None, query=None, params=None, *args, **kwargs):
        """Fills table with content of dataframe"""
        super().__init__(*args, **kwargs)
        self.table = table
        self.dataframe = dataframe
        self.query = query
        if params is None:
            params = {}
        self.params = params


class CreateTableStep(PlanStep):
    def __init__(self, table, columns=None, is_replace=False, *args, **kwargs):
        """Fills table with content of dataframe"""
        super().__init__(*args, **kwargs)
        self.table = table
        self.columns = columns
        self.is_replace = is_replace


class UpdateToTable(PlanStep):
    def __init__(self, table, dataframe, update_command, *args, **kwargs):
        """Fills table with content of dataframe"""
        super().__init__(*args, **kwargs)
        self.table = table
        self.dataframe = dataframe
        self.update_command = update_command


class DeleteStep(PlanStep):
    def __init__(self, table, where, *args, **kwargs):
        """Fills table with content of dataframe"""
        super().__init__(*args, **kwargs)
        self.table = table
        self.where = where


class SubSelectStep(PlanStep):
    def __init__(self, query, dataframe, table_name=None, add_absent_cols=False, *args, **kwargs):
        """Performs select from dataframe"""
        super().__init__(*args, **kwargs)
        self.query = query
        self.dataframe = dataframe
        self.table_name = table_name
        self.add_absent_cols = add_absent_cols


class QueryStep(PlanStep):
    def __init__(self, query, from_table=None, *args, strict_where=True, **kwargs):
        """Performs query using injected dataframe"""
        super().__init__(*args, **kwargs)
        self.query = query
        self.from_table = from_table
        self.strict_where = strict_where


class DataStep(PlanStep):
    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data
