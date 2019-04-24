from mindsdb.libs.helpers.general_helpers import evaluate_accuracy, get_value_bucket
from mindsdb.libs.phases.stats_generator.stats_generator import StatsGenerator
from mindsdb.libs.data_types.transaction_data import TransactionData

class ColumnEvaluator():
    """
    # The Hypothesis Executor is responsible for testing out various scenarios
    regarding the model, in order to determine things such as the importance of
    input variables or the variability of output values
    """

    def __init__(self, transaction):
        self.columnless_predictions = {}
        self.normal_predictions = None
        self.transaction = transaction

    def get_column_importance(self, model, output_columns, input_columns, full_dataset, stats):
        self.normal_predictions = model.predict('validate')
        normal_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

        column_importance_dict = {}
        buckets_stats = {}

        for input_column in input_columns:
            # See what happens with the accuracy of the outputs if only this column is present
            ignore_columns = [col for col in input_columns if col != input_column ]
            col_only_predictions = model.predict('validate', ignore_columns)
            col_only_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

            col_only_normalized_accuracy = col_only_accuracy/normal_accuracy

            # See what happens with the accuracy if all columns but this one are present
            ignore_columns = [input_column]
            col_missing_predictions = model.predict('validate', ignore_columns)

            self.columnless_predictions[input_column] = col_missing_predictions

            col_missing_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

            col_missing_reverse_accuracy = (normal_accuracy - col_missing_accuracy)/normal_accuracy
            column_importance = (col_only_normalized_accuracy + col_missing_reverse_accuracy)/2
            column_importance_dict[input_column] = column_importance

            # If this coulmn is either very important or not important at all, compute stats for each of the buckets (in the validation data)
            if column_importance > 0.8 or column_importance < 0.2:
                split_data = {}
                for value in full_dataset[input_column]:

                    if 'percentage_buckets' in stats[input_column]:
                        bucket = stats[input_column]['percentage_buckets']
                    else:
                        bucket = None

                    vb = get_value_bucket(value, bucket, stats[input_column])
                    if f'{input_column}_bucket_{vb}' not in split_data:
                        split_data[f'{input_column}_bucket_{vb}'] = []

                    split_data[f'{input_column}_bucket_{vb}'].append(value)

                row_wise_data = []
                max_length = max(list(map(len, split_data.values())))

                columns = []
                for i in range(max_length):
                    row_wise_data.append([])
                    for k in split_data.keys():
                        # If the sub bucket has less than 6 values, it's no relevant
                        if len(split_data[k]) > 6:
                            columns.append(k)
                            if len(split_data[k]) > i:
                                row_wise_data[-1].append(split_data[k][i])
                            else:
                                row_wise_data[-1].append(None)

                input_data = TransactionData()
                input_data.data_array = row_wise_data
                input_data.columns = columns

                stats_generator = StatsGenerator(session=None, transaction=self.transaction)
                col_buckets_stats = stats_generator.run(input_data=input_data, modify_light_metadata=False)

                buckets_stats.update(col_buckets_stats)

        return column_importance_dict, buckets_stats

    def get_column_influence(self):
        pass









#
