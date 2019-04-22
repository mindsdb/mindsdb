from mindsdb.libs.helpers.general_helpers import evaluate_accuracy, get_value_bucket
from mindsdb.libs.phases.stats_generator import StatsGenerator

class ColumnEvaluator():
    """
    # The Hypothesis Executor is responsible for testing out various scenarios
    regarding the model, in order to determine things such as the importance of
    input variables or the variability of output values
    """

    def __init__(self):
        self.columnless_predictions = {}
        self.normal_predictions = None


    def get_column_importance(self, model, output_columns, input_columns, full_dataset, stats):
        self.normal_predictions = model.predict('validate')
        normal_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

        column_importance_dict = {}
        col_buckets_stats = {}

        for input_column in input_columns:
            # See what happens with the accuracy of the outputs if only this column is present
            ignore_columns = [col for col in input_columns if col != input_column ]
            col_only_predictions = model.predict('validate', ignore_columns)
            col_only_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

            if col_only_accuracy > normal_accuracy*0.75:
                split_data = {}
                #columns = [[col, col_ind] for col_ind, col in enumerate(self.transaction.lmd['columns'])]
                for value in full_dataset[input_column]:

                    if 'percentage_buckets' in stats[input_column]:
                        bucket = stats[input_column]['percentage_buckets']
                    else:
                        bucket = None

                    vb = get_value_bucket(value, bucket, stats[input_column])
                    if vb not in split_data:
                        split_data[vb] = []

                    split_data[vb].append(value)


            col_only_normalized_accuracy = col_only_accuracy/normal_accuracy

            # See what happens with the accuracy if all columns but this one are present
            ignore_columns = [input_column]
            col_missing_predictions = model.predict('validate', ignore_columns)

            self.columnless_predictions[input_column] = col_missing_predictions

            col_missing_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, output_columns)

            if col_missing_accuracy > normal_accuracy*0.75:
                print(f'\n\n\n\n--------------------!!!!!!!!!!!!    {input_column}   !!!!!!!!!!!!--------------------\n\n\n\n')

            col_missing_reverse_accuracy = (normal_accuracy - col_missing_accuracy)/normal_accuracy

            column_importance = (col_only_normalized_accuracy + col_missing_reverse_accuracy)/2
            column_importance_dict[input_column] = column_importance
        return column_importance_dict

    def get_column_influence(self):
        pass









#
