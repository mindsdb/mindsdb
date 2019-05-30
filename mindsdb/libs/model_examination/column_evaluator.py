from mindsdb.libs.helpers.general_helpers import evaluate_accuracy, get_value_bucket
from mindsdb.libs.phases.stats_generator.stats_generator import StatsGenerator
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import disable_console_output

import pandas as pd


class ColumnEvaluator():
    """
    # The Hypothesis Executor is responsible for testing out various scenarios
    regarding the model, in order to determine things such as the importance of
    input variables or the variability of output values
    """

    def __init__(self, transaction):
        self.transaction = transaction

    def get_column_importance(self, model, output_columns, input_columns, full_dataset, stats):
        columnless_prediction_distribution = {}
        all_columns_prediction_distribution = {}

        normal_predictions = model.predict('validate')
        normal_accuracy = evaluate_accuracy(normal_predictions, full_dataset, stats, output_columns)
        column_importance_dict = {}
        buckets_stats = {}

        # Histogram for when all columns are present, in order to plot the force vectors
        for output_column in output_columns:
            # @TODO: Running stats generator just to get the histogram is very inefficient, change this
            validation_set_output_column_histogram, _ = StatsGenerator.get_histogram(normal_predictions[output_column], data_type=stats[output_column]['data_type'],data_subtype=stats[output_column]['data_subtype'])

            if validation_set_output_column_histogram is not None:
                all_columns_prediction_distribution[output_column] = validation_set_output_column_histogram

        ignorable_input_columns = []
        for input_column in input_columns:
            if stats[input_column]['data_type'] != DATA_TYPES.FILE_PATH:
                ignorable_input_columns.append(input_column)

        for input_column in ignorable_input_columns:
            # See what happens with the accuracy of the outputs if only this column is present
            ignore_columns = [col for col in ignorable_input_columns if col != input_column]
            col_only_predictions = model.predict('validate', ignore_columns)
            col_only_accuracy = evaluate_accuracy(col_only_predictions, full_dataset, stats, output_columns)

            col_only_normalized_accuracy = col_only_accuracy/normal_accuracy

            # See what happens with the accuracy if all columns but this one are present
            ignore_columns = [input_column]
            col_missing_predictions = model.predict('validate', ignore_columns)

            col_missing_accuracy = evaluate_accuracy(col_missing_predictions, full_dataset, stats, output_columns)

            col_missing_reverse_accuracy = (normal_accuracy - col_missing_accuracy)/normal_accuracy
            column_importance = (col_only_normalized_accuracy + col_missing_reverse_accuracy)/2
            column_importance_dict[input_column] = column_importance

            # Histogram for when the column is missing, in order to plot the force vectors
            for output_column in output_columns:
                if output_column not in columnless_prediction_distribution:
                    columnless_prediction_distribution[output_column] = {}

                # @TODO: Running stats generator just to get the histogram is very inefficient, change this
                col_missing_output_histogram, _ = StatsGenerator.get_histogram(col_missing_predictions[output_column], data_type=stats[output_column]['data_type'],data_subtype=stats[output_column]['data_subtype'])

                if col_missing_output_histogram is None:
                    columnless_prediction_distribution[output_column][input_column] = missing_output_histogram

        # @TODO should be go back to generating this information based on the buckets of the input columns ? Or just keep doing the stats generation for the input columns based on the indexes of the buckets for the output column
        #for column in ignorable_input_columns:
        #    if c(column_importance_dict[column] > 0.8 or column_importance_dict[column] < 0.2):

        for output_column in output_columns:
                buckets_stats[output_column] = {}

                bucket_indexes = {}
                for index,row in full_dataset.iterrows():
                    value = row[output_column]
                    if 'percentage_buckets' in stats[output_column]:
                        percentage_buckets = stats[output_column]['percentage_buckets']
                    else:
                        percentage_buckets = None

                    value_bucket = get_value_bucket(value, percentage_buckets, stats[output_column])
                    if value_bucket not in bucket_indexes:
                        bucket_indexes[value_bucket] = []
                    bucket_indexes[value_bucket].append(index)

                for bucket in bucket_indexes:
                    buckets_stats[output_column][bucket] = {}
                    input_data = TransactionData()
                    input_data.data_frame = full_dataset.loc[bucket_indexes[bucket]]
                    input_data.columns = input_data.data_frame.columns

                    stats_generator = StatsGenerator(session=None, transaction=self.transaction)
                    try:
                        with disable_console_output():
                            col_buckets_stats = stats_generator.run(input_data=input_data, modify_light_metadata=False, print_logs=False)
                        buckets_stats[output_column][bucket].update(col_buckets_stats)
                    except:
                        pass
                        # @TODO Is this worth informing the user about ?
                        #print('Cloud not generate bucket stats for sub-bucket: {}'.format(bucket))

        return column_importance_dict, buckets_stats, columnless_prediction_distribution, all_columns_prediction_distribution

    def get_column_influence(self):
        pass









#
