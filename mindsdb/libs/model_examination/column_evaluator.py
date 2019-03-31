import pickle


class ColumnEvaluator():
    """
    # The Hypothesis Executor is responsible for testing out various scenarios
    regarding the model, in order to determine things such as the importance of
    input variables or the variability of output values
    """

    def __init__(self):
        self.columnless_predictions = {}
        self.normal_predictions = None

    def pickle(self):
        """
        Returns a version of self that can be serialized into mongodb or tinydb
        :return: The data of a HypothesisExecutor serialized via pickle and decoded as a latin1 string
        """
        return pickle.dumps(self).decode(encoding='latin1')

    @staticmethod
    def unpickle(pickle_string):
        """
        :param pickle_string: A latin1 encoded python str containing the pickle data
        :return: Returns a HypothesisExecutor object generated from the pickle string
        """
        return pickle.loads(pickle_string.encode(encoding='latin1'))

    # @TODO: Move into a more generic file
    @staticmethod
    def evaluate_accuracy(predictions, real_values, col_stats, input_columns):
        score = 0
        for input_column in input_columns:
            cummulative_scores = 0
            for i in range(len(predictions[input_column])):
                if predictions[input_column][i] == real_values[input_column][i]:
                    cummulative_scores += 1

            score += cummulative_scores/len(predictions[input_column])
        score = score/len(input_columns)
        return score

    def get_column_importance(self, model, output_columns, input_columns, full_dataset, stats):
        self.normal_predictions = self.transaction.model_backend.predict('validate', ignore_columns)
        normal_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, input_columns)

        column_importance_dict = {}

        for input_column in input_columns:
            # See what happens with the accuracy of the outputs if only this column is present
            ignore_columns = [col if col != input_column for col in input_columns]
            col_only_predictions = model.predict('validate', ignore_columns)
            col_only_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, input_columns)
            col_only_normalized_accuracy = col_only_accuracy/normal_accuracy

            # See what happens with the accuracy if all columns but this one are present
            ignore_columns = [input_column]
            col_missing_predictions = model.predict('validate', ignore_columns)

            self.columnless_predictions[input_column] = col_missing_predictions

            col_missing_accuracy = evaluate_accuracy(self.normal_predictions, full_dataset, stats, input_columns)
            col_missing_reverse_accuracy = (normal_accuracy - col_missing_accuracy)/normal_accuracy

            column_importance = (col_only_normalized_accuracy + col_missing_reverse_accuracy)/2
            column_importance_dict[input_column] = column_importance

        return column_importance_dict

    def get_column_influence(self):
        pass









#
