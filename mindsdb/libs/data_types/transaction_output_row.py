from mindsdb.libs.helpers.explain_prediction import explain_prediction, get_important_missing_cols
from mindsdb.libs.constants.mindsdb import *


class TransactionOutputRow:
    def __init__(self, transaction_output, row_index):
        self.transaction_output = transaction_output
        self.row_index = row_index
        self.col_stats = self.transaction_output.transaction.lmd['column_stats']
        self.data = self.transaction_output.data
        self.evaluations = self.transaction_output.evaluations

    def __getitem__(self, item):
        return self.data[item][self.row_index]

    def __contains__(self, item):
        return item in self.data.keys()

    def explain(self):
        answers = {}
        for pred_col in self.evaluations:
            answers[pred_col] = {}

            prediction_row = {col: self.data[col][self.row_index] for col in list(self.data.keys())}

            evaluation = self.evaluations[pred_col][self.row_index]
            clusters = evaluation.explain(self.transaction_output.transaction.lmd['column_stats'][pred_col])

            cluster = clusters[0]

            answers[pred_col]['predicted_value'] = cluster['value']
            answers[pred_col]['confidence'] = round(cluster['confidence'] * 100)

            if f'{pred_col}_model_confidence' in prediction_row:
                answers[pred_col]['confidence'] = round((prediction_row[f'{pred_col}_model_confidence'] * 100 + answers[pred_col]['confidence'])/2)

            if answers[pred_col]['confidence'] > 70:
                quality = 'high certainty'
            elif answers[pred_col]['confidence'] > 35:
                quality = 'average certainty'
            else:
                quality = 'low certainty'

            answers[pred_col]['explanation'] = {
                'prediction_quality': quality
            }

            if self.col_stats[pred_col]['data_type'] in (DATA_TYPES.NUMERIC, DATA_TYPES.DATE):
                value_range = [cluster['buckets'][0],cluster['buckets'][-1]]

                range_pretty_start = value_range[0]
                if range_pretty_start > 1000:
                    range_pretty_start = round(range_pretty_start)
                if range_pretty_start > 100:
                    range_pretty_start = round(range_pretty_start,2)
                if range_pretty_start > 10:
                    range_pretty_start = round(range_pretty_start,1)
                elif range_pretty_start > pow(10,-3):
                    range_pretty_start = round(range_pretty_start,6)

                range_end_start = value_range[-1]
                if range_end_start > 1000:
                    range_end_start = round(range_end_start)
                if range_end_start > 100:
                    range_end_start = round(range_end_start,2)
                if range_end_start > 10:
                    range_end_start = round(range_end_start,1)
                elif range_end_start > pow(10,-3):
                    range_end_start = round(range_end_start,6)

                answers[pred_col]['explanation']['confidence_interval'] = [range_pretty_start, range_end_start]

            important_missing_cols = get_important_missing_cols(self.transaction_output.transaction.lmd, prediction_row, pred_col)
            answers[pred_col]['explanation']['important_missing_information'] = important_missing_cols

            if self.transaction_output.input_confidence is not None:
                answers[pred_col]['explanation']['confidence_composition'] = self.transaction_output.input_confidence[pred_col]

            if self.transaction_output.extra_insights is not None:
                answers[pred_col]['explanation']['extra_insights'] = self.transaction_output.extra_insights[pred_col]
        return answers

    def epitomize(self):
        answers = self.explain()
        simple_answers = []

        for pred_col in answers:
            confidence = answers[pred_col]['confidence']
            value = answers[pred_col]['predicted_value']
            simple_col_answer = f'We are {confidence}% confident the value of "{pred_col}" is {value}'
            simple_answers.append(simple_col_answer)

        return '* ' + '\n* '.join(simple_answers)

    def __str__(self):
        return str(self.epitomize())

    def as_dict(self):
        return {key: self.data[key][self.row_index] for key in list(self.data.keys()) if not key.startswith('model_')}

    def as_list(self):
        #Note that here we will not output the confidence columns
        return [self.data[col][self.row_index] for col in list(self.data.keys()) if not key.startswith('model_')]

    def raw_predictions(self):
        return {key: self.data[key][self.row_index] for key in list(self.data.keys()) if key.startswith('model_')}

    @property
    def _predicted_values(self):
        return {pred_col: evaluations[pred_col][self.row_index].predicted_value for pred_col in evaluations}
