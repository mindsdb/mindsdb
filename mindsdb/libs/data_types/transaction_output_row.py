from mindsdb.libs.helpers.explain_prediction import explain_prediction
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
            answers[pred_col] = []

            prediction_row = {col: self.data[col][self.row_index] for col in list(self.data.keys())}
            explaination = explain_prediction(self.transaction_output.transaction.lmd, prediction_row)

            evaluation = self.evaluations[pred_col][self.row_index]
            clusters = evaluation.explain()
            
            for cluster in clusters:
                pct_confidence = round(cluster['confidence'] * 100)
                probabilistic_value = cluster['middle_bucket']
                predicted_value = cluster['predicted_value']

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

                    answers[pred_col].append({
                        'most_likely_value': cluster['middle_bucket'],
                        'value_range': value_range,
                        'confidence': cluster['confidence'],
                        'explaination': explaination,
                        'simple': f'We are {pct_confidence}% confident your answer lies between {range_pretty_start} and {range_end_start}'
                    })
                else:
                    answers[pred_col].append({
                        'most_likely_value': cluster['middle_bucket'],
                        'confidence': cluster['confidence'],
                        'explaination': explaination,
                        'simple': f'We are {pct_confidence}% confident your answer is {probabilistic_value}'
                    })

        return answers

    def simple_explain(self):
        answers = self.explain()
        simple_answers = {}

        for pred_col in answers:
            col_answers = answers[pred_col]
            simple_col_answers = [x['simple'] for x in col_answers]
            simple_answers[pred_col] = simple_col_answers

        return simple_answers

    def why(self): return self.explain()

    def __str__(self):
        return str(self.as_dict())


    def as_dict(self):
        return {key: self.data[key][self.row_index] for key in list(self.data.keys())}

    def as_list(self):
        #Note that here we will not output the confidence columns
        return [self.data[col][self.row_index] for col in list(self.data.keys())]

    @property
    def _predicted_values(self):
        return {pred_col:evaluations[pred_col][self.row_index].predicted_value for pred_col in evaluations}
