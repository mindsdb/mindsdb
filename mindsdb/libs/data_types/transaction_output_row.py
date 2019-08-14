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
                    answers[pred_col].append({
                        'most_likely_value': cluster['middle_bucket'],
                        'value_range': value_range,
                        'confidence': cluster['confidence'],
                        'explaination': explaination,
                        'simple': f'We are {pct_confidence}% confident your answer lies between {value_range[0]} and {value_range[-1]}'
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
        simple_answers = [x['simple'] for x in answers]
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
