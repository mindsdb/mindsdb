from mindsdb.libs.helpers.explain_prediction import explain_prediction, get_important_missing_cols
from mindsdb.libs.constants.mindsdb import *


class TransactionOutputRow:
    def __init__(self, transaction_output, row_index):
        self.transaction_output = transaction_output
        self.predict_columns = self.transaction_output.transaction.lmd['predict_columns']
        self.row_index = row_index
        self.col_stats = self.transaction_output.transaction.lmd['column_stats']
        self.data = self.transaction_output.data
        self.explanation = self.new_explain()

    def __getitem__(self, item):
        return self.data[item][self.row_index]

    def __contains__(self, item):
        return item in self.data.keys()

    def new_explain(self):
        answers = {}
        for pred_col in self.predict_columns:
            answers[pred_col] = {}

            prediction_row = {col: self.data[col][self.row_index] for col in self.data.keys()}

            answers[pred_col]['predicted_value'] = prediction_row[pred_col]

            if f'{pred_col}_model_confidence' in prediction_row:
                answers[pred_col]['confidence'] = round((prediction_row[f'{pred_col}_model_confidence'] * 3 + prediction_row[f'{pred_col}_confidence'] * 1)/4, 4)
            else:
                answers[pred_col]['confidence'] = prediction_row[f'{pred_col}_confidence']

            quality = 'very confident'
            if answers[pred_col]['confidence'] < 0.8:
                quality = 'confident'
            if answers[pred_col]['confidence'] < 0.6:
                quality = 'somewhat confident'
            if answers[pred_col]['confidence'] < 0.4:
                quality = 'not very confident'
            if answers[pred_col]['confidence'] < 0.2:
                quality = 'not confident'

            answers[pred_col]['explanation'] = {
                'prediction_quality': quality
            }

            if self.col_stats[pred_col]['data_type'] in (DATA_TYPES.NUMERIC, DATA_TYPES.DATE):
                if f'{pred_col}_confidence_range' in prediction_row:
                    answers[pred_col]['explanation']['confidence_interval'] = prediction_row[f'{pred_col}_confidence_range']

            important_missing_cols = get_important_missing_cols(self.transaction_output.transaction.lmd, prediction_row, pred_col)
            answers[pred_col]['explanation']['important_missing_information'] = important_missing_cols

            if self.transaction_output.input_confidence is not None:
                answers[pred_col]['explanation']['confidence_composition'] = {k:v for (k,v) in self.transaction_output.input_confidence[pred_col].items() if v > 0}

            if self.transaction_output.extra_insights is not None:
                answers[pred_col]['explanation']['extra_insights'] = self.transaction_output.extra_insights[pred_col]

            for k in answers[pred_col]['explanation']:
                answers[pred_col][k] = answers[pred_col]['explanation'][k]

        return answers

    def explain(self):
        answers = {}
        for pred_col in self.predict_columns:
            answers[pred_col] = []

            prediction_row = {col: self.data[col][self.row_index] for col in list(self.data.keys())}

            clusters = [{'value': prediction_row[pred_col], 'confidence': prediction_row[f'{pred_col}_confidence']}]

            for cluster in clusters:
                pct_confidence = round(cluster['confidence'] * 100)
                predicted_value = cluster['value']

                if f'{pred_col}_model_confidence' in prediction_row:
                    new_conf = round((prediction_row[f'{pred_col}_model_confidence'] * 3 + cluster['confidence'] * 1)/4, 4)
                else:
                    new_conf = cluster['confidence']

                explanation = explain_prediction(self.transaction_output.transaction.lmd, prediction_row, cluster['confidence'], pred_col)
                answers[pred_col].append({
                    'value': predicted_value,
                    'confidence': new_conf,
                    'explanation': explanation,
                    'explaination': explanation,
                    'simple': f'We are {pct_confidence}% confident the value of "{pred_col}" is {predicted_value}'
                })

                if self.transaction_output.input_confidence is not None:
                    for i in range(len(answers[pred_col])):
                        answers[pred_col][i]['confidence_influence_scores'] = {
                            'confidence_variation_score': []
                            ,'column_names': []
                        }
                        for c in self.transaction_output.input_confidence:
                            answers[pred_col][i]['confidence_influence_scores']['confidence_variation_score'].append(self.transaction_output.input_confidence[c])
                            answers[pred_col][i]['confidence_influence_scores']['column_names'].append(str(c))

                model_result = {
                    'value': prediction_row[f'model_{pred_col}']
                }

                if f'{pred_col}_model_confidence' in prediction_row:
                    model_result['confidence'] = prediction_row[f'{pred_col}_model_confidence']

                answers[pred_col][-1]['model_result'] = model_result

            answers[pred_col] = sorted(answers[pred_col], key=lambda x: x['confidence'], reverse=True)

        return answers

    def epitomize(self):
        answers = self.new_explain()
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
        return [self.data[col][self.row_index] for col in list(self.data.keys()) if not col.startswith('model_')]

    def raw_predictions(self):
        return {key: self.data[key][self.row_index] for key in list(self.data.keys()) if key.startswith('model_')}

    @property
    def _predicted_values(self):
        return {pred_col: evaluations[pred_col][self.row_index].predicted_value for pred_col in evaluations}
