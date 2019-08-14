from mindsdb.libs.helpers.explain_prediction import explain_prediction


class TransactionOutputRow:
    def __init__(self, transaction_output, row_index):
        self.transaction_output = transaction_output
        self.row_index = row_index
        self.col_stats = self.transaction_output.col_stats
        self.data = self.transaction_output.data
        self.evaluations = self.transaction_output.evaluations

    def __getitem__(self, item):
        return self.data[item][self.row_index]

    def __contains__(self, item):
        return item in self.data.keys()

    def explain(self):
        for pred_col in self.evaluations:
            evaluation = self.evaluations[pred_col][self.row_index]
            clusters = evaluation.explain()
            print(self.col_stats[pred_col]['data_type'])
            print(self.col_stats[pred_col]['data_subtype'])
            return clusters

            prediction_row = {col: self.data[col][self.row_index] for col in list(self.data.keys())}
            return explain_prediction(self.transaction_output.transaction.lmd, prediction_row)

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
