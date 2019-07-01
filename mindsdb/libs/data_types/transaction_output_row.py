from mindsdb.libs.helpers.explain_prediction import explain_prediction


class TransactionOutputRow:
    def __init__(self, transaction_output, row_index):
        self.transaction_output = transaction_output
        self.row_index = row_index

    def __getitem__(self, item):
        return self.transaction_output.data[item][self.row_index]

    def __contains__(self, item):
        return item in self.transaction_output.data.keys()

    def explain(self):
        prediction_row = {col: self.transaction_output.data[col][self.row_index] for col in list(self.transaction_output.data.keys())}
        #self.transaction_output.data.iloc[self.row_index]
        return explain_prediction(self.transaction_output.transaction.lmd, prediction_row)

    def why(self): return self.explain()

    def __str__(self):
        return str(self.as_dict())


    def as_dict(self):
        return {key: self.transaction_output.data[key][self.row_index] for key in list(self.transaction_output.data.keys())}

    def as_list(self):
        #Note that here we will not output the confidence columns
        return [self.transaction_output.data[col][self.row_index] for col in list(self.transaction_output.data.keys())]

    @property
    def _predicted_values(self):
        return {pred_col:self.transaction_output.evaluations[pred_col][self.row_index].predicted_value for pred_col in self.transaction_output.evaluations}
