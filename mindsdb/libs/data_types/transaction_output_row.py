
class TransactionOutputRow:

    def __init__(self, transaction_output, row_key):
        self.transaction_output = transaction_output
        self.row_key = row_key

    def __getitem__(self, item):
        return self.transaction_output.data[item][self.row_key]

    def as_dict(self):
        return {key:self.transaction_output.data[key][self.row_key] for key in self.transaction_output.data}

    def explain(self):

        for pred_col in self.transaction_output.evaluations:

            self.transaction_output.evaluations[pred_col][self.row_key].explain()

    def __str__(self):
        return str(self.as_dict())

    def as_list(self):
        #Note that here we will not output the confidence columns

        return [self.transaction_output.evaluations[col][self.row_key] for col in self.transaction_output.columns]

    @property
    def _predicted_values(self):
        return {pred_col:self.transaction_output.evaluations[pred_col][self.row_key].predicted_value for pred_col in self.transaction_output.evaluations}
