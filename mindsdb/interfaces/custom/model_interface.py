class ModelInterface:
    # Should not be override, or if it is, should be addtionally called using `super(YourChildClass, self).initialize_column_types()`
    def initialize_column_types(self):
        if self.column_type_map is None:
            self.column_type_map = {
                'Empty_target': {
                    'typing': {
                        'data_type': 'Text'
                        ,'data_subtype': 'Short Text'
                    }
                }
                ,'Empty_input': {
                    'typing': {
                        'data_type': 'Text'
                        ,'data_subtype': 'Short Text'
                    }
                }
            }

        if self.to_predict is None:
            self.to_predict = 'Empty_target'

    # Optional methods
    def setup(self):
        pass

    # Optional but with a default implementation
    column_type_map = None
    to_predict = None

    def save(self, path):
        import pickle
        with open(path, 'wb') as fp:
            pickle.dump(self, fp)

    @staticmethod
    def load(path):
        import pickle
        with open(path, 'rb') as fp:
            model = pickle.load(fp)
        return model

    # Mandatory under certain circumstances
    def fit(self, from_data, to_predict, data_analysis, kwargs):
        self.to_predict = to_predict
        self.column_type_map = data_analysis
        raise NotImplementedError('You must implement `fit` in order to be able to train your model')

    # Mandatory
    def predict(self, from_data, kwargs):
        raise NotImplementedError('You must implement `predict` in order to be able to use your model')
