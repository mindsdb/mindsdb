class ModelInterface:
    # Optional methods
    def setup(self):
        pass

    # Optional but with a default implementation
    def save(self, path):
        import pickle
        with open(path, 'wb'):
            pickle.dump(self, path)

    @staticmethod
    def load(path):
        import pickle
        with open(path, 'rb'):
            model = pickle.load(path)
        return model

    # Mandatory under certain circumstances
    def fit(self, from_data, to_predict, data_analysis, kwargs):
        raise NotImplementedError('You must implement `fit` in order to be able to train your model')

    # Mandatory
    def predict(self, from_data, kwargs):
        raise NotImplementedError('You must implement `predict` in order to be able to use your model')
