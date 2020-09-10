class ModelInterface:
    # Optional methods
    def setup(self):
        pass

    # Optional but with a default implementation
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
        raise NotImplementedError('You must implement `fit` in order to be able to train your model')

    # Mandatory
    def predict(self, from_data, kwargs):
        raise NotImplementedError('You must implement `predict` in order to be able to use your model')
