
class ModelData():

    def __init__(self):

        self.train_set = {}
        self.test_set = {}
        self.validation_set = {}

        self.predict_set = {}

        # map from set to index in input data
        self.test_set_map = {}
        self.train_set_map = {}
        self.validation_set_map = {}

        self.predict_set_map = {}

