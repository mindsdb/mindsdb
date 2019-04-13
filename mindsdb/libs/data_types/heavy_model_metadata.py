class HeavyModelMetadata():

    _entity_name = 'heavy_model_metadata'
    _pkey = ['model_name']

    def setup(self):

        self.probabilistic_validators = None
        self.model_name = None
