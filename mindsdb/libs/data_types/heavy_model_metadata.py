from mindsdb.libs.data_types.persistent_object import PersistentObject

class HeavyModelMetadata(PersistentObject):

    _entity_name = 'heavy_model_metadata'
    _pkey = ['model_name']

    def setup(self):

        self.probabilistic_validators = None
        self.model_name = None
