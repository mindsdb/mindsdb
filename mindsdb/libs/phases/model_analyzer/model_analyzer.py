from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.ml_models.pytorch.libs import base_model;
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator


class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        #for group in self.transaction.model_data.validation_set:
        #columns = self.transaction.model_data.validation_set[group]
        validation_sampler = Sampler(self.transaction.model_data.validation_set,
        metadata_as_stored=self.transaction.persistent_model_metadata, sampler_mode=SAMPLER_MODES.TRAIN)

        '''
        @ <--- field ids is not yet set at this point
        bm = base_model.BaseModel(validation_sampler.getSampleBatch())
        self.data_model_object = bm.load_from_disk(file_ids=self.transaction.persistent_ml_model_info.fs_file_ids)
        '''

        probabilistic_validator = ProbabilisticValidator()

        for batch in validation_sampler:
            input = batch.getInput(flatten=False)

            real_values = []
            for col in self.transaction.metadata.model_predict_columns:
                print(input.keys())
                real_values.append(input[col])

            features = input.drop(self.transaction.metadata.model_predict_columns, axis=1)

            results = self.transaction.data_model_object.forward(features)
            for i in range(results):
                print(i)
                result = results[i]
                features = fe
