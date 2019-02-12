from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler


class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        #for group in self.transaction.model_data.validation_set:
        #columns = self.transaction.model_data.validation_set[group]
        validation_samper = Sampler(self.transaction.model_data.train_set,
        metadata_as_stored=self.transaction.persistent_model_metadata, sampler_mode=SAMPLER_MODES.LEARN)

        for batch in validation_samper:
            ret = self.transaction.data_model_object.forward(batch.getInput(flatten=False))
            print(ret)
