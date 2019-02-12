from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule


class StatsLoader(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        #for group in self.transaction.model_data.validation_set:
        #columns = self.transaction.model_data.validation_set[group]
        validation_samper = Sampler(self.data.validation_set, metadata_as_stored=self.persistent_model_metadata)
