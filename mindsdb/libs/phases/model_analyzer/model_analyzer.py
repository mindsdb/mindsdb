from mindsdb.libs.helpers.general_helpers import convert_snake_to_cammelcase_string
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.sampler import Sampler
from mindsdb.libs.ml_models.pytorch.libs import base_model;
from mindsdb.libs.ml_models.pytorch.libs.torch_helpers import array_to_float_variable
import importlib


class ModelAnalyzer(BaseModule):

    phase_name = PHASE_MODEL_ANALYZER

    def run(self):
        #for group in self.transaction.model_data.validation_set:
        #columns = self.transaction.model_data.validation_set[group]

        validation_sampler = Sampler(self.transaction.model_data.validation_set, metadata_as_stored=self.transaction.persistent_model_metadata,
                                    ignore_types=self.transaction.data_model_object.ignore_types, sampler_mode=SAMPLER_MODES.LEARN)

        validation_sampler.variable_wrapper = array_to_float_variable # it should be self.transaction.data_model_object.variable_wrapper but for some reason is passing a bound object method, figure it out, but for now this works
        '''
        @ <--- field ids is not yet set at this point
        bm = base_model.BaseModel(validation_samper.getSampleBatch())
        self.data_model_object = bm.load_from_disk(file_ids=self.transaction.persistent_ml_model_info.fs_file_ids)
        '''


        ret = self.transaction.data_model_object.testModel(validation_sampler)#forward(batch.getInput(flatten=False))
        print(ret)



def test():
    from mindsdb.libs.controllers.predictor import Predictor
    from mindsdb import CONFIG

    CONFIG.DEBUG_BREAK_POINT = PHASE_MODEL_ANALYZER

    mdb = Predictor(name='home_rentals')

    mdb.learn(
        from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv",
        # the path to the file where we can learn from, (note: can be url)
        to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
        sample_margin_of_error=0.02,
        stop_training_in_x_seconds=3
    )



# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()