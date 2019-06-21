from mindsdb.libs.phases.base_module import BaseModule


class DataTransformer(BaseModule):

    def run(self, input_data):
        for column in input_data.columns:
            data_type = self.transaction.lmd['column_stats'][column]['data_type']
            data_stype = self.transaction.lmd['column_stats'][column]['data_subtype']
            
        exit()
        pass
