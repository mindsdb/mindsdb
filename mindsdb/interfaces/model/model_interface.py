class ModelInterface():
    def __init__(self):
        from mindsdb.interfaces.model.model_controller import ModelController
        self.controller = ModelController(False)

    def create(self, *args, **kwargs):
        return self.controller.create(*args, **kwargs)

    def learn(self, *args, **kwargs):
        return self.controller.learn(*args, **kwargs)

    def adjust(self, *args, **kwargs):
        return self.controller.adjust(*args, **kwargs)

    def predict(self, *args, **kwargs):
        return self.controller.predict(*args, **kwargs)

    def analyse_dataset(self, *args, **kwargs):
        return self.controller.analyse_dataset(*args, **kwargs)

    def get_model_data(self, *args, **kwargs):
        return self.controller.get_model_data(*args, **kwargs)

    def get_model_description(self, *args, **kwargs):
        return self.controller.get_model_description(*args, **kwargs)

    def get_models(self, *args, **kwargs):
        return self.controller.get_models(*args, **kwargs)

    def delete_model(self, *args, **kwargs):
        return self.controller.delete_model(*args, **kwargs)

    def rename_model(self, *args, **kwargs):
        return self.controller.rename_model(*args, **kwargs)

    def update_model(self, *args, **kwargs):
        return self.controller.update_model(*args, **kwargs)

    def generate_predictor(self, *args, **kwargs):
        return self.controller.generate_predictor(*args, **kwargs)

    def edit_json_ai(self, *args, **kwargs):
        return self.controller.edit_json_ai(*args, **kwargs)

    def edit_code(self, *args, **kwargs):
        return self.controller.edit_code(*args, **kwargs)

    def fit_predictor(self, *args, **kwargs):
        return self.controller.fit_predictor(*args, **kwargs)

    def code_from_json_ai(self, *args, **kwargs):
        return self.controller.code_from_json_ai(*args, **kwargs)

    def export_predictor(self, *args, **kwargs):
        return self.controller.export_predictor(*args, **kwargs)

    def import_predictor(self, *args, **kwargs):
        return self.controller.import_predictor(*args, **kwargs)


ray_based = False

'''
Notes: Remove ray from actors are getting stuck
try:
    from mindsdb_worker.cluster.ray_interface import ModelInterfaceRay
    import ray
    try:
        ray.init(ignore_reinit_error=True, address='auto')
    except Exception:
        ray.init(ignore_reinit_error=True)
    ModelInterface = ModelInterfaceRay
    ray_based = True
except Exception as e:
    ModelInterface = ModelInterfaceNativeImport
    ray_based = False
'''
