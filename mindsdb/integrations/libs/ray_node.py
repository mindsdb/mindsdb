import ray

ray.init()


class RayWrapper:

    def init_handler(self, *args, **kwargs ):
        self.actor = RayActor.remote()
        self.actor.init_handler.remote(*args, **kwargs)

    def close(self):
        del self.actor

    def __getattr__(self, method_name):
        # is call of the method
        def call(*args, **kwargs):
            method = getattr(self.actor, method_name)

            return ray.get(method.remote(*args, **kwargs))

        return call


@ray.remote(num_gpus=1, num_cpus=8)
class RayActor:

    def init_handler(self, class_path, company_id, integration_id, predictor_id):
        import importlib
        from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage

        module_name, class_name = class_path
        module = importlib.import_module(module_name)
        HandlerClass = getattr(module, class_name)

        handlerStorage = HandlerStorage(company_id, integration_id)
        modelStorage = ModelStorage(company_id, predictor_id)

        ml_handler = HandlerClass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        self.ml_instance = ml_handler

    def create(self, *args, **kwargs):
        return self.ml_instance.create(*args, **kwargs)

    def predict(self, *args, **kwargs):
        return self.ml_instance.predict(*args, **kwargs)
