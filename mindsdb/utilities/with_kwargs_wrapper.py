import inspect


class WithKWArgsWrapper(object):
    def __init__(self, original_instance, **kwargs):
        self.default_kwargs = kwargs
        self.original_instance = original_instance
        self.wrapped_methods = {}
        for method_name, method in inspect.getmembers(original_instance, inspect.ismethod):
            method_meta = inspect.getfullargspec(method)
            for arg_name in kwargs:
                if arg_name in method_meta.args:
                    if method_name not in self.wrapped_methods:
                        self.wrapped_methods[method_name] = {
                            'args': [],
                            'index': []
                        }
                    self.wrapped_methods[method_name]['args'].append(arg_name)
                    self.wrapped_methods[method_name]['index'].append(method_meta.args.index(arg_name) - 1)

    def __getattr__(self, method_name):
        def wrapper(*args, **kwargs):
            method = getattr(self.original_instance, method_name)
            if method_name in self.wrapped_methods:
                wrapped_args_names = self.wrapped_methods[method_name]['args']
                wrapped_args_indexes = self.wrapped_methods[method_name]['index']
                for i, arg_name in enumerate(wrapped_args_names):
                    if wrapped_args_indexes[i] < len(args):
                        continue
                    if arg_name not in kwargs:
                        kwargs[arg_name] = self.default_kwargs[arg_name]
            return method(*args, **kwargs)
        return wrapper

    @staticmethod
    def _test():
        class T:
            def one(self, *args, **kwargs):
                print(f"test one: {kwargs.get('test')}")
                return kwargs.get('test')

            def two(self, test=1):
                print(f'test two: {test}')
                return test

            def three(self, x, test=1):
                print(f'test three: {test}')
                return test

            def four(self, x, test=1, y='y'):
                print(f'test four: {test}')
                return test

        t = WithKWArgsWrapper(T(), test='x')

        assert t.one() is None
        assert t.one(test=0) == 0
        assert t.one(test=None) is None

        assert t.two(2) == 2
        assert t.two() == 'x'
        assert t.two(test=0) == 0

        assert t.three(3) == 'x'
        assert t.three(3, 0) == 0
        assert t.three(3, test=0) == 0

        assert t.four(4) == 'x'
        assert t.four(4, 0, 4) == 0
        assert t.four(4, y=4, test=0) == 0
        assert t.four(4, test=0, y=4) == 0
        assert t.four(4, y=4) == 'x'
