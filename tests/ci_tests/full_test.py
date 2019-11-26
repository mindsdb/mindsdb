from tests import BasicTest
import tests
import torch
import mindsdb
import unittest


use_gpu_settings = []
if torch.cuda.is_available():
    use_gpu_settings.append(True)

use_gpu_settings.append(False)

# Cycle through a few options:
for backend in ['lightwood', 'ludwig']:
    for use_gpu in use_gpu_settings:
        print(f'use_gpu is set to {use_gpu}, backend is set to {backend}')
        BasicTest.backend = backend
        BasicTest.use_gpu = use_gpu
        BasicTest.ignore_columns = []
        BasicTest.IS_CI_TEST = True
        suite = unittest.TestLoader().loadTestsFromModule(tests)
        unittest.TextTestRunner(verbosity=2).run(suite)
        unittest.main(argv=[backend], exit=False)
        print(
            '\n\n=============[Success]==============\n     Finished running quick test !\n=============[Success]==============\n\n')
        # basic_test(backend=backend, use_gpu=use_gpu, ignore_columns=[], IS_CI_TEST=True)

# Try ignoring some columns and running only the stats generator

BasicTest.backend = 'lightwood'
BasicTest.use_gpu = use_gpu_settings[0]
BasicTest.ignore_columns = ['days_on_market', 'number_of_bathrooms']
BasicTest.IS_CI_TEST = True
BasicTest.run_extra = True
suite = unittest.TestLoader().loadTestsFromModule(tests)
unittest.TextTestRunner(verbosity=2).run(suite)
unittest.main(argv=["full_test"], exit=False)

# basic_test(backend='lightwood', use_gpu=use_gpu_settings[0],
#            ignore_columns=['days_on_market', 'number_of_bathrooms'], run_extra=True, IS_CI_TEST=True)
print(
    '\n\n=============[Success]==============\n     Finished running full test suite !\n=============[Success]==============\n\n')

