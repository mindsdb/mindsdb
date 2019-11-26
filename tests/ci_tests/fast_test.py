from tests import BasicTest
import torch
import tests
import unittest

BasicTest.backend = 'lightwood'
BasicTest.use_gpu = torch.cuda.is_available()
BasicTest.ignore_columns = []
BasicTest.IS_CI_TEST = True
suite = unittest.TestLoader().loadTestsFromModule(tests)
unittest.TextTestRunner(verbosity=2).run(suite)
unittest.main(argv=['fast-test'], exit=False)
print( '\n\n=============[Success]==============\n     Finished running quick test !\n=============[Success]==============\n\n')
