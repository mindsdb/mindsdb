from tests import basic_test
import torch

# Test with a few basic options
if __name__ == "__main__":
    basic_test(backend='lightwood',use_gpu=torch.cuda.is_available(), IS_CI_TEST=True)
    print('\n\n=============[Success]==============\n     Finished running quick test !\n=============[Success]==============\n\n')
