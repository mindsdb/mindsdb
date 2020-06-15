import torch
import mindsdb
from tests import basic_test


if __name__ == "__main__":
    use_gpu_settings = [False]
    if torch.cuda.is_available():
        use_gpu_settings.append(True)

    # Cycle through a few options:
    for backend in ['lightwood']:
        for use_gpu in use_gpu_settings:
            print(f'use_gpu is set to {use_gpu}, backend is set to {backend}')
            basic_test(backend=backend,
                       use_gpu=use_gpu,
                       IS_CI_TEST=True)

    print('\n\n=============[Success]==============\n     Finished running full test suite !\n=============[Success]==============\n\n')
