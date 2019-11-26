from tests import basic_test
import torch
import mindsdb


if __name__ == "__main__":
    use_gpu_settings = []
    if torch.cuda.is_available():
        use_gpu_settings.append(True)

    use_gpu_settings.append(False)

    # Cycle through a few options:
    for backend in ['lightwood','ludwig']:
        for use_gpu in use_gpu_settings:
            print(f'use_gpu is set to {use_gpu}, backend is set to {backend}')
            basic_test(backend=backend,use_gpu=use_gpu,ignore_columns=[], IS_CI_TEST=True)

    # Try ignoring some columns and running only the stats generator
    basic_test(backend='lightwood',use_gpu=use_gpu_settings[0],ignore_columns=['days_on_market','number_of_bathrooms'],run_extra=True, IS_CI_TEST=True)
    print('\n\n=============[Success]==============\n     Finished running full test suite !\n=============[Success]==============\n\n')
