import torch
import mindsdb
from tests import basic_test


if __name__ == "__main__":
    use_gpu_settings = []
    if torch.cuda.is_available():
        use_gpu_settings.append(True)

    use_gpu_settings.append(False)

    # Try ignoring some columns and running only the stats generator
    ran_extra = True

    # Cycle through a few options:
    for backend in ['lightwood']: #,'ludwig'
        for use_gpu in use_gpu_settings:
            print(f'use_gpu is set to {use_gpu}, backend is set to {backend}')
            if run_extra:
                basic_test(backend=backend,use_gpu=use_gpu,ignore_columns=['days_on_market','number_of_bathrooms'], IS_CI_TEST=True, run_extra=True)
                run_extra = False
            else:
                basic_test(backend=backend,use_gpu=use_gpu,ignore_columns=[], IS_CI_TEST=True)

    print('\n\n=============[Success]==============\n     Finished running full test suite !\n=============[Success]==============\n\n')
