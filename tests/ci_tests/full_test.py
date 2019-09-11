from tests import basic_test


# Cycle through a few options:
first = True
for backend in ['lightwood','ludwig']:
    for use_gpu in [False,True]:
        print(f'use_gpu is set to {use_gpu}, backend is set to {backend}')
        basic_test(backend=backend,use_gpu=use_gpu,ignore_columns=[],run_extra=first)
        first = False
# Try ignoring some columns
basic_test(backend='lightwood',use_gpu=True,ignore_columns=['days_on_market','number_of_bathrooms'])
print('\n\n=============[Success]==============\n     Finished running full test suite !\n=============[Success]==============\n\n')
