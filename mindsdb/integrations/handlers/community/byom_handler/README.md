
## BYOM Handler

### Http api:

**POST** api/handlers/byom/<engine_name>

Uploaded files
- code - python file with model code
- modules - text file with requirements

**Logic:**

1. Create new ml engine
2. If virtualenv is installed on computer:
   - try to create a new virtual environment using current python interpreter
   - install all requirements for model
     - if pandas is not in requirements - it is installed anyway 
   - install pyarrow (for sending dataframe between different pandas versions)
3. If not virtualenv is installed:
   - try to install requirements to current environment
4. Try to import uploaded model and check predict and train methods
5. If not success:
   - remove virtual environment if it was created
   - remove ml engine

Using:

1. Add args argument to train and predict methods:
```
class CustomPredictor():
    def train(self, df, target_col, args=None):
       ...
       
    def predict(self, df, args=None):
       ...
```

2. Passing args to model training
``` 
create predictor pred 
from files (select * from byom)
predict Time
using engine='uploaded_model',
  param1=1, param2='2'
```

3. Passing args at predict
```
select * from files.byom t 
join pred3 p
using param1=1, param2='2'
```

