
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

