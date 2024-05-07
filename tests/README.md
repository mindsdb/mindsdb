## Test Structure

Our tests are organized into several subdirectories, each focusing on different aspects of our application:

* api: Contains tests related to the MindsDB's API endpoints.
* integration_tests: Contains the integration tests
* load: Contains the load tests
* scripts: Scripts and utilitis used for tests
* unit: This directory contains the unit tests:
        * handlers: A subset of unit tests specifically targeting data handlers.
        * ml_handlers: A subset of unit tests specifically targeting ML handlers.

## Installation

To run the tests, you need to install the necessary dependencies. You can do this by running the following command:

```
pip install -r requirements/requirements.txt -r requirements/requirements-test.txt
```

This command will install all the required packages as listed in requirements-test.txt and the requirements.txt files.

> Note: You will also need to install the dependencies required by any of the integrations that the tests you are running use. This can be done by running `pip install .[<integration_name>]` in the root directory of the repository. 

## Runing tests

### Unit and Integration Tests

To execute unit or integration tests, use the following pytest command:

```
pytest -vx <test_dir>
```

For example, to run the unit tests for the handlers, use the following command:

```
pytest -vx tests/unit/handlers
```

### Load Tests

For load testing, we use `locust`:
```
locust -f test_start.py --headless -u <number_of_users> -r <spawn_rate>
```
Options:

 *   --headless: Runs Locust in headless mode without the web UI.
 *   -u: Specifies the total number of users to simulate.
 *   -r: Defines the spawn rate of users, i.e the number of users to spawn per second.

 > Note: The load tests require an environment variable called `INTEGRATIONS_CONFIG` to be set containing information related to the testing environments used. These environments will also need be pre-loaded with the necessary data for the tests to run successfully.

## Generating Test Reports

We use `pytest` for running tests and `coveralls` for generating test coverage reports. To run the tests and generate a coverage report, use the following command:

```
pytest --cov=<code_dir> <test_dir> && coveralls
```

For example, to run tests for the HTTP API, use the following command:

```
pytest --cov=mindsdb/api/http tests/api/http && coveralls
```

For the above command to be successful, you need to either have the `COVERALLS_REPO_TOKEN` environment variable set to your Coveralls token or have a `.coveralls.yml` file in the root directory of the repository with the token.

Otherwise, you simply run the following command:

```
pytest --cov=<code_dir> <test_dir>
```