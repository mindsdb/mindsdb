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
pip install -r requirements/requirements-test.txt
```

> Note: You will also need MindsDB's dependencies and the handler dependencies pre-installed.

This command will install all the required packages as listed in requirements-test.txt and the requirements.txt files.

## Runing tests

### Unit and Integration Tests

To execute unit or integration tests, use the following pytest command:

```
pytest -vx test_dir
```

### Load Tests

For load testing, we use Locust:
```
locust -f <test_directory> --headless -u <number_of_users> -r <spawn_rate>
```
Options:

 *   --headless: Runs Locust in headless mode without the web UI.
 *   -u: Specifies the number of users to simulate.
 *   -r: Defines the spawn rate of users.

## Generating Test Reports

We use pytest for running tests and coveralls for generating test coverage reports. To run the tests and generate a coverage report, use the following command:

```
pytest --cov=./ test_dir && coveralls
```

