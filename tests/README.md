# Unit tests
Unit tests use the [pytest](https://docs.pytest.org/en/latest/index.html) testing framework. [pytest-randomly](https://pypi.org/project/pytest-randomly/) is used to fix random seed for reproducibility.

## Test guidelines
* Each unit test should ideally test just one thing.
* Unit tests should be easy to read, even if it requires some code duplication.
* Unit tests should be as simple as possible, so they don't become a burden to support.

[More good unit testing practices](https://pylonsproject.org/community-unit-testing-guidelines.html).

## Useful commands

Run tests including slow tests:
```pytest --run-slow```

Run tests until first failure:

```pytest -x```

Run last failed test:

```pytest --lf```

Select and run tests by pattern:

```pytest -k <part_of_test_name>```

Run with a different random-seed (by default fixed to 42):

```pytest --randomly-seed=123```

This also randomizes the order of execution of tests.
