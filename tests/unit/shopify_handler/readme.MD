# Shopify Handler Unit Tests
Shopify Handler has been unit tested using Pytest.

Author: [Varun Dalal](https://github.com/dalalvarun/)
## Setup

Install the test dependencies using the following command:

```bash
pip install -r tests\unit\shopify_handler\requirements.txt
```

## Running the unit tests

You can run the unit tests using the following command:
```bash
pytest -v tests\unit\shopify_handler\
```

## Generating Coverage Report

If you want to see the coverage report, you need to install the pytest-cov dependency as follows:
```bash
pip install pytest-cov
```

Generate the coverage report using the following command:
```bash
pytest tests\unit\shopify_handler --cov=mindsdb\integrations\handlers\shopify_handler --cov-report=html
```

## Coverage Report
<img src="https://u.cubeupload.com/dalalvarun/Screenshot2023102703.jpg" />