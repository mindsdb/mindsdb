<div align="center">

![MindsDB](https://raw.githubusercontent.com/mindsdb/mindsdb/master/mindsdb/proxies/web/static/img/logo1gw.png 'MindsDB')

#

[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/mindsdb/mindsdb/issues)
[![Build Status](https://travis-ci.org/mindsdb/mindsdb.svg?branch=master)](https://travis-ci.org/mindsdb/mindsdb)
[![PyPI version](https://badge.fury.io/py/mindsdb.svg)](https://badge.fury.io/py/mindsdb)
[![Feature Requests](https://img.shields.io/github/issues/mindsdb/mindsdb/enhancement.svg)](https://github.com/mindsdb/mindsdb/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement+sort%3Areactions-%2B1-desc)
[![Bugs](https://img.shields.io/github/issues/mindsdb/mindsdb/bug.svg)](https://github.com/mindsdb/mindsdb/issues?utf8=✓&q=is%3Aissue+is%3Aopen+label%3Abug)
[![contributors](https://img.shields.io/github/contributors/mindsdb/mindsdb.svg)](https://github.com/webpack/webpack/graphs/contributors)
[![The MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](http://opensource.org/licenses/MIT)

MindsDB's goal is to make it very simple for developers to use the power of artificial neural networks in their projects. [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20in%20one%20line%20of%20code%21&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks)

</div>

## Contents

- [Quick Overview](#Quick-Overview)
- [Installing MindsDB](docs/Installing.md)
- [Learning from Examples](docs/examples/basic/README.md)
- [Frequently Asked Questions](docs/FAQ.md)
- [Provide feedback to improve MindsDB](https://mindsdb.typeform.com/to/c3CEtj)
- [How to Contribute](##Contributing)
- [Report Issues](##Report-Issues)
- [Lincense](##License)

## Quick Overview

You can get started in your own computer/cloud or you can also try it via your browser using [Google Colab](docs/GoogleColab.md).

It's very simple to set up

```bash
 pip3 install mindsdb --user
```

[(Having issues? learn more)](docs/Installing.md)

Once you have MindsDB installed, you can use it as follows [(learn more)](docs/examples/basic/README.md):

To **train a model**:

```python

from mindsdb import *


# We tell mindsDB what we want to learn and from what data
MindsDB().learn(
    from_data="home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    predict='rental_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)

```

To **use the model**:

```python

from mindsdb import *

# use the model to make predictions
result = MindsDB().predict(predict='rental_price', when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190}, model_name='home_rentals')

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result.predicted_values[0]['rental_price'], conf=result.predicted_values[0]['prediction_confidence']))

```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Report Issues

Please help us by reporting any issues you may have while using MindsDB.

[New issue](https://github.com/mindsdb/mindsdb/issues/new/choose)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
