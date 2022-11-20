from setuptools import setup, find_packages
from mindsdb.integrations.handlers.mlflow_handler.__about__ import __title__, __version__, __github__, __pypi__, __license__, __author__, __description__

with open('requirements.txt') as req_file:
    requirements = [req.strip() for req in req_file.read().splitlines()]

setup(
    name=__title__,
    version=__version__,
    url=__github__,
    download_url=__pypi__,
    license=__license__,
    author=__author__,
    description=__description__,
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8"
)
