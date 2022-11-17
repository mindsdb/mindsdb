from setuptools import setup, find_packages

from setup import install_deps
from .__about__ import __dict__ as about

pkgs, new_links = install_deps()

setup(
    name=about['__title__'],
    version=about['__version__'],
    url=about['__github__'],
    download_url=about['__pypi__'],
    license=about['__license__'],
    author=about['__author__'],
    author_email=about['__email__'],
    description=about['__description__'],
    long_description=F"{about['__title__']} version:{about['__version__']}",
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=pkgs,
    dependency_links=new_links,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7"
)
