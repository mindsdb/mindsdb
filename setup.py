import setuptools
import subprocess

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

long_description = open('README.md', encoding='utf-8').read()

with open('requirements.txt') as req_file:
    requirements = req_file.read().splitlines()

setuptools.setup(
    name=about['__title__'],
    version=about['__version__'],
    url=about['__github__'],
    download_url=about['__pypi__'],
    license=about['__license__'],
    author=about['__author__'],
    author_email=about['__email__'],
    description=about['__description__'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    python_requires=">=3.6"
)

try:
    subprocess.call(['python3','-m','spacy','download','en'])
except:
    subprocess.call(['python','-m','spacy','download','en'])
