import setuptools
import subprocess
import platform


def remove_requirement(requirements, name):
    return [x for x in requirements if name != x.split(' ')[0]]

os = platform.system()

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

long_description = open('README.md', encoding='utf-8').read()

with open('requirements.txt') as req_file:
    requirements = req_file.read().splitlines()

dependency_links = []

# Linux specific requirements
if os == 'Linux':
    requirements = remove_requirement(requirements, 'tensorflow-estimator')
    requirements = remove_requirement(requirements, 'lightwood')
    requirements.append('lightwood == 0.6.7')

# OSX specific requirements
if os == 'Darwin':
    requirements = requirements

# Windows specific requirements
if os == 'Windows':
    requirements = remove_requirement(requirements, 'tensorflow-estimator')
    requirements = remove_requirement(requirements,'wheel')
    requirements.append('wheel == 0.26.0')

    requirements = remove_requirement(requirements, 'lightwood')
    requirements.append('lightwood == 0.7.1')
    dependency_links.append('https://github.com/mindsdb/lightwood/tarball/ci_testing#egg=lightwood-0.7.1')

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
    dependency_links=dependency_links,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    python_requires=">=3.6"
)

try:
    subprocess.call(['python3','-m','spacy','download','en_core_web_sm'])
except:
    subprocess.call(['python','-m','spacy','download','en_core_web_sm'])

try:
    subprocess.call(['python3','-m','spacy','download','en'])
except:
    subprocess.call(['python','-m','spacy','download','en'])
