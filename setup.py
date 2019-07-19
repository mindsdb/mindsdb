import setuptools
import subprocess
import sys


def remove_requirements(requirements, name):
    return [x for x in requirements if name != x.split(' ')[0]]

sys_platform = sys.platforms

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

long_description = open('README.md', encoding='utf-8').read()

with open('requirements.txt') as req_file:
    requirements = [req.strip() for req in req_file.read().splitlines()]

dependency_links = []

# Linux specific requirements
if sys_platform == 'linux' or sys_platform.startswith('linux'):
    requirements = remove_requirements(requirements, 'tensorflow-estimator')

# OSX specific requirements
elif sys_platform == 'darwin':
    requirements = requirements

# Windows specific requirements
elif sys_platform in ['win32','cygwin'] :
    requirements = remove_requirements(requirements, 'tensorflow-estimator')
    requirements = remove_requirements(requirements,'wheel')
    requirements.append('wheel == 0.26.0')

# For stuff like freebsd
else:
    print('\n\n====================\n\nError, platform {sys_platform} not recognized, proceeding to install anyway, but lightwood might not work properly !\n\n====================\n\n')

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
