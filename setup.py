import setuptools
import sys
import os


def remove_requirements(requirements, name, replace=None):
    new_requirements = []
    for requirement in requirements:
        if requirement.split(' ')[0] != name:
            new_requirements.append(requirement)
        elif replace is not None:
            new_requirements.append(replace)
    return new_requirements

sys_platform = sys.platform

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

long_description = open('README.md', encoding='utf-8').read()

with open('requirements.txt', 'r') as req_file:
    requirements = [req.strip() for req in req_file.read().splitlines()]

extra_data_sources_requirements = []
with open('optional_requirements_extra_data_sources.txt', 'r') as fp:
    for line in fp:
        extra_data_sources_requirements.append(line.rstrip('\n'))

ludwig_model_requirements = []
beta_requirements = []
with open('optional_requirements_ludwig_model.txt', 'r') as fp:
    for line in fp:
        ludwig_model_requirements.append(line.rstrip('\n'))

dependency_links = []

# Linux specific requirements
if sys_platform == 'linux' or sys_platform.startswith('linux'):
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'tensorflow-estimator')

# OSX specific requirements
elif sys_platform == 'darwin':
    requirements = requirements
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'tensorflow', 'tensorflow == 1.13.1')
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'tensorflow-estimator', 'tensorflow-estimator == 1.13.0')
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'ludwig', 'ludwig == 0.1.2')

# Windows specific requirements
elif sys_platform in ['win32','cygwin','windows']:
    requirements = ['cwrap',*requirements]
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'tensorflow', 'tensorflow == 1.13.1')
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'ludwig', 'ludwig == 0.1.2')
    ludwig_model_requirements = remove_requirements(ludwig_model_requirements, 'tensorflow-estimator')
    requirements = remove_requirements(requirements,'wheel', replace='wheel == 0.26.0')

else:
    print('\n\n====================\n\nError, platform {sys_platform} not recognized, proceeding to install anyway, but mindsdb might not work properly !\n\n====================\n\n')


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
    extras_require = {
        'extra_data_sources': extra_data_sources_requirements
        ,'ludwig_model': ludwig_model_requirements
        ,'beta': beta_requirements
    },
    dependency_links=dependency_links,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    python_requires=">=3.6"
)
