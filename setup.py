import os
import glob
from setuptools import find_packages, setup

# A special env var that allows us to disable the installation of the default extras for advanced users / containers / etc
MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS = os.getenv("MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS", "true").lower() == "true"

DEFAULT_PIP_EXTRAS = []
if os.path.exists("default_handlers.txt"):
    with open("default_handlers.txt") as f:
        DEFAULT_PIP_EXTRAS = [line.split("#")[0].rstrip() for line in f if not line.strip().startswith("#")]

class Deps:
    pkgs = []
    pkgs_exclude = ["tests", "tests.*"]
    extras = {}

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

def expand_requirements_links(requirements: list) -> list:
    """Expand requirements that contain links to other requirement files"""
    expanded_requirements = []
    for requirement in requirements:
        if requirement.startswith("-r "):
            req_file = requirement.split()[1]
            if os.path.exists(req_file):
                with open(req_file) as fh:
                    expanded_requirements += expand_requirements_links(
                        [req.strip() for req in fh.read().splitlines()]
                    )
        else:
            expanded_requirements.append(requirement)
    return list(set(expanded_requirements))  # Remove duplicates

def define_deps():
    """Reads requirements.txt requirements-extra.txt files and preprocess it to be fed into setuptools."""
    requirements = []
    if os.path.exists('requirements/requirements.txt'):
        with open(os.path.normpath('requirements/requirements.txt')) as req_file:
            requirements = [req.strip() for req in req_file.read().splitlines()]

    extra_requirements = {}
    all_extra_reqs = []

    for fn in os.listdir(os.path.normpath('./requirements')):
        if fn.startswith('requirements-') and fn.endswith('.txt'):
            extra_name = fn.replace('requirements-', '').replace('.txt', '')
            with open(os.path.normpath(f"./requirements/{fn}")) as fp:
                extra = [req.strip() for req in fp.read().splitlines()]
            extra_requirements[extra_name] = extra
            all_extra_reqs += extra

    extra_requirements['all_extras'] = list(set(all_extra_reqs))

    handlers_requirements = []
    handlers_dir_path = os.path.normpath('./mindsdb/integrations/handlers')
    if os.path.exists(handlers_dir_path):
        for fn in os.listdir(handlers_dir_path):
            if os.path.isdir(os.path.join(handlers_dir_path, fn)) and fn.endswith("_handler"):
                extra_name = fn.replace("_handler", "")
                for req_file_path in glob.glob(os.path.join(handlers_dir_path, fn, "requirements*.txt")):
                    file_name = os.path.basename(req_file_path)
                    extra_subname = file_name.replace("requirements_", "").replace(".txt", "")
                    if extra_subname != "":
                        extra_name += f"-{extra_subname}"
                    with open(req_file_path) as fp:
                        extra = expand_requirements_links([req.strip() for req in fp.read().splitlines()])
                    extra_requirements[extra_name] = extra
                    handlers_requirements += extra

                if MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS and extra_name in DEFAULT_PIP_EXTRAS:
                    requirements += extra

    extra_requirements['all_handlers_extras'] = list(set(handlers_requirements))

    Deps.pkgs = requirements
    Deps.extras = extra_requirements

define_deps()

setup(
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
    packages=find_packages(exclude=Deps.pkgs_exclude),
    install_requires=Deps.pkgs,
    extras_require=Deps.extras,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8,<3.11",
)
