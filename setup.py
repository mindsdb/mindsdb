import os
import glob

from setuptools import find_packages, setup

# A special env var that allows us to disable the installation of the default extras for advanced users / containers / etc
MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS = (
    True
    if os.getenv("MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS", "true").lower() == "true"
    else False
)
DEFAULT_PIP_EXTRAS = [
    line.split("#")[0].rstrip()
    for line in open("default_handlers.txt").read().splitlines()
    if not line.strip().startswith("#")
]


class Deps:
    pkgs = []
    pkgs_exclude = ["tests", "tests.*"]
    new_links = []
    extras = {}


about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)


with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()


def expand_requirements_links(requirements: list) -> list:
    """Expand requirements that contain links to other requirement files"""
    to_add = []
    to_remove = []

    for requirement in requirements:
        if requirement.startswith("-r "):
            if os.path.exists(requirement.split()[1]):
                with open(requirement.split()[1]) as fh:
                    to_add += expand_requirements_links(
                        [req.strip() for req in fh.read().splitlines()]
                    )
            to_remove.append(requirement)

    for req in to_remove:
        requirements.remove(req)
    for req in to_add:
        requirements.append(req)

    return list(set(requirements))  # Remove duplicates


def define_deps():
    """Reads requirements.txt requirements-extra.txt files and preprocess it
    to be feed into setuptools.

    This is the only possible way (we found)
    how requirements.txt can be reused in setup.py
    using dependencies from private github repositories.

    Links must be appendend by `-{StringWithAtLeastOneNumber}`
    or something like that, so e.g. `-9231` works as well as
    `1.1.0`. This is ignored by the setuptools, but has to be there.

    Warnings:
        to make pip respect the links, you have to use
        `--process-dependency-links` switch. So e.g.:
        `pip install --process-dependency-links {git-url}`

    Returns:
         list of packages, extras and dependency links.
    """
    with open(os.path.normpath('requirements/requirements.txt')) as req_file:
        defaults = [req.strip() for req in req_file.read().splitlines()]

    links = []
    requirements = []
    for r in defaults:
        if 'git+https' in r:
            pkg = r.split('#')[-1]
            links.append(r + '-9876543210')
            requirements.append(pkg.replace('egg=', ''))
        else:
            requirements.append(r.strip())

    extra_requirements = {}
    full_requirements = []
    for fn in os.listdir(os.path.normpath('./requirements')):
        extra = []
        if fn.startswith('requirements-') and fn.endswith('.txt'):
            extra_name = fn.replace('requirements-', '').replace('.txt', '')
            with open(os.path.normpath(f"./requirements/{fn}")) as fp:
                extra = [req.strip() for req in fp.read().splitlines()]
            extra_requirements[extra_name] = extra
            full_requirements += extra

    extra_requirements['all_extras'] = list(set(full_requirements))

    full_handlers_requirements = []
    handlers_dir_path = os.path.normpath('./mindsdb/integrations/handlers')
    for fn in os.listdir(handlers_dir_path):
        if os.path.isdir(os.path.join(handlers_dir_path, fn)) and fn.endswith(
            "_handler"
        ):
            extra = []
            for req_file_path in glob.glob(
                os.path.join(handlers_dir_path, fn, "requirements*.txt")
            ):
                extra_name = fn.replace("_handler", "")
                file_name = os.path.basename(req_file_path)
                if file_name != "requirements.txt":
                    extra_name += "-" + file_name.replace("requirements_", "").replace(
                        ".txt", ""
                    )

                # If requirements.txt in our handler folder, import them as our extra's requirements
                if os.path.exists(req_file_path):
                    with open(req_file_path) as fp:
                        extra = expand_requirements_links(
                            [req.strip() for req in fp.read().splitlines()]
                        )

                    extra_requirements[extra_name] = extra
                    full_handlers_requirements += extra

                # Even with no requirements in our handler, list the handler as an extra (with no reqs)
                else:
                    extra_requirements[extra_name] = []

                # If this is a default extra and if we want to install defaults (enabled by default)
                #   then add it to the default requirements needing to install
                if (
                    MINDSDB_PIP_INSTALL_DEFAULT_EXTRAS
                    and extra_name in DEFAULT_PIP_EXTRAS
                    and extra
                ):
                    requirements += extra

    extra_requirements['all_handlers_extras'] = list(set(full_handlers_requirements))

    Deps.pkgs = requirements
    Deps.extras = extra_requirements
    Deps.new_links = links

    return Deps


deps = define_deps()

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
    packages=find_packages(exclude=deps.pkgs_exclude),
    install_requires=deps.pkgs,
    dependency_links=deps.new_links,
    extras_require=deps.extras,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9,<3.12",
)
