import os
from setuptools import setup, find_packages


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
    with open('requirements.txt') as req_file:
        defaults = [req.strip() for req in req_file.read().splitlines()]

    links = []
    requirements = []
    for r in defaults:
        if 'git+https' in r:
            pkg = r.split('#')[-1]
            links.append(r + '-9876543210')
            requirements.append(pkg.replace('egg=', ''))
        else:
            requirements.append(r)

    extra_requirements = {}
    for fn in os.listdir('.'):
        if fn.startswith('requirements-') and fn.endswith('.txt'):
            extra_name = fn.replace('requirements-', '').replace('.txt', '')
            with open(fn) as fp:
                extra = [req.strip() for req in fp.read().splitlines()]
            extra_requirements[extra_name] = extra
    full_requirements = []
    for v in extra_requirements.values():
        full_requirements += v
    extra_requirements['all_extras'] = list(set(full_requirements))

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
    python_requires=">=3.8"
)
