import glob
import os

from setuptools import find_packages, setup


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
    with open(os.path.normpath("requirements/requirements.txt")) as req_file:
        defaults = [req.strip() for req in req_file.read().splitlines()]

    links = []
    requirements = []
    for r in defaults:
        if "git+https" in r:
            pkg = r.split("#")[-1]
            links.append(r + "-9876543210")
            requirements.append(pkg.replace("egg=", ""))
        else:
            requirements.append(r.strip())

    extra_requirements = {}
    full_requirements = []
    for fn in os.listdir(os.path.normpath("./requirements")):
        if fn.startswith("requirements-") and fn.endswith(".txt"):
            extra_name = fn.replace("requirements-", "").replace(".txt", "")
            with open(os.path.normpath(f"./requirements/{fn}")) as fp:
                extra = [req.strip() for req in fp.read().splitlines()]
            extra_requirements[extra_name] = extra
            full_requirements += extra

    extra_requirements["all_extras"] = list(set(full_requirements))

    full_handlers_requirements = []
    handlers_dir_path = os.path.normpath("./mindsdb/integrations/handlers")
    for fn in os.listdir(handlers_dir_path):
        if os.path.isdir(os.path.join(handlers_dir_path, fn)) and fn.endswith(
            "_handler"
        ):
            for req_file_path in glob.glob(
                os.path.join(handlers_dir_path, fn, "requirements*.txt")
            ):
                with open(req_file_path) as fp:
                    extra = [req.strip() for req in fp.read().splitlines()]
                extra_name = fn.replace("_handler", "")
                file_name = os.path.basename(req_file_path)
                if file_name != "requirements.txt":
                    extra_name += "_" + file_name.replace("requirements_", "").replace(
                        ".txt", ""
                    )
                extra_requirements[extra_name] = extra
                full_handlers_requirements += extra

    extra_requirements["all_handlers_extras"] = list(set(full_handlers_requirements))

    Deps.pkgs = requirements
    Deps.extras = extra_requirements
    Deps.new_links = links

    return Deps


deps = define_deps()

setup(
    name=about["__title__"],
    version=about["__version__"],
    url=about["__github__"],
    download_url=about["__pypi__"],
    license=about["__license__"],
    author=about["__author__"],
    author_email=about["__email__"],
    description=about["__description__"],
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
    python_requires=">=3.8",
)
