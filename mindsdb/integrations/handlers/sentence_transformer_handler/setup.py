from setuptools import find_packages, setup

pkgs, new_links = install_deps()

about = {}
with open("__about__.py") as fp:
    exec(fp.read(), about)

with open("requirements.txt") as req_file:
    requirements = [req.strip() for req in req_file.read().splitlines()]

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
    packages=find_packages(),
    install_requires=pkgs,
    dependency_links=new_links,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
