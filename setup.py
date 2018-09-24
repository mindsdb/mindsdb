import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="mindsdb",
    version="0.6.9",
    author="MindsDB Inc",
    author_email="jorge@mindsdb.com",
    description="MindsDB's goal is to make it very simple for developers to use the power of artificial neural networks in their projects. ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mindsdb/main",
    packages=setuptools.find_packages(),
    install_requires=[
        'tinydb',
        'tinymongo',
        'autobahn',
        'twisted',
        'pandas',
        'numpy',
        'scipy',
        'sklearn',
        'python-dateutil',
        'pymongo',
        'click',
        'torch',
        'torchvision',
        'flask',
        'flask_socketio'
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)