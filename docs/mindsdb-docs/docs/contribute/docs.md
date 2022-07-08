# MindsDB Documentation

This section will walk you through installing the docs locally. MindsDB documentation is built using [MkDocs](https://github.com/mkdocs/mkdocs) and 
use [Material theme for MkDocs](https://squidfunk.github.io/mkdocs-material/). The source code is located under [docs directory](https://github.com/mindsdb/mindsdb/tree/staging/docs) in MindsDB repo. 

Documentation can always be improved so we don't have a strict guideliness to follow. We accept any sort of documentation and tutorials improvments, adding missing documentation, new tutorials or keep up the documentation up to date.

## Running the docs locally

First install the mkdocs and mkdocs-material theme in your python virtual environment:
```
pip install -r requirements.txt
```
Then, navigate to the `/mindsdb-docs` directory and start the server:

```
mkdocs serve
```

The documentation website will be available at `http://127.0.0.1:8000`


## Repository structure

The mindsdb-docs layout is as follows:

```
docs                                   # Contains documentation source files
|__assets/                             # Image and icons used in pages
│  ├─ images/                          # Images and icons
│__stylesheets/                        # CSS
|__.md                                 # All of the markdown files used as pages
overrides
├─ partials/
│  ├─ footer.html                      # Footer bar(empty)
│  ├─ header.html                      # Header and navigation bar
└─ main.html                           # Main page used for adding script blocks
.mkdocs.yml                            # Mkdocs configuration file
```

