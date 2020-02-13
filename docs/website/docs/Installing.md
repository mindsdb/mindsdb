---
id: installing-mindsdb
title: Installing MindsDB
---

## Prerequisites

Before you begin, you need [**python>=3.7**](https://realpython.com/installing-python/) or [**Conda Python3**](https://www.anaconda.com/download/), and make sure you have the **latest pip3**.

To install python & pip:

```bash
curl https://bootstrap.pypa.io/get-pip.py | python3
pip3 install --upgrade pip
```

To install conda (only required on some windows environments) [download conda from here](https://www.anaconda.com/download/#windows)
Once that's done run the **anaconda prompt**.


Once that's done, you can install mindsdb from your terminal or from the **anaconda prompt**.

## Standard installation

Install MindsDB:

```bash
pip install mindsdb
```

## What to do if installation fails

1. Try using `pip3` instead of `pip` (`pip3 install mindsdb`) and try installing for your current user only (`pip install mindsdb --user`)

2. Try manually installing pytorch following the simple instructions on their official website: https://pytorch.org/get-started/locally/

3. If you are using linux install `tkinter` from your package manager in certain situations.

- Ubuntu/Debian: `sudo apt-get install python3-tk tk`
- Fedora: `sudo dnf -y install python3-tkinter`
- Arch: `sudo pacman -S tk`

4. If you are using windows, but are not using Conda, try installing conda and running the installation from the **anaconda prompt**.

5. If you've previously installed mindsdb and are having issues upgrading to a new version, try installing with the command: `pip install mindsdb --upgrade`, if that still fails, try: `pip install mindsdb --no-cache-dir --force-reinstall`.

6. If none of this works, try installing mindsdb using the docker container and create an issue with the installation errors you got on: https://github.com/mindsdb/mindsdb/issues, we'll try to review it within a few hours.

## Build and run your docker container

Alternatively, you can also run MindsDB in a docker container. Assuming that you have [docker](https://docs.docker.com/install/) installed in your computer.
On your terminal, you can do the following:

```
sh -c "$(curl -sSL https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/build-docker.sh)"

```

## Hardware

Due to the fact that pytorch only supports certain instruction sets, mindsdb can only use certain types of GPUs.
Currently, on AWS, `g3` and `p3` instance types should be fine, but `p2` and `g2` instances are not supported
