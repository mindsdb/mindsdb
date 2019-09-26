---
id: installing-mindsdb
title: Installing MindsDB
---

## Prerequisites

Before you begin, you need [**python>=3.7**](https://realpython.com/installing-python/) or [**Conda Python3**](https://www.anaconda.com/download/), and make sure you have the **latest pip3**.

```bash
curl https://bootstrap.pypa.io/get-pip.py | python3
pip3 install --upgrade pip
```

Once you have those, you can install MindsDB.

## On Mac or Linux or any other operating system

Install MindsDB:

```bash
pip3 install mindsdb --user --no-cache-dir
```

It might be needed that you need to install `tkinter` from your package manager in certain situations.

- Ubuntu/Debian: `sudo apt-get install python3-tk tk`
- Fedora: `sudo dnf -y install python3-tkinter`
- Arch: `sudo pacman -S tk`

## On Windows 10

Install Conda [download here](https://www.anaconda.com/download/#windows)
and then run the **anaconda prompt**:

```bash
pip install git+https://github.com/mindsdb/mindsdb.git@master --upgrade --force-reinstall
```
# Build and run your docker container

Alternatively, you can also run MindsDB in a docker container. Assuming that you have [docker](https://docs.docker.com/install/) installed in your computer.
On your terminal, you can do the following:

```
sh -c "$(curl -sSL https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/build-docker.sh)"

```

# Hardware

Due to the fact that pytorch only supports certain instruction sets, mindsdb can only use certain types of GPUs.
Currently, on AWS, `g3` and `p3` instance types should be fine, but `p2` and `g2` instances are not supported
