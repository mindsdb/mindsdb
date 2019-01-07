[<Back to Table of Contents](../README.md)

* [Installing MindsDB on your environment](Installing.md#installing-mindsdb)
* [Use MindsDB in the cloud with Google Colab](GoogleColab.md)

Alternatively you can:

* [Build your MindsDB docker container](../distributions/docker)
* [Build your AWS container](../distributions/aws)

# Installing MindsDB


Before you begin, you need **[python>=3.7](https://realpython.com/installing-python/)** or **[Conda Python3](https://www.anaconda.com/download/)**, and make sure you have the **latest pip3**
```bash
curl https://bootstrap.pypa.io/get-pip.py | python3
pip3 install --upgrade pip
```

Once that, you can install MindsDB
##### On Mac or Linux 

```bash
pip3 install mindsdb --user
```

##### On Windows 10


Install Conda [download here](https://www.anaconda.com/download/#windows).
 
 and then run the **anaconda prompt**: 

```bash
conda install -c peterjc123 pytorch
conda install -c blaze blaze
conda install -c blaze sqlite3
curl -o reqs.txt https://raw.githubusercontent.com/mindsdb/mindsdb/master/requirements-win.txt
pip install --requirement reqs.txt
pip install mindsdb --no-dependencies
```

# Build and run your docker container

Assuming that you have [docker](https://docs.docker.com/install/) installed in your computer.
on your terminal, you can do the following:

```
sh -c "$(curl -sSL https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/build-docker.sh)"

```

