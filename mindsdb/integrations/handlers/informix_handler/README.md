# IBM Informix Handler

This is the implementation of the IBM Informix handler for MindsDB.

## IBM Informix
IBM Informix is a product family within IBM's Information Management division that is centered on several relational database management system (RDBMS) offerings.The Informix server supports the object–relational model and supports (through extensions) data types that are not a part of the SQL standard. The most widely used of these are the JSON, BSON, time series and spatial extensions, which provide both data type support and language extensions that permit high performance domain specific queries and efficient storage for data sets based on semi-structured, time series, and spatial data. 

## Implementation
This handler was implemented using the `IfxPy/IfxPyDbi`, a Python library that allows you to use Python code to run SQL commands on DB2 Database.

The required arguments to establish a connection are,
* `user`: username asscociated with database
* `password`: password to authenticate your access
* `host`: host to server IP Address or hostname
* `port`: port through which TCPIP connection is to be made
* `database`: Database name to be connected
* `schema_name`: schema name to get tables 
* `server`: Name of server you want connect
* `loging_enabled`: Is loging is enabled or not. Default is True

## Usage
In order to make use of this handler and connect to Informix in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE informix_datasource
WITH
engine='informix',
parameters={
        "server": "server",
        "host": "127.0.0.1",
        "port": 9091,
        "user": "informix",
        "password": "in4mix",
        "database": "stores_demo",
        "schema_name": "love",
        "loging_enabled": False
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM informix_datasource.items;
~~~~


This integration uses IfxPy it is in develpment stage there it can be install using `pip install IfxPy`.But it doesn't work for higher version of python, therfore you have to build it from source.

<br>

# You can use below methods 
<br>



<details> 
  <summary> Check out For Linux </summary>

Below code download and extracts onedb-ODBC Driver use to make connection 


```bash

cd $HOME
mkdir Informix
cd Informix
mkdir -p home/informix/cli
wget https://hcl-onedb.github.io/odbc/OneDB-Linux64-ODBC-Driver.tar
sudo tar xvf  OneDB-Linux64-ODBC-Driver.tar -C ./home/informix/cli
rm OneDB-Linux64-ODBC-Driver.tar

```

* After running  above command you need to go in `.bashrc` file and add enviroment variable there

```bash
export INFORMIXDIR = $HOME/Informix/home/informix/cli/onedb-driver
export LD_LIBRARY_PATH=${INFORMIXDIR}/lib:${INFORMIXDIR}/lib/esql:${INFORMIXDIR}/lib/cli
```
* Now you are done with setting Enviroment variable.
* Running below command clone IfxPy repo , build a wheel and install it .

```bash

pip install wheel
mkdir Temp
cd Temp
git clone https://github.com/OpenInformix/IfxPy.git
cd IfxPy/IfxPy
python setup.py bdist_wheel
pip install --find-links=./dist IfxPy
cd ..
cd ..
cd ..
rm -rf Temp



```


</details>

<details> 
  <summary> Check out For Windows </summary>

> Run Below Given Commands in CMD

```cmd
 cd $HOME
mkdir Informix
cd Informix
mkdir  /home/informix/cli
wget https://hcl-onedb.github.io/odbc/OneDB-Win64-ODBC-Driver.zip
tar xvf  OneDB-Win64-ODBC-Driver.zip -C ./home/informix/cli
del OneDB-Win64-ODBC-Driver.zip
```

* Above code will Download, Extract OneDB ODBC zip file.
* You need to add THis To ENViroment Variable
* `**set INFORMIXDIR=$HOME/Informix/home/informix/cli/onedb-driver**`
* Add  **`%INFORMIXDIR%\bin to PATH`**

* Below code will clone, build and install wheel

pip install wheel
mkdir Temp
cd Temp
git clone https://github.com/OpenInformix/IfxPy.git
cd IfxPy/IfxPy
python setup.py bdist_wheel
pip install --find-links=./dist IfxPy
cd ..
cd ..
cd ..
rmdir Temp

</details>

<br>

> For more Info checkout [here](https://github.com/OpenInformix/IfxPy) also it has some prerequisite.

> There are many method for Build but wheel method easy and Recommended.

