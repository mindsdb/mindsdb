## Manual QA Testing for EdgelessDB Handler

# Explaining the test

The test is performed on a local machine.EdgelessDB works on top of a MySQL database. So, we have to install MySQL
first. Then, we have to install EdgelessDB. After that, we have to create a database in EdgelessDB. Finally, we have to
test the database by creating a table and inserting some data into it.
EdgelessDB is designed to work for SGX enviroment. Because i don't have SGX enviroment, i have to run EdgelessDB in
simulation mode. In simulation mode, EdgelessDB runs as a normal MySQL database. So is important to note that the test
is performed on a normal MySQL database.

1. Testing CREATE DATABASE without encryption option of EdgelessDB

```sql
CREATE DATABASE edgelessdb_datasource
    WITH ENGINE = "EdgelessDB",
    PARAMETERS = {
    "user": "root",
    "password": "test123@!Aabvhj",
    "host": "localhost",
    "port": 3306,
    "database": "test_schema"
    }
```

The result is as follows:
[![Screenshot-from-2023-07-13-17-03-04.png](https://i.postimg.cc/fyM3Q5TV/Screenshot-from-2023-07-13-17-03-04.png)](https://postimg.cc/qhZB8sKr)

2. Testing CREATE DATABASE with encryption option of EdgelessDB

```sql
CREATE DATABASE edgelessdb_datasource2
    WITH ENGINE = "EdgelessDB",
    PARAMETERS = {
    "user": "root",
    "password": "test123@!Aabvhj",
    "host": "localhost",
    "port": 3306,
    "database": "test_schema",
    "ssl_cert": "/home/marios/demo/cert.pem",
    "ssl_key": "/home/marios/demo/key.pem"

    }
```

The result is as follows:
[![Screenshot-from-2023-07-13-17-08-16.png](https://i.postimg.cc/mg7G74SG/Screenshot-from-2023-07-13-17-08-16.png)](https://postimg.cc/LJ47Mr7y)
...

n. Testing SELECT FROM edgelessdb_datasource

```sql
select *
from edgelessdb_datasource2.meetings
```

`
[![Screenshot-from-2023-07-13-17-11-08.png](https://i.postimg.cc/vHHzWtL8/Screenshot-from-2023-07-13-17-11-08.png)](https://postimg.cc/3WVmhpcz)

### Results

Drop a remark based on your observation.

- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] Further Testing Required 🟡 (This means that the test was not performed under every possible scenario. It is
  recommended to perform also th tests in an SGX compatible system and also in Microsoft Azure.)

---