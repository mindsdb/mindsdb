# Setup for Docker

## Install Docker

If you haven't done that already, install Docker on your machine following [the instructions](https://docs.docker.com/install). To make sure Docker is successfully installed on your machine, run a test container as follows:

```bash
docker run hello-world
```

You should see the `Hello from Docker!` message. Otherwise, check the [Docker's Get Started](https://www.docker.com/get-started) documentation.

???+ warning "Docker for Mac users - RAM allocation issues"
    By default, Docker for Mac allocates 2 GB of RAM, which is insufficient for deploying MindsDB with Docker. We recommend increasing the default RAM limit to 4 GB. Please refer to the [Docker Desktop for Mac users manual](https://docs.docker.com/desktop/mac/#resources) for more information on how to increase the allocated memory.

## Start MindsDB

Run the command below to start MindsDB in Docker.

```bash
docker run -p 47334:47334 -p 47335:47335 mindsdb/mindsdb
```

Let's analyze this command part by part:

- `#!bash docker run` is a native Docker command used to start a container
- `#!bash -p 47334:47334` publishes port 47334 to access MindsDB GUI and HTTP API
- `#!bash -p 47335:47335` publishes port 47335 to access MindsDB MySQL API
- `#!bash mindsdb/mindsdb` is the container we want to start

## Default Configuration

The default configuration for MindsDB's Docker image is stored as a JSON code, as below.

```json
{
 "config_version":"1.4",
 "storage_dir": "/root/mdb_storage",
 "log": { 
     "level": {
         "console": "ERROR",
	 "file": "WARNING",
	 "db": "WARNING"
	      } 
        },
 "debug": false,
 "integrations": {},
 "api":
  {
   "http": {
       "host": "0.0.0.0",
       "port": "47334"
           },
   "mysql": {
       "host": "0.0.0.0",
       "password": "",
       "port": "47335",
       "user": "mindsdb",
       "database": "mindsdb",
       "ssl": true
            },
   "mongodb": {
       "host": "0.0.0.0",
       "port": "47336",
       "database": "mindsdb"
              }
  }
}
```

## Custom Configuration

To override the default configuration, you can provide values via the `MDB_CONFIG_CONTENT` environment variable, as below.

```bash
docker run -e MDB_CONFIG_CONTENT='{"api":{"http": {"host": "0.0.0.0","port": "8080"}}}' mindsdb/mindsdb
```

## Known Issues

### #1
If you experience any issues related to MKL or your training process does not complete, please add the `#!bash MKL_SERVICE_FORCE_INTEL` environment variable, as below.

```bash
docker run -e MKL_SERVICE_FORCE_INTEL=1 -p 47334:47334 -p 47335:47335 mindsdb/mindsdb
```

!!! tip "What's next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).
