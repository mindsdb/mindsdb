# Deploy using Docker

To use MindsDB's Docker Container you first need to have <a href="https://docs.docker.com/install" target="_blank">Docker</a> installed on your machine. To make sure Docker is successfully installed on your machine, run:

```
docker run hello-world
```

You should see the `Hello from Docker!` message displayed. If not, check the <a href="https://www.docker.com/get-started" target="_blank">Get Started</a> documentation.


### MindsDB container

MindsDB images are uploaded to the <a href="https://hub.docker.com/u/mindsdb" target="_blank">MindsDB repo on docker hub</a> after each release.


#### Pull image

First, run the below command to pull our latest production image:

```
docker pull mindsdb/mindsdb
```

Or, to try out the latest beta version, pull the beta image:

```
docker pull mindsdb/mindsdb_beta
```

#### Start container

Next, run the below command to start the container:

```
docker run -p 47334:47334 -p 47335:47335 mindsdb/mindsdb
```

![Docker run](/assets/docker-install.gif)

That's it. MindsDB GUI should be avaiable on [http://127.0.0.1:47334/](http://127.0.0.1:47334/) on your default browser.


### 



## Troubleshooting

!!! info "Extend config.json"
	If you want to extend the default configuration, you will be able to send the config.json value as JSON string argument to the MDB_CONFIG_CONTENT as:
	```
	docker run -e MDB_CONFIG_CONTENT='{"api":{"http": {"host": "0.0.0.0","port": "47334"}}}' mindsdb/mindsdb
	```
	Or, you can pipe `<`  the content of the file to the MDB_CONFIG_CONTENT.

!!! warning "MKL Issues"
	Note. If you experience any issue related to MKL or if your training process does not complete, please add env var or starter Docker with this command:
	```
	docker run --env MKL_SERVICE_FORCE_INTEL=1 -it -p 47334:47334 mindsdb/mindsdb
	```
!!! warning "Docker for Mac users"

	By default, Docker for Mac allocates __2.00GB of memory__. This is insufficient for deploying to docker. We recommend increasing the default memory limit to __4.00GB__. 

	Please refer to [https://docs.docker.com/desktop/mac/#resources](https://docs.docker.com/desktop/mac/#resources) for more information on how to increase the allocated memory.