# DockerHub Handler

DockerHub handler for MindsDB provides interfaces to connect to DockerHub via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [DockerHub Handler](#dockerhub-handler)
  - [Table of Contents](#table-of-contents)
  - [About DockerHub](#about-dockerhub)
  - [DockerHub Handler Implementation](#dockerhub-handler-implementation)
  - [DockerHub Handler Initialization](#dockerhub-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About DockerHub

Docker Hub is the world's easiest way to create, manage, and deliver your team's container applications.


## DockerHub Handler Implementation

This handler was implemented using the `requests` library that makes http calls to https://docs.docker.com/docker-hub/api/latest/#tag/resources.

## DockerHub Handler Initialization

The DockerHub handler is initialized with the following parameters:

- `username`: Username used to login to DockerHub
- `password`: Password used to login to DockerHub

Read about creating an account [here](https://hub.docker.com/).

## Implemented Features

- [x] DockerHub Repo Images Summary for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Repository's Images Table for a given Repository
- [ ] Image's Tags Table for a given Image
- [ ] Repository Tags Table for a given Repository
- [ ] Repository Tag Table for a given Repository

## Example Usage

The first step is to create a database with the new `dockerhub` engine. 

~~~~sql
CREATE DATABASE mindsdb_dockerhub
WITH ENGINE = 'dockerhub',
PARAMETERS = {
  "username": "user",
  "password": "pass"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_dockerhub.repo_images_summary WHERE namespace="docker" AND repository="trusted-registry-nginx";
~~~~