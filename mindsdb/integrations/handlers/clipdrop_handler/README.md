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


## Example Usage

The first step is to create a database with the new `dockerhub` engine. 

~~~~sql
CREATE DATABASE mindsdb_clipdrop
WITH ENGINE = 'clipdrop',
PARAMETERS = {
  "api_key": "api_key",
  "dir_to_save": "/Users/Sam/Documents/test/"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_clipdrop.remove_text WHERE img_url="https://static.vecteezy.com/system/resources/thumbnails/022/721/714/small/youtube-logo-for-popular-online-media-content-creation-website-and-application-free-png.png";
~~~~

~~~~sql
SELECT * FROM mindsdb_dockerhub.repo_images WHERE namespace="docker" AND repository="trusted-registry-nginx";
~~~~

~~~~sql
SELECT * FROM mindsdb_dockerhub.repo_tag_details WHERE namespace="docker" AND repository="trusted-registry-nginx" AND tag="latest";
~~~~

~~~~sql
SELECT * FROM mindsdb_dockerhub.repo_tags WHERE namespace="docker" AND repository="trusted-registry-nginx";
~~~~

=======
SELECT * FROM mindsdb_dockerhub.org_settings where organization="docker";
~~~~
