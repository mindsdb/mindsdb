# Introduction

The docker-compose option will build a mindsdb container along with multiple database containers. All containers will be available on the host network.

The databases will retain their data, locally. Feel free to take the containers offline when needed. The next time you start up the containers your data will be utilized in the container.

# Instructions

All commands assume you are in the following directory:
```mindsdb/distributions/docker-compose/```

1. Open the docker-compose and comment any database block that you don't want to run within a container.
2. Save the file.
3. Open a terminal in this directory, where docker-compose.yaml resides.
4. Run the following: ```docker-compose up```
   * If you don't want to see the logs streaming, instead run: ```docker-compose up -d```
5. When you are ready to take the containers offline, run: ```docker-compose down```

# Container Issues

If you find you are having issues with your containers after multiple starts and stops, you can quickly by-pass the issue by removing your docker images.

>Caution: This should be reseved as a last resort

All commands assume you are using a terminal with the following directory:
```mindsdb/distributions/docker-compose/```

1. Run: ```docker images```
2. You will see a list of images similar to:
   * docker-compose_postgres
   * docker-compose_mariadb
   * mariadb
   * mysql
   * mindsdb/mindsdb
   * postgres
   * yandex/clickhouse-server
3. Delete each image.
4. Next run: ```docker-compose up```
5. If you see a message request a yes/no response, asking to overwrite a volume, you can say yes. Your data will be restored from local docker volumes.

# Feature and Bug reports
We use GitHub issues to track bugs and features. Report them by opening a [new issue](https://github.com/mindsdb/mindsdb/issues/new/choose) and fill out all of the required inputs.

# Code review process
The Pull Request reviews are done on a regular basis. 
Please, make sure you respond to our feedback/questions.

# Community
If you have additional questions or you want to chat with MindsDB core team, you can join our community [![Discourse posts](https://img.shields.io/discourse/posts?server=https%3A%2F%2Fcommunity.mindsdb.com%2F)](https://community.mindsdb.com/). To get updates on MindsDB’s latest announcements, releases, and events, [sign up for our newsletter](https://mindsdb.us20.list-manage.com/subscribe/post?u=5174706490c4f461e54869879&amp;id=242786942a).