#!/bin/bash
if [ $1 = "mariadb" ]; then
    echo "prepare"
    mkdir -p mariadb/jars/jdbc/
    if [ ! -f "mariadb/jars/jdbc/dremio-jdbc-driver-4.2.2-202004211133290458-b550b6fa.jar" ]; then
        wget -P mariadb/jars/jdbc/ http://download.dremio.com/jdbc-driver/4.2.2-202004211133290458-b550b6fa/dremio-jdbc-driver-4.2.2-202004211133290458-b550b6fa.jar
    fi
    mkdir -p mariadb/jars/wrapper/
    if [ ! -f "mariadb/jars/wrapper/JavaWrappers.jar" ]; then
        # wget -P mariadb/jars/wrapper/ https://jira.mariadb.org/secure/attachment/44179/JavaWrappers.jar
        wget --no-check-certificate 'https://docs.google.com/uc?export=download&id=1MWPTvX_QDR9-7u_8qhwx9vF5RpeJc0za' -O mariadb/jars/wrapper/JavaWrappers.jar
    fi
    if [ ! -f "mariadb/jars/wrapper/JdbcInterface.jar" ]; then
        wget --no-check-certificate 'https://docs.google.com/uc?export=download&id=1uH2yKnvLBCpDpQnhOmm_Us988-AcKkLw' -O mariadb/jars/wrapper/JdbcInterface.jar
    fi
    mkdir -p mariadb/connectData/
    docker-compose up mariadb
elif [ $1 = "mariadb-stop" ]; then
    docker-compose stop mariadb
elif [ $1 = "clickhouse" ]; then
    mkdir -p clickhouse/database/
    docker-compose up clickhouse
elif [ $1 = "clickhouse-stop" ]; then
    docker-compose stop clickhouse
elif [ $1 = "mssql" ]; then
    docker-compose up mssql
elif [ $1 = "mssql-stop" ]; then
    docker-compose stop mssql
elif [ $1 = "mysql" ]; then
    mkdir -p mysql/storage/
    docker-compose up mysql
elif [ $1 = "mysql-stop" ]; then
    docker-compose stop mysql
elif [ $1 = "postgres" ]; then
    mkdir -p postgres/storage/
    docker-compose up postgres
elif [ $1 = "postgres-stop" ]; then
    docker-compose stop postgres
elif [ $1 = "mongo" ]; then
    mkdir -p mongodb/storage/
    docker-compose up mongo
elif [ $1 = "mongo-stop" ]; then
    docker-compose stop mongo

elif [ $1 = "mongo-config" ]; then
    rm -rf mongodb/storage_config/
    mkdir -p mongodb/storage_config/
    docker-compose -f docker-compose-mongo.yml up mongo-config
elif [ $1 = "mongo-config-stop" ]; then
    docker-compose -f docker-compose-mongo.yml stop mongo-config

elif [ $1 = "mongo-instance" ]; then
    rm -rf mongodb/storage_instance/
    mkdir -p mongodb/storage_instance/
    docker-compose -f docker-compose-mongo.yml up mongo-instance
elif [ $1 = "mongo-instance-stop" ]; then
    docker-compose -f docker-compose-mongo.yml stop mongo-instance

elif [ $1 = "mongo-mongos" ]; then
    rm -rf mongodb/storage_mongos/
    mkdir -p mongodb/storage_mongos/
    docker-compose -f docker-compose-mongo.yml up mongo-mongos
elif [ $1 = "mongo-mongos-stop" ]; then
    docker-compose -f docker-compose-mongo.yml stop mongo-mongos

fi
