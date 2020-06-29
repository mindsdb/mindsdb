FROM mariadb:10.3

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
        mariadb-plugin-connect

RUN apt-get update && apt-get install -y openjdk-8-jdk && export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

COPY ./jars/wrapper/* /usr/lib/mysql/plugin/

COPY ./jars/jdbc/* /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/lib/ext/

RUN chmod 0444 /etc/mysql/mariadb.conf.d/connect.cnf

VOLUME /var/lib/mysql

EXPOSE 3306

CMD ["mysqld"]
