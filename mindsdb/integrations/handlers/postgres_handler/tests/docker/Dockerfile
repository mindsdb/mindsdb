FROM postgres

ENV POSTGRES_DB test

COPY ./sql-scripts/ /docker-entrypoint-initdb.d/
COPY ./certs/* /var/lib/.postgresql/
# COPY ./postgresql.conf /var/lib/postgresql/data/
RUN chown postgres:postgres /var/lib/.postgresql/server.*
COPY home_rentals.csv /
