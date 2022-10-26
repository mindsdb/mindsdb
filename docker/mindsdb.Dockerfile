FROM docker.io/pytorch/pytorch:1.9.1-cuda11.1-cudnn8-runtime


ENV PYTHONUNBUFFERED=1

RUN apt update && apt install -y build-essential

# db2 requirement
RUN apt install -y libxml2 || true

RUN python -m pip install --prefer-binary --no-cache-dir --upgrade pip==22.1.2 && \
    pip install --prefer-binary --no-cache-dir wheel==0.37.1 && \
    pip install --prefer-binary --no-cache-dir \
        cassandra-driver \
        mysql-connector-python \
        pymssql>=2.1.4 \
        psycopg[binary]>=1.15.3 \
        pymongo certifi \
        snowflake-connector-python>=2.7.6 \
        snowflake-sqlalchemy>=1.4.3 \
        clickhouse-driver \
        scylla-driver && \
    pip install --prefer-binary --no-cache-dir \
        databricks-sql-connector sqlalchemy-databricks || true && \
    pip install --prefer-binary --no-cache-dir \
        fdb pyodbc || true && \
    pip install --prefer-binary --no-cache-dir \
        sqlalchemy-access || true && \
    pip install --prefer-binary --no-cache-dir \
        ckanapi || true && \
    pip install --prefer-binary --no-cache-dir \
        couchbase==4.0.2 || true && \
    pip install --prefer-binary --no-cache-dir \
        ibm-db-sa ibm-db || true && \
    pip install --prefer-binary --no-cache-dir \
        oracledb==1.0.2 || true && \
    pip install --prefer-binary --no-cache-dir \
        redshift_connector sqlalchemy-redshift || true && \
    pip install --prefer-binary --no-cache-dir \
        trino~=0.313.0 requests-kerberos==0.12.0 || true && \
    pip install --prefer-binary --no-cache-dir \
        google-cloud-bigquery db-dtypes sqlalchemy-bigquery || true \
    pip install --prefer-binary --no-cache-dir \
        boto3 || true \
    pip install --prefer-binary --no-cache-dir \
        vertica-python sqlalchemy-vertica-python || true \
    pip install --prefer-binary --no-cache-dir \
        crate[sqlalchemy] || true \
    pip install --prefer-binary --no-cache-dir \
        pinotdb || true

WORKDIR /
COPY requirements.txt /
RUN pip install -r /requirements.txt

ENV PYTHONPATH "/mindsdb"
ENV FLASK_DEBUG "1"

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
