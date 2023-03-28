# Dockerfile
FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN pip3 install --upgrade pip && pip3 install psycopg2-binary

WORKDIR /

# Install our reqs
COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt --force-reinstall
RUN pip install git+https://github.com/mindsdb/lightwood.git@staging --upgrade --no-cache-dir
# Install our app
COPY ./mindsdb /mindsdb/mindsdb

ENV PORT 5000
ENV HOST "0.0.0.0"
ENV PYTHONPATH "/mindsdb"
EXPOSE $PORT
ENTRYPOINT ["python3", "/mindsdb/mindsdb/integrations/handlers_wrapper/ml_handler_service.py"]
