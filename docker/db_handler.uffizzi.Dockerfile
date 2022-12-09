# Dockerfile
FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN pip3 install --upgrade pip

WORKDIR /

# Install our reqs
COPY mindsdb/integrations/handlers_wrapper/common_requirements.txt /mindsdb/
# RUN pip3 install -r /mindsdb/handlers_requirements.txt
RUN pip3 install -r /mindsdb/common_requirements.txt
# Install our app

ENV PORT 5000
ENV HOST "localhost"
ENV PYTHONPATH "/mindsdb"
EXPOSE $PORT