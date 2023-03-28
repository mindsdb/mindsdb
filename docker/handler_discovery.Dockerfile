# Dockerfile
FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN pip3 install --upgrade pip

WORKDIR /

# Install our reqs
COPY ./handler_discovery /handler_discovery
RUN pip install -r ./handler_discovery/requirements.txt --no-cache-dir

ENV PORT 5000
ENV HOST "0.0.0.0"
EXPOSE $PORT
ENTRYPOINT ["python3", "/handler_discovery/sd.py"]
