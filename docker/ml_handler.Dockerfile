# Dockerfile
FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN pip3 install --upgrade pip

WORKDIR /

# Install our reqs
COPY mindsdb/integrations/handlers_wrapper/common_requirements.txt /mindsdb/
COPY mindsdb/integrations/handlers_wrapper/huggingface_requirements.txt /mindsdb/
RUN pip3 install -r /mindsdb/common_requirements.txt
RUN pip3 install -r /mindsdb/huggingface_requirements.txt
# Install our app
COPY ./mindsdb /mindsdb/mindsdb

ENV PORT 5000
ENV HOST "0.0.0.0"
ENV PYTHONPATH "/mindsdb"
EXPOSE $PORT
ENTRYPOINT ["python3", "/mindsdb/mindsdb/integrations/handlers_wrapper/ml_handler_service.py"]
