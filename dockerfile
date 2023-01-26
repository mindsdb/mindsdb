FROM python:3.8.4-slim-buster
COPY . usr/src/app
WORKDIR /usr/src/app

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

RUN  python -m pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT python -m mindsdb --reload
