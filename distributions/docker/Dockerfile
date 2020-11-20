FROM pytorch/pytorch:latest

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

RUN pip install git+https://github.com/mindsdb/mindsdb.git@staging

CMD ["python", "-m", "mindsdb", "--api=http,mysql"]
