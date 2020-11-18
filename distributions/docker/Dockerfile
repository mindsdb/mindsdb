FROM pytorch/pytorch:latest

RUN pip install mindsdb

# Use cmd entry point to start the server
CMD ["python", "-m", "mindsdb", "--api=http,mysql"]
EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
