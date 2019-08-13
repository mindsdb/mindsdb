FROM python:3.7.4

# Do docker requirements
RUN curl https://bootstrap.pypa.io/get-pip.py | python3
RUN pip install --upgrade pip
RUN pip install --no-cache-dir mindsdb

# Use bash cmd entry point
CMD ["bash"]
