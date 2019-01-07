FROM pytorch/pytorch:0.4.1-cuda9-cudnn7-runtime

# Do docker requirements
RUN curl https://bootstrap.pypa.io/get-pip.py | python3
RUN pip install --upgrade pip
RUN pip install --no-cache-dir mindsdb

# Use bash cmd entry point
CMD ["bash"]

