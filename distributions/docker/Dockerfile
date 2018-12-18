FROM pytorch/pytorch:0.4.1-cuda9-cudnn7-runtime

# Do docker requirements
COPY distributions/docker/requirements-docker.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt

# Install Lib to site packages
COPY mindsdb /opt/conda/lib/python3.6/site-packages/mindsdb

# Copy mindsdb docs into workspace
COPY docs /workspace/
COPY LICENSE /workspace/
COPY *.md /workspace/

# Use bash cmd entry point
CMD ["bash"]

