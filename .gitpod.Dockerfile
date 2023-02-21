FROM gitpod/workspace-postgres

ENV NODE_VERSION=18
ENV PYTHON_VERSION=3.8

RUN pyenv install $PYTHON_VERSION -s \
    && pyenv global $PYTHON_VERSION

RUN bash -c 'source $HOME/.nvm/nvm.sh && nvm install $NODE_VERSION \
    && nvm use $NODE_VERSION && nvm alias default $NODE_VERSION'

RUN echo "nvm use default &>/dev/null" >> ~/.bashrc.d/51-nvm-fix
