FROM gitpod/workspace-postgres

RUN pyenv install 3.8 \
    && pyenv global 3.8
