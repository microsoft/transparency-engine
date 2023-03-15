#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
# 


# install python and java required for Spark
# Image details:
# - Ubuntu 20.04.5
# - Spark 3.2.2
# - Python 3.9.15
FROM mcr.microsoft.com/mmlspark/build-minimal:0.10.2

# COPY --from=py3 / /

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

ENV SPARK_EXECUTOR_MEMORY=2g

# Install necessary dependencies to compile
# C++11 support needed for gensim
RUN apt-get update -y &&\
    apt-get install -y curl &&\
    apt-get install -y git &&\
    apt-get install -y gcc &&\
    apt-get install --no-install-recommends --yes build-essential &&\
    apt-get install -y libc-dev &&\
    rm -rf /var/lib/apt/lists/*


# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python - --version 1.3.2
ENV PATH="${PATH}:/root/.local/bin"

WORKDIR /workdir

# Copy backend assets
#   This is done separately so the base layers won't change that often
#   to speed up image pushing
COPY ./poetry.lock .
COPY ./pyproject.toml .
COPY ./scripts ./scripts


# Install python dependencies
RUN ./scripts/install_python_deps.sh

# Copy remaining files
COPY . .

ENTRYPOINT ["bash"] 