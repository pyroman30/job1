# pull official base image
ARG my_arg

FROM docker-hub.binary.alfabank.ru/python:3.7.9 as base
WORKDIR /app
COPY setup_docker/fs_db/main_requirements.txt .

ENV PIP_CONFIG_FILE pip.conf
RUN pip install --upgrade pip && \
    pip install -r main_requirements.txt --no-cache-dir

COPY fs_db/ .
RUN pip3 install -e .

FROM base as setup-version-1
COPY setup_docker/fs_db/fs_requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r fs_requirements.txt --no-cache-dir

FROM base as setup-version-2
COPY fs_common_lib ./fs_common_lib
RUN pip install -e ./fs_common_lib

FROM setup-version-${my_arg} as final
CMD docker_db/upgrade_db.sh
