# pull official base image
ARG my_arg

FROM docker-hub.binary.alfabank.ru/python:3.7.9 as base
WORKDIR /app
COPY setup_docker/fs_general_api/main_requirements.txt .

ENV PIP_CONFIG_FILE pip.conf
RUN pip install --upgrade pip && \
    pip install -r main_requirements.txt --no-cache-dir
RUN git config --global http.sslverify false

FROM base as setup-version-1
COPY setup_docker/fs_general_api/fs_requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r fs_requirements.txt --no-cache-dir

FROM base as setup-version-2
COPY fs_common_lib ./fs_common_lib
RUN pip install -e ./fs_common_lib
COPY fs_db ./fs_db
RUN pip install -e ./fs_db

FROM setup-version-${my_arg} as final
COPY fs_general_api/ .
CMD uvicorn fs_general_api.server:app --host 0.0.0.0 --port 8000

#--from=base
