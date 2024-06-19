# pull official base image
FROM docker-hub.binary.alfabank.ru/python:3.7.9

# set work directory
WORKDIR /app

# copy project
COPY . .

# set environment variables
ENV PIP_CONFIG_FILE pip.conf

# install dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir

RUN git config --global http.sslverify false

CMD uvicorn fs_general_api.server:app --host 0.0.0.0 --port 8000