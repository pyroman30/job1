# FS General API

Сервис взаимодействия с надстендовой информацией FeatureStore
=====================================

Сервис можно запускать через python fs_general_api/run.py

  Также сервис читает переменные среды:
- `DB_USER`: имя пользователя базы данных
- `DB_PASS`: пароль для доступа к базе данных
- `DB_HOST`: хост базы данных
- `DB_PORT`: порт базы данных
- `DB_NAME`: имя базы данных
- `SCHEMA_NAME`: имя схемы в базе данных
- `LOG_LEVEL`: уровень логирования сервиса
- `LOG_FORMAT`: формат логов в сервисе
- `DATEFMT`: формат даты-времени в сервисе
- `GIT_MANAGER_URI`: URI GIT MANAGER сервиса, который используется при создании etl-проекта в гит
- `USE_GIT_MANAGER`: Переменная отвечающая за использование или неиспользование GIT MANAGER
- `BACKEND_URI_DEV`: URI backend сервиса в `dev` для обращения к нему по REST
- `BACKEND_URI_PROD`: URI backend сервиса в `prod` для обращения к нему по REST
- `JIRA_URI`: URI jira, используемый для формирования ссылки на задачу в JIRA, в рамках которой разрабатывается проект


API данного сервиса можно просмотреть в Confluence по ссылке:
http://confluence.moscow.alfaintra.net/display/AAA/API+General#/

В случае вноса каких-либо изменений в работу API или при добавлении новых, необходимо описать это в документе, указанном выше

## Запуск тестов
Для запуска тестов установить необходимые зависимости командой:

```
$ pip install -r ./tests/requirements.txt
```

Если локальной базы нет, то можно поднять базовый докер контейнер:

```
$ docker run --name basic-postgres --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=fs_metastore -p 5432:5432 -it postgres:13.1-alpine
```

В корне репозитория выполнить команду для запуска всех тестов:

```
$ export DB_HOST=localhost export DB_NAME=fs_metastore export DB_PASS=postgres \
export DB_PORT=5432 export DB_USER=postgres export SCHEMA_NAME=public \
export MLOPS_TEAMLEAD=someone USE_GIT_MANAGER=true BACKEND_URI_DEV=/dev:8001 \
export BACKEND_URI_PROD=/prod:8001; pytest ./tests
```

Вместо tests можно указать
- поддиректорию `tests/dir`,
- конкретный файл `tests/dir/test_file.py`
- тестовый класс `tests/dir/test_file.py::TestClass`
- тестовый метод `tests/dir/test_file.py::TestClass::test_method`

Для повышения читаемости вывода тестов  -v, -vv, -vvv