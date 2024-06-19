import json

import pytest as pytest
from aioresponses import aioresponses
from fastapi.testclient import TestClient
from fs_db.db_classes_general import Base

from fs_general_api.db import db as database
from fs_general_api.config import settings

pytest_plugins = [
    "tests.fixtures.etl_projects",
    "tests.fixtures.hubs",
    "tests.fixtures.backend_proxy",
    "tests.fixtures.transfer_requests",
    "tests.fixtures.checks",
    "tests.fixtures.history_events",
    "tests.fixtures.users",
]


@pytest.fixture
def client():
    # локальный импорт требуется для работы
    #  Depends(database.get_session)
    from fs_general_api.server import app

    client = TestClient(app)
    yield client


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="session")
def database_engine(monkeysession):
    session = next(database.get_session())

    def _dummy_get_session():
        yield session

    monkeysession.setattr(database, "get_session", _dummy_get_session)
    return database


@pytest.fixture(scope="session", autouse=True)
def database_setup(database_engine):
    Base.metadata.create_all(bind=database_engine.engine)
    yield
    Base.metadata.drop_all(bind=database_engine.engine, checkfirst=True)


@pytest.fixture
def db(database_engine, database_setup, monkeypatch):
    session = next(database_engine.get_session())
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope="session", autouse=True)
def shutdown_event():
    from fs_general_api.server import shutdown_processes

    yield
    shutdown_processes()


@pytest.fixture(autouse=True)
def mock_aioresponse(backend_proxy_user):
    with aioresponses() as responses_context:
        responses_context.get(
            f"{settings.backend_proxy_url}/user-info",
            payload=json.loads(backend_proxy_user.json()),
            repeat=True,
        )
        yield responses_context
