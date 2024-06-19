import pytest

from fs_db.db_classes_general import HistoryEvent


@pytest.fixture
def history_event_1(db, etl_project_1_version_2):
    history_event = HistoryEvent(
        etl_project_version=etl_project_1_version_2,
        name="name",
        old_value="old_value",
        new_value="new_value",
    )
    db.add(history_event)
    db.flush()
    return history_event


@pytest.fixture
def history_event_2(db, etl_project_1_version_2):
    history_event = HistoryEvent(
        etl_project_version=etl_project_1_version_2,
        name="name",
        old_value="old_value_2",
        new_value="new_value_2",
    )
    db.add(history_event)
    db.flush()
    return history_event
