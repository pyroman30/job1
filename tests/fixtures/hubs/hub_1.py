import pytest
from fs_db.db_classes_general import Hub


@pytest.fixture
def hub_1(db):
    hub = Hub(
        name="hub_ul",
        description="hub_ul_description",
    )

    db.add(hub)
    db.flush()
    return hub
