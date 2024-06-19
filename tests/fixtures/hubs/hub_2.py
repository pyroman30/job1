import pytest
from fs_db.db_classes_general import Hub


@pytest.fixture
def hub_2(db):
    hub = Hub(
        name="hub_ml",
        description="hub_ml_description",
    )

    db.add(hub)
    db.flush()
    return hub
