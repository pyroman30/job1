import pytest

from fs_db.db_classes_general import User


@pytest.fixture
def user_1(db):
    user = User(user_id=1, display_name="name", email="email")
    db.add(user)
    db.flush()
    return user
