import pytest

from fs_db.db_classes_general import GeneralCheck
from fs_common_lib.fs_general_api.data_types import ProjectCheckResult


@pytest.fixture
def general_check_1(db, etl_project_2_version_1):
    general_check = GeneralCheck(
        etl_project_version=etl_project_2_version_1,
        result=ProjectCheckResult.PROCESSING,
    )
    db.add(general_check)
    db.flush()
    return general_check
