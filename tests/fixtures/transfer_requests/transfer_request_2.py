import pytest

from fs_db.db_classes_general import ProjectTransferRequest
from fs_common_lib.fs_general_api.data_types import (
    ProjectTransferRequestResultType,
)


@pytest.fixture
def transfer_request_2(db, etl_project_1_version_2):
    transfer_request = ProjectTransferRequest(
        etl_project_version=etl_project_1_version_2,
        result=ProjectTransferRequestResultType.SUCCESS,
    )
    db.add(transfer_request)
    db.flush()
    return transfer_request
