import datetime

import pytest
from fs_common_lib.fs_backend_proxy.data_types import FsRoleTypes
from fs_common_lib.fs_backend_proxy.pdt import BackendProxyUser


@pytest.fixture
def backend_proxy_user():
    return BackendProxyUser.parse_obj(
        {
            "id": 1,
            "username": "admin",
            "display_name": "admin",
            "email": "admin@alfabank.ru",
            "session_begin": datetime.datetime.now(),
            "groups": [FsRoleTypes.CHIEF_RELEASE_ENGINEER.value],
        }
    )
