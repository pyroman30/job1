import datetime
import json

import pytest

from fs_db.db_classes_general import EtlProjectVersion
from fs_general_api.config import settings
from fs_common_lib.fs_backend_proxy.data_types import FsRoleTypes
from fs_common_lib.fs_backend_proxy.client import BackendProxyUser


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlGetTeamView:
    url = "v2/etl/{etl_id}/team"

    @pytest.fixture
    def project_with_users(
        self, db, user_1, etl_project_1_version_2: EtlProjectVersion
    ):
        etl_project_1_version_2.users = [user_1]
        db.add(etl_project_1_version_2)
        db.flush()
        return etl_project_1_version_2

    def test_success(
        self,
        db,
        client,
        project_with_users: EtlProjectVersion,
        user_1,
        mock_aioresponse
    ):

        data = {
            "id": 1,
            "username": "admin",
            "display_name": user_1.display_name,
            "email": user_1.email,
            "session_begin": datetime.datetime.now(),
            "groups": [FsRoleTypes.CHIEF_RELEASE_ENGINEER.value],
        }

        mock_aioresponse.get(f"{settings.backend_proxy_url}/user/{user_1.user_id}", payload=json.loads(BackendProxyUser.parse_obj(data).json()))


        response = client.get(
            self.url.format(etl_id=project_with_users.etl_project_id),
            params={"version": project_with_users.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "display_name": user_1.display_name,
                    "email": user_1.email,
                    "id": user_1.user_id,
                    "groups": [FsRoleTypes.CHIEF_RELEASE_ENGINEER.value]
                }
            ],
            "total": 1,
        }


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlUpdateTeamView:
    url = "v2/etl/{etl_id}/team"

    @pytest.fixture
    def project_with_users(
        self, db, user_1, etl_project_1_version_2: EtlProjectVersion
    ):
        etl_project_1_version_2.users = [user_1]
        db.add(etl_project_1_version_2)
        db.flush()
        return etl_project_1_version_2

    def test_success(
        self,
        db,
        client,
        project_with_users: EtlProjectVersion,
        user_1,
    ):
        response = client.put(
            self.url.format(etl_id=project_with_users.etl_project_id),
            json={
                "version": project_with_users.version,
                "data": [
                    {
                        "user_id": 999,
                        "display_name": "new_name",
                        "email": "new_email",
                    }
                ],
            },
        )

        assert response.status_code == 200

        db.refresh(project_with_users)
        assert project_with_users.users[0].user_id == 999
