import pytest

from fs_db.db_classes_general import (
    EtlProjectVersion,
    GeneralCheck,
    ProjectTransferRequest,
)


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlGetCheckView:
    url = "v2/etl/{etl_id}/check"

    def test_success(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        transfer_request_1: ProjectTransferRequest,
        general_check_1: GeneralCheck,
    ):
        response = client.get(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            params={"version": etl_project_2_version_1.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "general_check": {
                "check_type": None,
                "checks": [],
                "id": general_check_1.id,
                "result": "PROCESSING",
            },
            "git_status": {
                "message": "",
                "result": transfer_request_1.result.value,
            },
        }


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlCheckListView:
    url = "v2/etl/{etl_id}/check_list"

    def test_success(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        transfer_request_1: ProjectTransferRequest,
        general_check_1: GeneralCheck,
    ):
        response = client.get(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            params={"version": etl_project_2_version_1.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "check_type": None,
                    "checks": [],
                    "id": general_check_1.id,
                    "result": "PROCESSING",
                }
            ],
            "total": 1,
        }


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlCheckRunView:
    url = "v2/etl/{etl_id}/check"

    def test_success(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
    ):
        response = client.post(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            json={"version": etl_project_2_version_1.version},
        )

        assert response.status_code == 200

        created_check: GeneralCheck = db.query(GeneralCheck).first()
        assert (
            created_check.etl_project_version_id == etl_project_2_version_1.id
        )
