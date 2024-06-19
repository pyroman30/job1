import pytest

from fs_common_lib.fs_registry_api import join_urls
from fs_db.db_classes_general import EtlProjectVersion

from fs_general_api.config import settings


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlDeleteView:
    url = "v2/etl/{etl_id}"

    def test_success(
        self,
        db,
        client,
        mock_aioresponse,
        etl_project_1_version_2: EtlProjectVersion,
    ):
        backend_url = join_urls(
            settings.backend_uri_dev,
            "internal",
            "etl",
            str(etl_project_1_version_2.etl_project_id),
            "by_general_id",
        )
        mock_aioresponse.delete(
            backend_url,
            status=200,
            payload={},
        )

        git_url = join_urls(
            settings.git_manager_uri,
            "internal",
            "v2",
            "etl_project",
            "branch",
        )
        mock_aioresponse.delete(
            git_url,
            status=200,
            payload={},
        )

        airflow_url = join_urls(
            settings.git_manager_uri,
            "internal",
            "v2",
            "etl_project",
            "airflow",
        )
        mock_aioresponse.delete(
            airflow_url,
            status=200,
            payload={},
        )

        response = client.delete(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            json={
                "version": etl_project_1_version_2.version,
                "data": {
                    "user_id": 1,
                },
            },
        )

        assert response.status_code == 200

        deleted_version = (
            db.query(EtlProjectVersion)
            .filter(EtlProjectVersion.id == etl_project_1_version_2.id)
            .first()
        )
        assert not deleted_version

    def test_version_wrong_status(
        self,
        db,
        client,
        etl_project_1_version_1: EtlProjectVersion,
    ):
        response = client.delete(
            self.url.format(etl_id=etl_project_1_version_1.etl_project_id),
            json={
                "version": etl_project_1_version_1.version,
                "data": {
                    "user_id": 1,
                },
            },
        )

        assert response.status_code == 403
        assert (
            response.json()
            == "You cannot delete project in status `EtlProjectStatus.PRODUCTION`"
        )
