import pytest

from fs_common_lib.fs_registry_api import join_urls
from fs_db.db_classes_general import EtlProjectVersion

from fs_general_api.config import settings


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlDeleteFromAirflowView:
    url = "v2/etl/{etl_id}/airflow"

    def test_success(
        self,
        db,
        client,
        mock_aioresponse,
        etl_project_1_version_2: EtlProjectVersion,
    ):
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
            },
        )

        assert response.status_code == 200
