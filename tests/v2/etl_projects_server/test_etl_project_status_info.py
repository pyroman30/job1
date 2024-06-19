import pytest

from fs_common_lib.fs_general_api.data_types import EtlProjectStatus
from fs_db.db_classes_general import EtlProjectVersion


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlStatusInfoView:
    url = "v2/etl/{etl_id}/{status_type}/status_info"

    def test_success(
        self,
        db,
        client,
        etl_project_1_version_2: EtlProjectVersion,
    ):
        response = client.get(
            self.url.format(
                etl_id=etl_project_1_version_2.etl_project_id,
                status_type=EtlProjectStatus.DEVELOPING.value,
            ),
            params={"version": etl_project_1_version_2.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "git_branch": {
                "name": etl_project_1_version_2.branch_name,
                "url": etl_project_1_version_2.git_dev_branch_uri,
            }
        }
