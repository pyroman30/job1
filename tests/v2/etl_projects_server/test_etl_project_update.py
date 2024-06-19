import pytest

from fs_db.db_classes_general import EtlProjectVersion


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlUpdateView:
    url = "v2/etl/{etl_id}"

    def test_success(
        self,
        db,
        client,
        etl_project_1_version_2: EtlProjectVersion,
    ):
        new_description = "test new_description"
        response = client.put(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            json={
                "version": etl_project_1_version_2.version,
                "data": {
                    "user_id": 1,
                    "description": new_description,
                },
            },
        )

        assert response.status_code == 200

        db.refresh(etl_project_1_version_2.etl_project)
        assert (
            etl_project_1_version_2.etl_project.description == new_description
        )
