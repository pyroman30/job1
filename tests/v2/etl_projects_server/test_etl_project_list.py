import pytest

from fs_common_lib.fs_general_api.data_types import EtlProjectStatus
from fs_db.db_classes_general import EtlProjectVersion, Hub


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlListView:
    url = "v2/etl/list"

    def test_success(
        self,
        db,
        client,
        etl_project_1_version_1: EtlProjectVersion,
        etl_project_1_version_2: EtlProjectVersion,
        etl_project_2_version_1: EtlProjectVersion,
    ):
        response = client.get(self.url)

        assert response.status_code == 200

        assert {
            "description": etl_project_2_version_1.etl_project.description,
            "hub_id": etl_project_2_version_1.etl_project.hub_id,
            "id": etl_project_2_version_1.etl_project.id,
            "name": etl_project_2_version_1.etl_project.name,
            "versions": [
                {
                    "author_email": etl_project_2_version_1.author_email,
                    "author_name": etl_project_2_version_1.author_name,
                    "jira_task": etl_project_2_version_1.jira_task,
                    "status": etl_project_2_version_1.status.value,
                    "version": etl_project_2_version_1.version,
                }
            ],
        } in response.json()["items"]

        assert {
            "description": etl_project_1_version_1.etl_project.description,
            "hub_id": etl_project_1_version_1.etl_project.hub_id,
            "id": etl_project_1_version_1.etl_project.id,
            "name": etl_project_1_version_1.etl_project.name,
            "versions": [
                {
                    "author_email": etl_project_1_version_1.author_email,
                    "author_name": etl_project_1_version_1.author_name,
                    "jira_task": etl_project_1_version_1.jira_task,
                    "status": etl_project_1_version_1.status.value,
                    "version": etl_project_1_version_1.version,
                },
                {
                    "author_email": etl_project_1_version_2.author_email,
                    "author_name": etl_project_1_version_2.author_name,
                    "jira_task": etl_project_1_version_2.jira_task,
                    "status": etl_project_1_version_2.status.value,
                    "version": etl_project_1_version_2.version,
                },
            ],
        } in response.json()["items"]

        assert response.json()["total"] == 2

    def test_hub_filter(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        hub_2: Hub,
    ):
        response = client.get(self.url, params={"hub_id": hub_2.id})

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "description": etl_project_2_version_1.etl_project.description,
                    "hub_id": etl_project_2_version_1.etl_project.hub_id,
                    "id": etl_project_2_version_1.etl_project.id,
                    "name": etl_project_2_version_1.etl_project.name,
                    "versions": [
                        {
                            "author_email": etl_project_2_version_1.author_email,
                            "author_name": etl_project_2_version_1.author_name,
                            "jira_task": etl_project_2_version_1.jira_task,
                            "status": etl_project_2_version_1.status.value,
                            "version": etl_project_2_version_1.version,
                        }
                    ],
                }
            ],
            "total": 1,
        }

    def test_search_filter(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
    ):
        response = client.get(self.url, params={"search": "project_2"})

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "description": etl_project_2_version_1.etl_project.description,
                    "hub_id": etl_project_2_version_1.etl_project.hub_id,
                    "id": etl_project_2_version_1.etl_project.id,
                    "name": etl_project_2_version_1.etl_project.name,
                    "versions": [
                        {
                            "author_email": etl_project_2_version_1.author_email,
                            "author_name": etl_project_2_version_1.author_name,
                            "jira_task": etl_project_2_version_1.jira_task,
                            "status": etl_project_2_version_1.status.value,
                            "version": etl_project_2_version_1.version,
                        }
                    ],
                }
            ],
            "total": 1,
        }

    def test_status_filter(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
    ):
        response = client.get(
            self.url, params={"status": EtlProjectStatus.TESTING.value}
        )

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "description": etl_project_2_version_1.etl_project.description,
                    "hub_id": etl_project_2_version_1.etl_project.hub_id,
                    "id": etl_project_2_version_1.etl_project.id,
                    "name": etl_project_2_version_1.etl_project.name,
                    "versions": [
                        {
                            "author_email": etl_project_2_version_1.author_email,
                            "author_name": etl_project_2_version_1.author_name,
                            "jira_task": etl_project_2_version_1.jira_task,
                            "status": etl_project_2_version_1.status.value,
                            "version": etl_project_2_version_1.version,
                        }
                    ],
                }
            ],
            "total": 1,
        }
