import pytest

from fs_db.db_classes_general import EtlProjectVersion, ProjectTransferRequest
from fs_common_lib.fs_general_api.data_types import (
    ProjectTransferRequestResultType,
)


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlGetListToTransferView:
    url = "internal/etl/get_list_to_transfer"

    @pytest.fixture
    def transfer_request_without_result(
        self, db, transfer_request_1: ProjectTransferRequest
    ):
        transfer_request_1.result = None
        db.add(transfer_request_1)
        db.flush()
        return transfer_request_1

    def test_success(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        transfer_request_without_result: ProjectTransferRequest,
    ):
        response = client.get(
            self.url,
            params={"with_status_update": True},
        )

        assert response.status_code == 200
        assert response.json() == [
            {
                "branch_name": None,
                "error_msg": None,
                "etl_project": {
                    "description": "test_description",
                    "git_flow_type": "ONE_REPO",
                    "id": etl_project_2_version_1.etl_project_id,
                    "name": "etl_project_2",
                },
                "git_branch_url": None,
                "jira_task": "test_jira_task",
                'pr_params': {'comment': None,
                              'reviewer_usernames': None},
                "pull_request_url": None,
                "status": "TESTING",
                "version": "1.0",
            }
        ]

        db.refresh(transfer_request_without_result)
        assert (
            transfer_request_without_result.result
            == ProjectTransferRequestResultType.PROCESSING
        )
