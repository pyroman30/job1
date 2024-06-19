import pytest
from fastapi.encoders import jsonable_encoder

from fs_db.db_classes_general import EtlProjectVersion, ProjectTransferRequest
from fs_common_lib.fs_general_api.data_types import (
    ProjectTransferRequestResultType,
    EtlProjectStatus,
)
from fs_common_lib.fs_general_api.internal_dto import EtlVersionInternalPdt
from fs_common_lib.fs_registry_api import join_urls

from fs_general_api.config import settings


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlUpdateStatusesView:
    url = "internal/etl/update_statuses"

    @pytest.fixture
    def transfer_request_processing(
        self, db, transfer_request_2: ProjectTransferRequest
    ):
        transfer_request_2.result = ProjectTransferRequestResultType.PROCESSING
        db.add(transfer_request_2)
        db.flush()
        return transfer_request_2

    def test_success(
        self,
        db,
        client,
        mock_aioresponse,
        etl_project_1_version_2: EtlProjectVersion,
        etl_project_1_version_1: EtlProjectVersion,
        transfer_request_processing: ProjectTransferRequest,
    ):
        dev_backend_url = join_urls(
            settings.backend_uri_dev,
            "internal",
            "etl",
            "get_projects_by_general_list_id",
        )
        mock_aioresponse.get(
            dev_backend_url,
            status=200,
            payload=[1, 2, 3],
        )

        prod_backend_url = join_urls(
            settings.backend_uri_prod,
            "internal",
            "etl",
            "multiple_creation",
        )
        mock_aioresponse.post(
            prod_backend_url,
            status=200,
            payload={},
        )

        response = client.post(
            self.url,
            json=jsonable_encoder(
                {
                    "master_commit_hash": "test_master_commit_hash",
                    "etl_projects_versions": EtlVersionInternalPdt.get_entity([etl_project_1_version_2]),
                }
            )
        )

        assert response.status_code == 200

        db.refresh(etl_project_1_version_2)
        assert etl_project_1_version_2.status == EtlProjectStatus.PROD_REVIEW

        db.refresh(transfer_request_processing)
        assert (
            transfer_request_processing.result
            == ProjectTransferRequestResultType.SUCCESS
        )

        db.refresh(etl_project_1_version_1)
        assert etl_project_1_version_1.master_commit_hash == "test_master_commit_hash"
