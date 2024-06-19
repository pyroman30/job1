import re
from datetime import datetime

import pytest

from fs_common_lib.fs_registry_api import join_urls
from fs_common_lib.fs_general_api.data_types import EtlProjectStatus
from fs_db.db_classes_general import EtlProjectVersion

from fs_general_api.config import settings


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlGetView:
    url = "v2/etl/{etl_id}"

    def test_success(
            self,
            db,
            client,
            mock_aioresponse,
            etl_project_1_version_2: EtlProjectVersion,
    ):
        url = join_urls(
            settings.backend_uri_dev,
            "internal",
            "etl",
            str(etl_project_1_version_2.etl_project_id),
            "last_run",
        )
        mock_aioresponse.get(
            re.compile(url + r".*$"),
            status=200,
            payload={
                "result": "SUCCESS",
                "run_ts": datetime.now().isoformat(),
            },
        )

        response = client.get(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            params={"version": etl_project_1_version_2.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "current_version": {
                "author_email": "author_email",
                "author_name": "author_name",
                "created_timestamp": etl_project_1_version_2.created_timestamp.isoformat(),
                "branch_name": "branch_name",
                "git_develop_url": "git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1/?at=refs%2Fheads%2Fbranch_name",
                "git_master_url": "git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1/?at=refs%2Fheads%2Fbranch_name",
                "jira_task": "test_jira_task_2",
                "jira_task_url": "https://jira.moscow.alfaintra.net/browse/test_jira_task_2",
                "moved_to_prod_release_timestamp": None,
                "moved_to_prod_review_timestamp": None,
                "moved_to_production_timestamp": None,
                "moved_to_testing_timestamp": etl_project_1_version_2.moved_to_testing_timestamp.isoformat(),
                "pull_request_url": None,
                "schedule_interval": "1 * * 2 *",
                "schedule_interval_description": "At 1 minutes past the hour, "
                                                 "only in February",
                "status": "TESTING",
                "user_id": 1,
                "version": "2.0",
            },
            "description": "test_description",
            "git_flow_type": "ONE_REPO",
            "hub_id": etl_project_1_version_2.etl_project.hub_id,
            "id": etl_project_1_version_2.etl_project_id,
            "name": "etl_project_1",
            "project_type": "FEATURES",
            "versions": [{"version": "1.0"}, {"version": "2.0"}],
        }

    def test_turn_off_active_version(
            self,
            db,
            client,
            mock_aioresponse,
            etl_project_1_version_1: EtlProjectVersion,
            etl_project_1_version_2: EtlProjectVersion,
    ):
        url = join_urls(
            settings.backend_uri_prod,
            "internal",
            "etl",
            str(etl_project_1_version_2.etl_project_id),
            "last_run",
        )
        mock_aioresponse.get(
            re.compile(url + r".*$"),
            status=200,
            payload={
                "result": "SUCCESS",
                "run_ts": datetime.now().isoformat(),
            },
        )

        etl_project_1_version_2.status = EtlProjectStatus.PROD_REVIEW
        db.add(etl_project_1_version_2)
        db.flush()

        response = client.get(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            params={"version": etl_project_1_version_2.version},
        )

        assert response.status_code == 200

        db.refresh(etl_project_1_version_2)
        assert response.json() == {
            "current_version": {
                "author_email": "author_email",
                "author_name": "author_name",
                "branch_name": "branch_name",
                "created_timestamp": etl_project_1_version_2.created_timestamp.isoformat(),
                "git_develop_url": "https://git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1/?at=refs%2Fheads%2Fbranch_name",
                "git_master_url": "https://git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1",
                "jira_task": "test_jira_task_2",
                "jira_task_url": "https://jira.moscow.alfaintra.net/browse/test_jira_task_2",
                "moved_to_prod_release_timestamp": etl_project_1_version_2.moved_to_prod_release_timestamp.isoformat(),
                "moved_to_prod_review_timestamp": None,
                "moved_to_production_timestamp": etl_project_1_version_2.moved_to_production_timestamp.isoformat(),
                "moved_to_testing_timestamp": None,
                "pull_request_url": None,
                "schedule_interval": "1 * * 2 *",
                "schedule_interval_description": "At 1 minutes past the hour, "
                                                 "only in February",
                "status": "PRODUCTION",
                "user_id": 1,
                "version": "2.0",
            },
            "description": "test_description",
            "git_flow_type": "ONE_REPO",
            "hub_id": etl_project_1_version_2.etl_project.hub_id,
            "id": etl_project_1_version_2.etl_project_id,
            "name": "etl_project_1",
            "project_type": "FEATURES",
            "versions": [{"version": "1.0"}, {"version": "2.0"}],
        }

        db.refresh(etl_project_1_version_1)
        assert etl_project_1_version_1.status == EtlProjectStatus.TURNED_OFF

    def test_production_version(
            self,
            db,
            client,
            mock_aioresponse,
            etl_project_1_version_2: EtlProjectVersion,
    ):
        etl_project_1_version_2.status = EtlProjectStatus.PRODUCTION
        etl_project_1_version_2.master_commit_hash = "test_master_commit_hash"
        db.add(etl_project_1_version_2)
        db.flush()

        response = client.get(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            params={"version": etl_project_1_version_2.version},
        )

        assert response.status_code == 200
        assert response.json() == {
            "current_version": {
                "author_email": "author_email",
                "author_name": "author_name",
                "created_timestamp": etl_project_1_version_2.created_timestamp.isoformat(),
                "branch_name": "branch_name",
                "git_develop_url": "https://git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1/?at=refs%2Fheads%2Fbranch_name",
                "git_master_url": "https://git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/etl_project_1/?at=test_master_commit_hash",
                "jira_task": "test_jira_task_2",
                "jira_task_url": "https://jira.moscow.alfaintra.net/browse/test_jira_task_2",
                "moved_to_prod_release_timestamp": None,
                "moved_to_prod_review_timestamp": None,
                "moved_to_production_timestamp": None,
                "moved_to_testing_timestamp": None,
                "pull_request_url": None,
                "schedule_interval": "1 * * 2 *",
                "schedule_interval_description": "At 1 minutes past the hour, "
                                                 "only in February",
                "status": "PRODUCTION",
                "user_id": 1,
                "version": "2.0",
            },
            "description": "test_description",
            "git_flow_type": "ONE_REPO",
            "hub_id": etl_project_1_version_2.etl_project.hub_id,
            "id": etl_project_1_version_2.etl_project_id,
            "name": "etl_project_1",
            "project_type": "FEATURES",
            "versions": [{"version": "1.0"}, {"version": "2.0"}],
        }
