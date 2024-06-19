import pytest
from fs_common_lib.fs_general_api.data_types import (
    EtlProjectStatus,
    GitFlowType,
    ProjectType,
)
from fs_db.db_classes_general import EtlProject, EtlProjectVersion, Hub


@pytest.fixture
def etl_project_1(db, hub_1: Hub) -> EtlProject:
    etl_project = EtlProject(
        name="etl_project_1",
        description="test_description",
        hub=hub_1,
        project_type=ProjectType.FEATURES.value,
        git_flow_type=GitFlowType.ONE_REPO,
    )

    db.add(etl_project)
    db.flush()
    return etl_project


@pytest.fixture
def etl_project_1_version_1(db, etl_project_1: EtlProject) -> EtlProjectVersion:
    etl_project_version = EtlProjectVersion(
        etl_project=etl_project_1,
        version="1.0",
        jira_task="test_jira_task",
        schedule_interval="1 * * * *",
        author_name="author_name",
        author_email="author_email",
        status=EtlProjectStatus.PRODUCTION,
    )

    db.add(etl_project_version)
    db.flush()
    return etl_project_version


@pytest.fixture
def etl_project_1_version_2(db, etl_project_1) -> EtlProjectVersion:
    etl_project_version = EtlProjectVersion(
        etl_project=etl_project_1,
        version="2.0",
        jira_task="test_jira_task_2",
        schedule_interval="1 * * 2 *",
        author_name="author_name",
        author_email="author_email",
        status=EtlProjectStatus.DEVELOPING,
        git_dev_branch_uri="git_dev_branch_uri",
        branch_name="branch_name",
        user_id=1,
    )

    db.add(etl_project_version)
    db.flush()
    return etl_project_version
