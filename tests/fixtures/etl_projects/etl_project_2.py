import pytest
from fs_common_lib.fs_general_api.data_types import (
    EtlProjectStatus,
    GitFlowType,
    ProjectType,
)
from fs_db.db_classes_general import EtlProject, EtlProjectVersion, Hub


@pytest.fixture
def etl_project_2(db, hub_2: Hub) -> EtlProject:
    etl_project = EtlProject(
        name="etl_project_2",
        description="test_description",
        hub=hub_2,
        project_type=ProjectType.FEATURES.value,
        git_flow_type=GitFlowType.ONE_REPO,
    )

    db.add(etl_project)
    db.flush()
    return etl_project


@pytest.fixture
def etl_project_2_version_1(db, etl_project_2):
    etl_project_version = EtlProjectVersion(
        etl_project=etl_project_2,
        version="1.0",
        jira_task="test_jira_task",
        schedule_interval="1 * * * *",
        author_name="author_name",
        author_email="author_email",
        status=EtlProjectStatus.TESTING,
    )

    db.add(etl_project_version)
    db.flush()
    return etl_project_version
