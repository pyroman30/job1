import datetime
from typing import Optional, List

from pydantic import BaseModel, Field

from fs_common_lib.fs_general_api.data_types import GitFlowType, ProjectType
from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt

from fs_general_api.dto.etl_project import (
    RetroCalculationInterval,
    RetroCalculationType,
)


class EtlProjectCreatePdt(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str
    description: Optional[str]
    user_id: Optional[int]
    author_name: str
    author_email: str
    hub_id: int
    jira_task: str
    schedule_interval: str = Field(alias="cron")
    git_flow_type: Optional[GitFlowType] = Field(default=GitFlowType.ONE_REPO)
    project_type: Optional[str] = Field(default=ProjectType.FEATURES.value)


class EtlProjectVersionPreviewPdt(BasePdt):
    version: str
    status: str
    jira_task: Optional[str]
    author_name: Optional[str]
    author_email: Optional[str]


class EtlProjectPreviewPdt(BasePdt):
    id: int
    name: str
    hub_id: int
    description: str

    versions: List[EtlProjectVersionPreviewPdt]


class EtlProjectVersionFullPdt(BasePdt):
    version: Optional[str]
    status: Optional[str]
    jira_task: Optional[str]
    schedule_interval: Optional[str]
    schedule_interval_description: Optional[str]
    jira_task_url: Optional[str]
    author_name: Optional[str]
    author_email: Optional[str]
    user_id: Optional[int]
    pull_request_url: Optional[str]
    branch_name: Optional[str]
    git_master_url: Optional[str]
    git_develop_url: Optional[str]
    created_timestamp: Optional[datetime.datetime]
    moved_to_testing_timestamp: Optional[datetime.datetime]
    moved_to_prod_release_timestamp: Optional[datetime.datetime]
    moved_to_prod_review_timestamp: Optional[datetime.datetime]
    moved_to_production_timestamp: Optional[datetime.datetime]


class AvailableEtlProjectVersionPdt(BasePdt):
    version: str


class EtlProjectFullPdt(BasePdt):
    id: int
    name: str
    description: Optional[str]
    hub_id: int

    git_flow_type: Optional[GitFlowType]
    project_type: Optional[str]

    current_version: EtlProjectVersionFullPdt
    versions: Optional[List[AvailableEtlProjectVersionPdt]]


class UpdateEtlProjectPdt(BasePdt):
    user_id: int
    description: str


class DeleteEtlProjectPdt(BasePdt):
    user_id: int


class SendEtlToProdPdt(BasePdt):
    user_name: Optional[str]
    author_name: Optional[str]
    author_email: Optional[str]


class EtlRetroCalculationPdt(BasePdt):
    class Config:
        allow_population_by_field_name = True

    interval: RetroCalculationInterval
    calc_type: RetroCalculationType = Field(alias="type")


class GitBranchInfoPdt(BasePdt):
    name: Optional[str]
    url: Optional[str]


class EtlStatusInfoPdt(BasePdt):
    git_branch: GitBranchInfoPdt


class EtlProjectUserPermissionsPdt(BasePdt):
    update_datamart: bool
    start_retro_calc: bool
    update_etl_project: bool
    delete_etl_project: bool
    edit_team: bool
    start_auto_check: bool
    start_code_sync_in_mdp: bool
    send_to_review: bool
    create_new_version: bool
