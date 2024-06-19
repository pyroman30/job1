import enum
import datetime
from typing import Optional, Union, Any, List, Dict

from pydantic import BaseModel, validator
from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt
from fs_common_lib.fs_general_api.data_types import GitFlowType
from fs_common_lib.fs_registry_api.pydantic_classes import DagRunType

from fs_general_api.dto.hub import PdtHubPreview


class PdtEtl(BasePdt):
    id: Optional[int]
    name: Optional[str]
    description: Optional[str]
    status: Optional[str]
    jira_task: Optional[str]
    schedule_interval: Optional[str]
    schedule_interval_description: Optional[str]
    jira_task_url: Optional[str]
    author_name: Optional[str]
    author_email: Optional[str]
    user_id: Optional[int]
    hub: Optional[PdtHubPreview]
    git_flow_type: Optional[GitFlowType]
    pull_request_url: Optional[str]
    project_type: Optional[str]
    branch_name: Optional[str]
    git_master_url: Optional[str]
    git_develop_url: Optional[str]
    created_timestamp: Optional[datetime.datetime]
    moved_to_testing_timestamp: Optional[datetime.datetime]
    moved_to_prod_release_timestamp: Optional[datetime.datetime]
    moved_to_prod_review_timestamp: Optional[datetime.datetime]
    moved_to_production_timestamp: Optional[datetime.datetime]


class PdtHistoryEvent(BasePdt):
    id: Optional[int]
    name: Optional[str]
    old_value: Optional[str]
    new_value: Optional[str]
    author: Optional[str]
    created_timestamp: Optional[datetime.datetime]
    extra_data: Optional[list]


class RetroCalculationType(enum.Enum):
    DATAMART_METRIC = "DATAMART_METRIC"
    ETL = "ETL"

    def to_dag_run_type(self):
        if self == RetroCalculationType.DATAMART_METRIC:
            return DagRunType.BACKFILL_METRICS_ONLY
        elif self == RetroCalculationType.ETL:
            return DagRunType.BACKFILL_FULL
        raise ValueError("Invalid RetroCalculationType")


class RetroCalculationInterval(BaseModel):
    dateFrom: datetime.date
    dateTo: datetime.date

    @validator("dateFrom", "dateTo", pre=True)
    def string_to_date(cls, v: Union[str, Any]) -> datetime.date:
        if isinstance(v, str):
            return datetime.datetime.strptime(v, "%d.%m.%Y").date()
        return v
