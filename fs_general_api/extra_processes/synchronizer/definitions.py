from dataclasses import dataclass
from typing import Optional

from fs_common_lib.fs_general_api.data_types import ProjectType


@dataclass
class SynchronizeEventRequest:
    etl_project_id: int
    etl_project_version: str
    etl_project_name: str
    jira_task: str
    branch_name: str
    git_repo: str
    project_type: ProjectType


@dataclass
class SynchronizeEventResponse:
    synchronize_event_request: SynchronizeEventRequest
    schedule_interval: Optional[str] = None
    error_message: Optional[str] = None
