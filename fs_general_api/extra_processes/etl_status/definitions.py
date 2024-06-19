import enum
from dataclasses import dataclass

from pydantic import BaseModel
from fs_common_lib.fs_backend_api.internal_dto import InternalPdtEtlRun


class PeriodType(enum.Enum):
    hour = "hour"
    day = "day"


@dataclass
class PeriodParams:
    value: int
    type: PeriodType


class LastRunsForEtlProject(BaseModel):
    etl_project_id: int
    etl_project_version: str
    etl_run: InternalPdtEtlRun
