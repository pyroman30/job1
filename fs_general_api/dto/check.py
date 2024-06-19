from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt
from fs_common_lib.fs_general_api.data_types import (
    ProjectCheckResult,
    SimpleCheckResult,
    CheckType,
    ProjectTransferRequestResultType,
)
from typing import Optional, List


class PdtSimpleCheck(BasePdt):
    id: int
    description: Optional[str]
    result: Optional[SimpleCheckResult]


class PdtGeneralCheck(BasePdt):
    id: int
    check_type: Optional[CheckType]
    result: Optional[ProjectCheckResult]
    checks: List[PdtSimpleCheck]


class PdtGitStatus(BasePdt):
    result: Optional[ProjectTransferRequestResultType]
    message: Optional[str]


class PdtProjectState(BasePdt):
    general_check: Optional[PdtGeneralCheck]
    git_status: Optional[PdtGitStatus]
