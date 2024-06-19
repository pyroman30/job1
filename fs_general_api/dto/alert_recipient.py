from typing import Optional

from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt
from fs_common_lib.fs_general_api.data_types import AlertType


class PdtAlertRecipient(BasePdt):
    id: Optional[int]
    hub_id: Optional[int]
    alert_type: Optional[AlertType]
    display_name: Optional[str]
    email: Optional[str]
    description: Optional[str]
