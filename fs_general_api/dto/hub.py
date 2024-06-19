from typing import Optional
from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt


class PdtHubPreview(BasePdt):
    id: Optional[int]
    name: Optional[str]


class PdtHub(PdtHubPreview):
    description: Optional[str]
