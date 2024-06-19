from typing import List

from fs_common_lib.fs_backend_api.pydantic_classes import BasePdt
from fs_common_lib.fs_backend_proxy.client import BackendProxyUser
from fs_db.db_classes_general import User
from pydantic import Field


class PdtUser(BasePdt):
    class Config:
        allow_population_by_field_name = True

    user_id: int = Field(alias="id")
    display_name: str
    email: str


class PdtUserWithGroups(PdtUser):
    groups: List[str]

    @classmethod
    def from_backend_user(cls, usr: BackendProxyUser):
        return cls(user_id=usr.id, display_name=usr.display_name, email=usr.email, groups=[gr.value for gr in usr.groups])

    @classmethod
    def from_db_user(cls, usr: User):
        return cls(user_id=usr.user_id, display_name=usr.display_name, email=usr.email, groups=[])
