from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, List, Union, TYPE_CHECKING

from fs_common_lib.fs_backend_proxy.client import BackendProxyClient
from fs_common_lib.fs_backend_proxy.pdt import BackendProxyUser
from fs_common_lib.fs_logger.fs_logger import FsLoggerHandler
from fs_db.db_classes_general import User
from sqlalchemy.orm import Session

from fs_general_api.config import settings
from fs_general_api.db import get_data_storage
from fs_general_api.dto.user import PdtUser
from multiprocessing import Queue

if TYPE_CHECKING:
    from fs_general_api.extra_processes.checker.check import CheckEventRequest
    from fs_general_api.extra_processes.synchronizer.definitions import (
        SynchronizeEventRequest,
    )

logger = FsLoggerHandler(
    __name__,
    level=settings.log_level,
    log_format=settings.log_format,
    datefmt=settings.datefmt,
).get_logger()


class BaseRouter(ABC):
    __model_class__ = None

    checker_event_queue = None
    synchronizer_event_queue = None
    backfill_request_queue = None

    def __init__(self):
        self.data_storage = get_data_storage()
        self.settings = settings
        self.logger = logger
        self.tpe = ThreadPoolExecutor()
        self.backend_proxy_client = BackendProxyClient(
            base_url=settings.backend_proxy_url
        )

    async def current_user(
        self, session_token: str
    ) -> Optional[BackendProxyUser]:
        return await self.backend_proxy_client.get_current_user(session_token)

    def get_by_id(self, session: Session, id: int):
        if self.__model_class__ is None:
            raise ValueError(
                "__model_class__ is None! Please, set __model_class__ for your class"
            )
        return self.data_storage.find_record_by_id(
            session, class_=self.__model_class__, id=id
        )

    def get_all(self, session: Session, filter_: Optional[Dict] = None) -> Any:
        if self.__model_class__ is None:
            raise ValueError(
                "__model_class__ is None! Please, set __model_class__ for your class"
            )
        return self.data_storage.get_all(session, self.__model_class__, filter_)

    @staticmethod
    def search_in_list(
        obj_list: List[Any], field: str, value: str
    ) -> List[Any]:
        return list(
            filter(lambda el: getattr(el, field).find(value) >= 0, obj_list)
        )

    def _get_users_list(
        self, db_session: Session, users: List[PdtUser]
    ) -> List[User]:
        users_dict = {pdt_user.user_id: pdt_user for pdt_user in users}

        db_users = self.data_storage.get_users_by_list_id(
            session=db_session, ids=users_dict.keys()
        )

        if len(users_dict) == len(db_users):
            return db_users

        for db_user in db_users:
            del users_dict[db_user.user_id]

        for pdt_user in users_dict.values():
            db_users.append(
                User(
                    user_id=pdt_user.user_id,
                    display_name=pdt_user.display_name,
                    email=pdt_user.email,
                )
            )

        return db_users

    @staticmethod
    def put_msg(
        event_queue: Queue,
        data: Union["CheckEventRequest", "SynchronizeEventRequest"],
    ) -> None:
        event_queue.put(data)
