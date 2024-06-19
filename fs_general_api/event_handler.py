import time
import traceback
from multiprocessing import Queue
from queue import Empty
from threading import Thread
from typing import Union, Generator

from sqlalchemy.orm import Session
from fs_common_lib.fs_general_api.data_types import (
    CheckType,
    ProjectCheckResult,
    ProjectType,
)
from fs_common_lib.fs_logger.fs_logger import FsLoggerHandler
from fs_common_lib.fs_registry_api import join_urls, _post
from fs_db.db_classes_general import (
    SimpleCheck,
    ProjectTransferRequest,
    EtlProjectVersion,
)

from fs_general_api.extra_processes.synchronizer.definitions import (
    SynchronizeEventResponse,
)
from fs_general_api.extra_processes.checker.check import CheckEventResponse
from fs_general_api.config import settings
from fs_general_api.db import get_data_storage, db


class EventHandler(Thread):
    """Класс для обработки результатов проверки в отдельном потоке и записи их в БД"""

    def __init__(self, ctrl_queue: Queue):
        Thread.__init__(self)
        self.is_started: bool = True
        self.ctrl_queue: Queue = ctrl_queue
        self.data_storage = get_data_storage()
        self.logger = FsLoggerHandler(
            __name__,
            level=settings.log_level,
            log_format=settings.log_format,
            datefmt=settings.datefmt,
        ).get_logger()

    def run(self) -> None:
        while self.is_started:
            db_session_gen: Generator[Session, None, None] = db.get_session()
            try:
                session = next(db_session_gen, None)
                response: Union[
                    CheckEventResponse, SynchronizeEventResponse
                ] = self.ctrl_queue.get_nowait()

                if isinstance(response, CheckEventResponse):
                    self.handle_check_event_response(
                        session=session, response=response
                    )
                elif isinstance(response, SynchronizeEventResponse):
                    self.handle_synchronize_event_response(
                        session=session, response=response
                    )

            except Empty:
                pass

            except Exception as e:
                self.logger.error(f"{e}\n{traceback.format_exc()}")
            finally:
                next(db_session_gen, None)
            time.sleep(1)

    def handle_synchronize_event_response(
        self, session: Session, response: SynchronizeEventResponse
    ) -> None:
        self.logger.info(f"Received synchronize response: {response}")

        if response.error_message:
            self.logger.error(response.error_message)
            return

        with self.data_storage.check_lock:
            etl_project_version: EtlProjectVersion = self.data_storage.get_etl_project_version(
                db_session=session,
                etl_project_id=response.synchronize_event_request.etl_project_id,
                etl_project_version=response.synchronize_event_request.etl_project_version,
            )
            response_schedule_interval: str = response.schedule_interval

            if (
                etl_project_version.schedule_interval
                == response_schedule_interval
            ):
                self.logger.info(
                    f"Schedule interval for ETL-project with id={etl_project_version.id} didn't change"
                )
                return

            etl_project_version.schedule_interval = response_schedule_interval
            session.add(etl_project_version)
            session.commit()

        url = join_urls(
            settings.backend_uri_prod,
            "internal",
            "etl",
            str(etl_project_version.etl_project_id),
        )
        response = _post(
            url=url,
            json={
                "data": {"cron": response_schedule_interval},
                "general_etl_project_version": etl_project_version.version,
            },
        )

        if not (200 <= response.status_code < 300):
            self.logger.error(
                f"Cant update schedule_interval for ETL-project version with id={etl_project_version.etl_project_id}, "
                f"version={etl_project_version.version} in backend_api schema.\n"
                f"Error on dev backend:\n{response.text}"
            )

    def handle_check_event_response(
        self, session: Session, response: CheckEventResponse
    ) -> None:
        self.logger.info(f"Received check response: {response}")

        with self.data_storage.check_lock:
            etl_project_version: EtlProjectVersion = self.data_storage.get_etl_project_version(
                db_session=session,
                etl_project_id=response.check_event_request.etl_project_id,
                etl_project_version=response.check_event_request.etl_project_version,
            )

            for gen_check in etl_project_version.checks:
                if (
                    gen_check.id
                    == response.check_event_request.general_check_id
                ):
                    general_check = gen_check
                    break

            checks = []
            for check in response.checks:
                checks.append(
                    SimpleCheck(
                        description=check.description, result=check.result
                    )
                )

            general_check.result = response.result
            general_check.checks = checks

            """
            Отправляем запрос на перемещение исходников проекта в git_manger в случае когда проверка 
            производится перед REVIEW
            """
            if (
                response.check_event_request.check_type == CheckType.REVIEW
                and response.result == ProjectCheckResult.SUCCESS
                and response.check_event_request.user_data is not None
            ):

                reviewers = []
                comment = None

                if etl_project_version.etl_project.project_type == ProjectType.TARGETS.value:
                    reviewers = settings.pr_target_project_reviewers
                    users_str = ", ".join([f"@{rev}" for rev in reviewers])
                    comment = f"Перед мерджем данного pull request-a необходимо, чтобы пользователи {users_str} проверили код"

                transfer_request = ProjectTransferRequest(
                    user_name=response.check_event_request.user_data.user_name,
                    author_email=response.check_event_request.user_data.author_email,
                    author_name=response.check_event_request.user_data.author_name,
                    etl_project_version_id=etl_project_version.id,
                    pr_reviewer_usernames=reviewers,
                    pr_comment=comment
                )

                self.logger.info(
                    f"Creating transfer request: {transfer_request}"
                )
                session.add(transfer_request)

            session.add(general_check)
            session.add(etl_project_version)
            session.commit()
