from datetime import datetime
from itertools import chain
from typing import Iterable, List, Optional, Tuple

import aiohttp
import croniter
from fastapi import Body, Depends
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fs_common_lib.fs_general_api.data_types import (
    EtlProjectStatus,
    GitFlowType,
    ProjectTransferRequestResultType,
)
from fs_common_lib.fs_general_api.internal_dto import (
    EtlVersionInternalPdt,
    InternalPdtEtlVersionPreviewMonitoring,
    EtlVersionInternalForTransfer,
    PullRequestSetting
)
from fs_common_lib.fs_registry_api import join_urls
from fs_db.db_classes_general import EtlProject, EtlProjectVersion
from more_itertools import first
from sqlalchemy.orm import Session

from fs_general_api.db import db
from fs_general_api.views import BaseRouter
from fs_general_api.views.v2.etl_project import BASE_MODEL_VERSION

internal_etl_project_router = InferringRouter()


@cbv(internal_etl_project_router)
class InternalEtlProjectServer(BaseRouter):
    __model_class__ = EtlProject

    route = "/etl/"

    @internal_etl_project_router.get(path=route + "get_list_to_transfer")
    def get_list_to_transfer(
        self,
        with_status_update: bool = False,
        db_session: Session = Depends(db.get_session),
    ) -> List[EtlVersionInternalForTransfer]:
        """
        Возвращает список ETL-проектов для перемещения в git
        Args:
            with_status_update: Нужно ли обновить статусы `transfer_requests` на `PROCESSING`
            db_session: сессия базы данных
        """
        with db_session.begin_nested():
            etl_projects_versions = self.data_storage.get_projects_to_transfer(
                db_session
            )

            transfers = []

            for project in etl_projects_versions:
                transfer_request = first(project.project_transfer_requests)
                if with_status_update:
                    transfer_request.result = (
                        ProjectTransferRequestResultType.PROCESSING
                    )
                etl_version: EtlVersionInternalForTransfer = EtlVersionInternalForTransfer.get_entity(project)
                pr_params = PullRequestSetting(reviewer_usernames=transfer_request.pr_reviewer_usernames, comment=transfer_request.pr_comment)
                etl_version.pr_params = pr_params
                transfers.append(etl_version)

            db_session.commit()

        return transfers

    @internal_etl_project_router.post(path=route + "update_statuses")
    async def update_statuses(
        self,
        etl_projects_versions: List[EtlVersionInternalPdt],
        master_commit_hash: Optional[str] = Body(default=None),
        db_session: Session = Depends(db.get_session),
    ):
        """
        Обновляет статус у ETL-проектов после клонирования ветки или создания Pull Request в git-manager
        Args:
            etl_projects_versions: список etl-проектов, данные которых необходимо обновить
            master_commit_hash: хеш коммита для формирования ссылок предыдущих версий
            db_session: сессия базы данных
        """
        success_data = {}
        error_data = {}

        for etl_project_version in etl_projects_versions:
            version_identifier = (
                etl_project_version.etl_project.id,
                etl_project_version.version,
            )

            if etl_project_version.error_msg:
                error_data.update(
                    {version_identifier: etl_project_version.error_msg}
                )
            else:
                success_data.update({version_identifier: etl_project_version})

        alchemy_etl_projects_versions: List[
            EtlProjectVersion
        ] = self.data_storage.get_projects_by_list(
            db_session,
            versions_data=chain(error_data.keys(), success_data.keys()),
        )

        for alchemy_etl_project_version in alchemy_etl_projects_versions:
            version_identifier = (
                alchemy_etl_project_version.etl_project_id,
                alchemy_etl_project_version.version,
            )

            if version_identifier not in success_data:
                continue

            if (
                alchemy_etl_project_version.etl_project.git_flow_type
                == GitFlowType.TWO_REPOS
            ):
                alchemy_etl_project_version.git_prod_branch_uri = (
                    success_data.get(version_identifier).git_branch_url
                )
            else:
                alchemy_etl_project_version.pull_request_url = success_data.get(
                    version_identifier
                ).pull_request_url

            alchemy_etl_project_version.status = EtlProjectStatus.PROD_REVIEW
            alchemy_etl_project_version.moved_to_prod_review_timestamp = (
                datetime.now()
            )

        previous_versions_data = self._get_previous_projects_versions_data(
            versions_data=success_data.keys()
        )
        previous_versions_objects = self._update_previous_projects_versions(
            db_session,
            versions_data=previous_versions_data,
            master_commit_hash=master_commit_hash,
        )
        self.logger.info(f"`master` last commit hash updated for versions: {previous_versions_objects}")

        if success_data:
            with db_session.begin_nested():
                db_session.commit()

            await self._send_etl_projects_to_prod(
                versions_data=success_data.keys()
            )

        try:
            with db_session.begin_nested():
                last_transfer_requests = (
                    self.data_storage.get_last_transfer_requests(
                        db_session,
                        versions_ids=(
                            item.id for item in alchemy_etl_projects_versions
                        ),
                    )
                )

                if last_transfer_requests:
                    for transfer_request in last_transfer_requests:
                        version_identifier = (
                            transfer_request.etl_project_version.etl_project_id,
                            transfer_request.etl_project_version.version,
                        )

                        if version_identifier in success_data:
                            transfer_request.result = (
                                ProjectTransferRequestResultType.SUCCESS
                            )
                        else:
                            transfer_request.retry_counter -= 1
                            if transfer_request.retry_counter == 0:
                                transfer_request.result = (
                                    ProjectTransferRequestResultType.FAILED
                                )
                                transfer_request.error_msg = error_data.get(
                                    version_identifier
                                )
                            else:
                                transfer_request.result = (
                                    ProjectTransferRequestResultType.RETRYING
                                )

                    db_session.commit()
        except Exception as e:
            self.logger.error(
                f"Error while transfer_request result update: {e}"
            )

    @staticmethod
    def _get_previous_projects_versions_data(
        versions_data: Iterable[Tuple[int, str]]
    ):
        return [
            (proj_id, str(float(proj_version) - BASE_MODEL_VERSION))
            for proj_id, proj_version in versions_data
        ]

    def _update_previous_projects_versions(
        self,
        db_session: Session,
        versions_data: Iterable[Tuple[int, str]],
        master_commit_hash: Optional[str],
    ):
        previous_projects_versions = self.data_storage.get_projects_by_list(
            db_session,
            versions_data=versions_data,
        )

        for project_version in previous_projects_versions:
            project_version.master_commit_hash = master_commit_hash

        return previous_projects_versions

    async def _send_etl_projects_to_prod(
        self, versions_data: Iterable[Tuple[int, str]]
    ):
        get_url = join_urls(
            self.settings.backend_uri_dev,
            "internal",
            "etl",
            "get_projects_by_general_list_id",
        )
        async with aiohttp.ClientSession() as session:
            response = await session.get(
                get_url,
                json={"general_versions_data": list(versions_data)},
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(f"Error on dev backend:\n{response_text}")
                return

            etl_projects_versions = await response.json()

        if not etl_projects_versions:
            self.logger.warn(
                f"Etl projects received from dev backend are empty!"
            )
            return

        send_url = join_urls(
            self.settings.backend_uri_prod,
            "internal",
            "etl",
            "multiple_creation",
        )
        async with aiohttp.ClientSession() as session:
            response = await session.post(
                send_url,
                json={"pdt_etl_projects_versions": etl_projects_versions},
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(f"Error on prod backend:\n{response_text}")
                return

        return True

    @internal_etl_project_router.get(
        path=route + "get_production_projects_for_interval"
    )
    def get_not_started_projects(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        db_session: Session = Depends(db.get_session),
    ) -> List[InternalPdtEtlVersionPreviewMonitoring]:
        """
        Принимает на вход часы и сдвиг, за которые необходимо получить список проектов в PRODUCTION,
        которые должны были быть запущены за этот промежуток времени
        Args:
            datetime_start: Начальный диапозон, для которого необходимо найти проекты, которые должны были запуститься
            datetime_end: Конечный диапозон, для которого необходимо найти проекты, которые должны были запуститься
            db_session: сессия базы данных
        """
        projects_versions_list = (
            self.data_storage.get_projects_with_schedule_in_production(
                db_session
            )
        )

        if not projects_versions_list:
            return []

        pdt_projects_versions = []
        for version in projects_versions_list:
            cron = croniter.croniter(version.schedule_interval, datetime_start)
            next_run_time = cron.get_next(datetime)

            if datetime_start < next_run_time < datetime_end:
                pdt_project_version = (
                    InternalPdtEtlVersionPreviewMonitoring.get_entity(version)
                )
                pdt_project_version.run_ts = next_run_time
                pdt_projects_versions.append(pdt_project_version)

        return pdt_projects_versions
