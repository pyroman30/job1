import asyncio
from typing import Any, List, Optional, Tuple

import aiohttp
import asyncssh
from fs_common_lib.utils.pagination import LimitOffsetPage
from more_itertools import first
from fastapi import Body, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.params import Cookie
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fs_common_lib.fs_backend_api.internal_dto import InternalPdtEtlRun
from fs_common_lib.fs_general_api.data_types import (
    CheckType,
    EtlProjectStatus,
    GitFlowType,
    ProjectCheckResult,
    ProjectType,
)
from fs_common_lib.fs_general_api.internal_dto import (
    EtlVersionInternalPdt,
)
from fs_common_lib.fs_git_manager.internal_dto import (
    PdtProjectCreateParams,
    ProjectGenerationResponse,
)
from fs_common_lib.fs_registry_api import join_urls
from fs_common_lib.utils.cron_utils import get_description_by_schedule_interval
from fs_common_lib.utils.pagination import (
    LimitOffsetPage,
    LimitOffsetParams,
    Page,
    paginate,
)
from fs_db.alchemy_objects_creator import PydanticToAlchemy
from fs_db.db_classes_general import (
    EtlProject,
    EtlProjectVersion,
    GeneralCheck,
    Hub,
    ProjectTransferRequest,
    User,
)
from more_itertools import first
from sqlalchemy.orm import Session, selectinload

from fs_general_api.db import db
from fs_general_api.dto.check import (
    PdtGeneralCheck,
    PdtGitStatus,
    PdtProjectState,
)
from fs_general_api.dto.etl_project import (
    PdtHistoryEvent,
    RetroCalculationInterval,
    RetroCalculationType,
)
from fs_general_api.dto.user import PdtUser, PdtUserWithGroups
from fs_general_api.exceptions import (
    DataNotFoundException,
    NotEnoughDataException,
    ProjectModificationError,
    RecordAlreadyExistsException,
    StatusUpdateError,
    ThirdPartyServiceError,
)
from fs_general_api.extra_processes.checker.check import (
    CheckEventRequest,
    UserRequestData,
)
from fs_general_api.extra_processes.ssh_executor.worker import (
    SshBackfillRequest,
)
from fs_general_api.extra_processes.synchronizer.definitions import (
    SynchronizeEventRequest,
)
from fs_general_api.permissions import get_permissions_for_user_with_project
from fs_general_api.views import BaseRouter
from fs_general_api.views.v2.dto.etl_project import (
    DeleteEtlProjectPdt,
    EtlProjectCreatePdt,
    EtlProjectFullPdt,
    EtlProjectPreviewPdt,
    EtlProjectUserPermissionsPdt,
    EtlStatusInfoPdt,
    SendEtlToProdPdt,
    UpdateEtlProjectPdt,
)
from fs_general_api.views.v2.event_history_data.comment import (
    generate_comment_event_data,
)
from fs_general_api.views.v2.event_history_data.retro_calculation import (
    get_extra_data_for_retro_calculation_event,
)

etl_project_router = InferringRouter()

BASE_MODEL_VERSION = 1.0


@cbv(etl_project_router)
class EtlProjectServer(BaseRouter):
    __model_class__ = EtlProject

    route = "/etl/"

    @etl_project_router.get(path=route + "list")
    def get_list(
        self,
        hub_id: Optional[int] = None,
        user_id: Optional[int] = None,
        search: Optional[str] = None,
        status: Optional[EtlProjectStatus] = None,
        params: LimitOffsetParams = Depends(LimitOffsetParams),
        db_session: Session = Depends(db.get_session),
    ) -> LimitOffsetPage[EtlProjectPreviewPdt]:
        """
        Возвращает JSONResponse, содержащий список etl проектов
        Args:
            hub_id: - в каких хабах искать проекты
            user_id: - проекты какого пользователя вернуть
            search: - запрос на поиск
            params: - параметры пагинации
            status: - статус по с которым необходимо вернуть проекты
            db_session: - сессия базы данных
        """
        query = (
            db_session.query(EtlProject)
            .join(EtlProject.versions)
            .options(selectinload(EtlProject.versions))
            .distinct()
        )

        if hub_id is not None:
            query = query.filter(EtlProject.hub_id == hub_id)

        if user_id is not None:
            query = query.filter(EtlProjectVersion.user_id == user_id)

        if status is not None:
            query = query.filter(EtlProjectVersion.status == status)

        if search is not None:
            query = query.filter(
                EtlProject.name.like(f"%{search}%")
                | EtlProject.description.like(f"%{search}%")
            )

        return paginate(
            query, params=params, response_schema=EtlProjectPreviewPdt
        )

    def _data_enrichment(
        self, etl_project_version: EtlProjectVersion
    ) -> EtlProjectVersion:
        etl_project_version.schedule_interval_description = (
            get_description_by_schedule_interval(
                etl_project_version.schedule_interval
            )
        )

        if self.settings.use_git_manager:
            (
                etl_project_version.git_master_url,
                etl_project_version.git_develop_url,
            ) = self._form_git_url(
                etl_name=etl_project_version.etl_project.name,
                branch_name=etl_project_version.branch_name,
                status=etl_project_version.status.value,
                flow_type=etl_project_version.etl_project.git_flow_type,
                master_commit_hash=etl_project_version.master_commit_hash,
            )

        if etl_project_version.jira_task:
            etl_project_version.jira_task_url = (
                self.settings.jira_uri + etl_project_version.jira_task
            )

        return etl_project_version

    def _form_git_url(
        self,
        etl_name: str,
        status: str,
        branch_name: Optional[str],
        master_commit_hash: Optional[str],
        flow_type: GitFlowType,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Метод для формирования ссылки на git-репозиторий ETL-проекта
        """
        prod = None
        dev = None

        git_uri_prod = (
            self.settings.git_uri_prod
            if not self.settings.git_uri_prod.endswith("/")
            else self.settings.git_uri_prod[:-1]
        )
        git_uri_dev = (
            self.settings.git_uri_dev
            if not self.settings.git_uri_dev.endswith("/")
            else self.settings.git_uri_dev[:-1]
        )

        if branch_name is None:
            return prod, dev

        if flow_type == GitFlowType.ONE_REPO:
            dev = f'{git_uri_prod}/{etl_name}/?at=refs%2Fheads%2F{branch_name.replace("/", "%2F")}'

            if status in (
                EtlProjectStatus.PRODUCTION.value,
                EtlProjectStatus.PROD_RELEASE.value,
                EtlProjectStatus.TURNED_OFF.value,
            ):
                prod = f"{git_uri_prod}/{etl_name}"

                if master_commit_hash:
                    prod = f"{prod}/?at={master_commit_hash}"
            else:
                prod = dev

        elif flow_type == GitFlowType.TWO_REPOS:
            # В случае вывода ETL-проекта с двумя репозиториями в прод, его репозиторий меняется
            git_repo = (
                git_uri_prod
                if status == EtlProjectStatus.PRODUCTION.value
                else git_uri_dev
            )

            dev = f'{git_repo}/{etl_name}/?at=refs%2Fheads%2F{branch_name.replace("/", "%2F")}'

            if status == EtlProjectStatus.PRODUCTION.value:
                prod = f"{git_repo}/{etl_name}"
            else:
                prod = dev

        return prod, dev

    def _get_detailed_etl_project(
        self, db_session: Session, etl_project_version: EtlProjectVersion
    ) -> EtlProject:
        etl_project: EtlProject = etl_project_version.etl_project

        available_versions = (
            db_session.query(EtlProjectVersion)
            .filter(EtlProjectVersion.etl_project_id == etl_project.id)
            .order_by(EtlProjectVersion.created_timestamp)
            .all()
        )

        etl_project.current_version = self._data_enrichment(
            etl_project_version=etl_project_version
        )
        etl_project.versions = available_versions

        return etl_project

    async def _get_version_last_run(
        self, etl_project_version: EtlProjectVersion
    ):
        if etl_project_version.status in (
            EtlProjectStatus.PROD_REVIEW,
            EtlProjectStatus.PROD_RELEASE,
        ):
            base_url = self.settings.backend_uri_prod
        else:
            base_url = self.settings.backend_uri_dev

        url = join_urls(
            base_url,
            "internal",
            "etl",
            str(etl_project_version.etl_project_id),
            "last_run",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.get(
                url,
                params={
                    "general_etl_project_version": etl_project_version.version
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                raise ThirdPartyServiceError(
                    f"Error on the backend api side:\n{response_text}"
                )

            return await response.json()

    async def _disable_project_version_monitoring(self, etl_project_version: EtlProjectVersion):
        url = join_urls(
            self.settings.metric_manager_uri,
            "internal", "datamart", "monitoring", "disable",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.post(
                url,
                json={
                    "general_etl_project_id": etl_project_version.etl_project_id,
                    "general_etl_project_version": etl_project_version.version,
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(
                   f"An error occurred while disabling monitoring for version "
                   f"(etl_id: {etl_project_version.etl_project_id}, version: {etl_project_version.version}): "
                   f"\n{response_text}"
                )

            return await response.json()

    async def _turn_off_active_project_version(
        self,
        etl_project_version: EtlProjectVersion,
        db_session: Session,
    ):
        turned_off_version: Optional[EtlProjectVersion] = self.data_storage.turn_off_active_project_version(
            db_session,
            new_etl_version_id=etl_project_version.id,
            etl_project_id=etl_project_version.etl_project_id,
            author_name=etl_project_version.author_name,
        )
        if not turned_off_version:
            return None

        if self.settings.use_metric_manager:
            # Monitoring disabling is optional process, so we don`t need to wait for the task to complete
            asyncio.create_task(self._disable_project_version_monitoring(turned_off_version))

    @etl_project_router.get(path=route + "{etl_id}")
    async def get_etl_project(
        self,
        etl_id: int,
        version: str,
        db_session: Session = Depends(db.get_session),
    ) -> EtlProjectFullPdt:
        """
        Возвращает версию etl project, полученный по etl_id и version
        Args:
            etl_id: id etl_project, который необходимо получить
            version: строковое представление версии etl_project
            session_token: токен сессии
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException(
                f"Etl project version by etl_id=`{etl_id}`, version=`{version}` didn't find!"
            )

        if etl_project_version.status != EtlProjectStatus.PRODUCTION:
            last_etl_run = await self._get_version_last_run(etl_project_version)

            if last_etl_run:
                last_etl_run = InternalPdtEtlRun.parse_obj(last_etl_run)

                if (
                    last_etl_run.result == "SUCCESS"
                    and etl_project_version.status
                    == EtlProjectStatus.DEVELOPING
                ):
                    await self.data_storage.update_etl_project_status(
                        etl_project_version=etl_project_version,
                        status=EtlProjectStatus.TESTING,
                        etl_run=last_etl_run,
                        db_session=db_session,
                        author_name=etl_project_version.author_name,
                    )
                elif (
                    last_etl_run.result == "FAIL"
                    and etl_project_version.status
                    == EtlProjectStatus.PROD_REVIEW
                ):
                    await self.data_storage.update_etl_project_status(
                        db_session=db_session,
                        etl_project_version=etl_project_version,
                        status=EtlProjectStatus.PROD_RELEASE,
                        etl_run=last_etl_run,
                        author_name=etl_project_version.author_name,
                    )
                elif (
                    last_etl_run.result == "SUCCESS"
                    and etl_project_version.status
                    in [
                        EtlProjectStatus.PROD_REVIEW,
                        EtlProjectStatus.PROD_RELEASE,
                    ]
                ):
                    await self.data_storage.update_etl_project_status(
                        db_session=db_session,
                        etl_project_version=etl_project_version,
                        status=EtlProjectStatus.PRODUCTION,
                        etl_run=last_etl_run,
                        author_name=etl_project_version.author_name,
                    )
                    await self._turn_off_active_project_version(
                        db_session=db_session, etl_project_version=etl_project_version,
                    )

        return EtlProjectFullPdt.get_entity(
            self._get_detailed_etl_project(
                db_session, etl_project_version=etl_project_version
            )
        )

    @etl_project_router.put(path=route + "{etl_id}")
    async def update_project(
        self,
        etl_id: int,
        data: UpdateEtlProjectPdt,
        version: str = Body(),
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> EtlProjectFullPdt:
        """
        Обновляет etl project по etl_id
        Args:
            etl_id: id etl_project, который необходимо обновить
            version: версия etl_project, который необходимо обновить
            data: данные для обновления проекта
            session_token: токен сессии
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )
        etl_project: EtlProject = etl_project_version.etl_project

        if not etl_project_version:
            raise DataNotFoundException(
                f"Etl project version by etl_id=`{etl_id}`, version=`{version}` didn't find!"
            )

        if etl_project_version.user_id != data.user_id:
            self.logger.error(
                f"This is project of `{etl_project_version.user_id}`, "
                f"but user=`{data.user_id}` is trying to update this!"
            )
            raise ProjectModificationError("You cannot update this project!")

        await self._apply_history_event(
            db_session=db_session,
            event_name="Редактирование описания",
            etl_project_version_id=etl_project_version.id,
            old_value=etl_project.description,
            new_value=data.description,
            session_token=session_token,
        )

        with db_session.begin_nested():
            etl_project.description = data.description
            db_session.commit()

        return EtlProjectFullPdt.get_entity(
            self._get_detailed_etl_project(
                db_session, etl_project_version=etl_project_version
            )
        )

    async def _delete_project_from_backend(
        self, etl_project_version: EtlProjectVersion
    ):
        delete_url = join_urls(
            self.settings.backend_uri_dev,
            "internal",
            "etl",
            str(etl_project_version.etl_project_id),
            "by_general_id",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.delete(
                delete_url,
                json={
                    "general_etl_project_version": etl_project_version.version
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                raise ThirdPartyServiceError(
                    f"Error on the backend api side:\n{response_text}"
                )

            return await response.json()

    async def _delete_project_from_git(
        self, etl_project_version: EtlProjectVersion
    ):
        if not etl_project_version.jira_task:
            return None

        if (
            etl_project_version.etl_project.git_flow_type
            == GitFlowType.ONE_REPO
        ):
            url = join_urls(
                self.settings.git_manager_uri,
                "internal",
                "v2",
                "etl_project",
                "branch",
            )
        else:
            url = join_urls(
                self.settings.git_manager_uri,
                "internal",
                "v1",
                "etl_project",
                "branch",
            )

        async with aiohttp.ClientSession() as session:
            response = await session.delete(
                url,
                json={
                    "jira_task": etl_project_version.jira_task,
                    "project_name": etl_project_version.etl_project.name,
                }
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)

    @etl_project_router.delete(path=route + "{etl_id}")
    async def delete_project(
        self,
        etl_id: int,
        data: DeleteEtlProjectPdt,
        version: str = Body(),
        db_session: Session = Depends(db.get_session),
    ) -> None:
        """
        Удаляет etl project по etl_id
        Args:
            etl_id: id etl_project, который необходимо удалить
            version: версия etl_project, который необходимо удалить
            data: id пользователя для проверки, может ли он удалить проект
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException(
                f"Etl project version by etl_id=`{etl_id}`, version=`{version}` didn't find!"
            )

        if etl_project_version.status != EtlProjectStatus.DEVELOPING:
            raise ProjectModificationError(
                f"You cannot delete project in status `{etl_project_version.status}`"
            )
        if etl_project_version.user_id != data.user_id:
            self.logger.error(
                f"This is project of `{etl_project_version.user_id}`, "
                f"but user=`{data.user_id}` is trying to delete this!"
            )
            raise ProjectModificationError("You cannot delete this project")

        await self._delete_project_from_backend(etl_project_version)

        if self.settings.use_git_manager_for_deletion:
            await self._delete_project_from_git(etl_project_version)
            self.logger.info("Start to delete project from airflow branch")
            await self._delete_from_airflow_branch(etl_project_version)

        with db_session.begin_nested():
            db_session.delete(etl_project_version)
            db_session.commit()

    @etl_project_router.post(path=route + "{etl_id}/send_to_prod")
    async def send_to_prod(
        self,
        etl_id: int,
        data: SendEtlToProdPdt,
        version: str = Body(),
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> EtlProjectFullPdt:
        """
        Запускает проверки с типом `REVIEW` для ETL-проекта.
        При успешном выполнении всех проверок создается соответствующая запись в таблице `project_transfer_requests`.

        Args:
            etl_id: id etl_project, который необходимо перевести в статус "Заявка на переход в прод"
            version: версия etl_project, которыю необходимо перевести в статус "Заявка на переход в прод"
            data: данные для отправки в прод
            session_token: токен сессии для получения информации о пользователе
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException(
                f"Etl project version by etl_id=`{etl_id}`, version=`{version}` didn't find!"
            )

        if etl_project_version.status == EtlProjectStatus.DEVELOPING:
            raise StatusUpdateError(
                "You cannot send etl project to production without successful runs!"
            )
        elif etl_project_version.status == EtlProjectStatus.PROD_REVIEW:
            raise StatusUpdateError(
                f"Etl project with id=`{etl_id}` already in review"
            )
        elif etl_project_version.status in {
            EtlProjectStatus.PROD_RELEASE,
            EtlProjectStatus.PRODUCTION,
        }:
            raise StatusUpdateError(
                f"Etl project with id=`{etl_id}` already in production with status `{etl_project_version.status.value}`"
            )

        assert etl_project_version.status == EtlProjectStatus.TESTING

        # Сначала обязательно проверяем на наличие TransferRequest, если они есть, то и смысла в запуске логики для
        # проверок нет смысла
        existing_transfer_request = (
            db_session.query(ProjectTransferRequest)
            .filter(
                ProjectTransferRequest.etl_project_version_id
                == etl_project_version.id
            )
            .first()
        )

        if existing_transfer_request is not None:
            raise StatusUpdateError(
                "Transfer to production already requested for this project!"
            )

        existing_gen_check = (
            db_session.query(GeneralCheck)
            .filter(
                GeneralCheck.etl_project_version_id == etl_project_version.id
            )
            .first()
        )

        if (
            existing_gen_check is not None
            and existing_gen_check.result == ProjectCheckResult.PROCESSING
        ):
            raise StatusUpdateError(
                "Pre-checks already requested for this project!"
            )

        user_data = UserRequestData(
            user_name=data.user_name,
            author_email=data.author_email,
            author_name=data.author_name,
        )

        self._run_check_etl_project(
            db_session=db_session,
            etl_project_version=etl_project_version,
            user_data=user_data,
        )

        await self._apply_history_event(
            db_session=db_session,
            event_name="Отправка ETL-проекта на проверку",
            etl_project_version_id=etl_project_version.id,
            old_value=etl_project_version.status.value,
            session_token=session_token,
        )

        return EtlProjectFullPdt.get_entity(
            self._get_detailed_etl_project(
                db_session, etl_project_version=etl_project_version
            )
        )

    @etl_project_router.post(path=route + "{etl_id}/start_retro_calculation")
    async def start_retro_calculation(
        self,
        etl_id: int,
        interval: RetroCalculationInterval,
        version: str = Body(),
        calc_type: RetroCalculationType = Body(alias="type"),
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session)
    ) -> None:
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if etl_project_version is None:
            raise DataNotFoundException("ETL проект не найден")

        if etl_project_version.status not in {EtlProjectStatus.PRODUCTION, EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.PROD_RELEASE}:
            raise NotEnoughDataException("Для запуска ретро расчетов проект должен находиться в продакшене")

        last_success_dag = await self.get_last_successful_dag_run_for_etl_project(etl_project_version)

        if last_success_dag is None or last_success_dag.dag_id is None:
            raise NotEnoughDataException("Не найдено успешных дагов для etl проекта")

        if not self.is_valid_fs_etl_version_for_retro_calc(last_success_dag.fs_etl_version):
            raise NotEnoughDataException(f"Для запуска ретро расчетов для DAG-а необходима версия fs_etl>={self.settings.retro_metric_start_version_require}")

        etl_dag_run_type = calc_type.to_dag_run_type()

        await self.backfill_request_queue.put(SshBackfillRequest(start_date=interval.dateFrom, end_date=interval.dateTo, dag_id=last_success_dag.dag_id, dag_run_type=etl_dag_run_type))

        extra_data = get_extra_data_for_retro_calculation_event(interval.dateFrom, interval.dateTo)

        await self._apply_history_event(
            db_session=db_session,
            event_name="Запуск ретрорасчета метрик",
            etl_project_version_id=etl_project_version.id,
            session_token=session_token,
            extra_data=extra_data,
        )

    def is_valid_fs_etl_version_for_retro_calc(self, version: str):
        def normalize_version(ver: str):
            ps = ver.split(".")
            if len(ps) == 3:
                return ps
            for _i in range(3 - len(ps)):
                ps.append("0")

            return ps

        points = normalize_version(version)
        require = normalize_version(
            self.settings.retro_metric_start_version_require
        )
        if len(require) != 3:
            raise ValueError(
                "invalid retro fs_etl version require for retro calculation"
            )

        for p, rp in zip(points, require):
            if p > rp:
                return True
            elif p < rp:
                return False
        if points[-1] == require[-1]:
            return True

        return False

    async def get_last_successful_dag_run_for_etl_project(self, etl_project_version: EtlProjectVersion):
        backend_uri = self.settings.backend_uri_prod

        url = join_urls(
            backend_uri,
            "internal",
            "etl",
            str(etl_project_version.etl_project_id),
            "last_successful_run",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.get(
                url,
                params={
                    "general_etl_project_version": etl_project_version.version
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                raise ThirdPartyServiceError(
                    f"Error on the backend api side:\n{response_text}"
                )

            resp = await response.json()
            if resp is None:
                return None
            etl_run = InternalPdtEtlRun.parse_obj(resp)
            return etl_run

    @staticmethod
    def _check_version_creation_availability(
        db_session: Session,
        etl_project: EtlProject,
    ) -> bool:
        in_progress_versions = (
            db_session.query(EtlProjectVersion)
            .filter(
                EtlProjectVersion.etl_project_id == etl_project.id,
                EtlProjectVersion.status.notin_(
                    (EtlProjectStatus.PRODUCTION, EtlProjectStatus.TURNED_OFF)
                ),
            )
            .first()
        )

        if in_progress_versions:
            return False

        return True

    def _get_or_create_etl_project_from_pdt(
        self,
        db_session: Session,
        pdt_etl_project: EtlProjectCreatePdt,
    ) -> EtlProject:
        etl_project: Optional[EtlProject] = self.data_storage.find_record_by_name(
            db_session, class_=EtlProject, name=pdt_etl_project.name
        )
        if etl_project:
            if not self._check_version_creation_availability(
                db_session, etl_project=etl_project
            ):
                raise RecordAlreadyExistsException(
                    "version creation not available"
                )

            # даже если первая версия была `GitFlowType.TWO_REPOS`, меняем на актуальный гит флоу
            etl_project.git_flow_type = GitFlowType.ONE_REPO
            return etl_project

        etl_project = PydanticToAlchemy.create_alchemy_from_pydantic(
            pydantic_object=pdt_etl_project,
            alchemy_class=EtlProject,
            include_fields={
                "hub_id",
                "name",
                "description",
                "project_type",
                "git_flow_type",
            },
        )

        return etl_project

    @staticmethod
    def _get_new_etl_project_version(
        db_session: Session,
        etl_project: EtlProject,
    ) -> Tuple[Any, str]:
        if not etl_project.id:
            return None, str(BASE_MODEL_VERSION)

        etl_project_version = (
            db_session.query(EtlProjectVersion)
            .filter(EtlProjectVersion.etl_project_id == etl_project.id)
            .order_by(EtlProjectVersion.id.desc())
            .first()
        )

        if not etl_project_version:
            return None, str(BASE_MODEL_VERSION)

        return etl_project_version.version, str(
            float(etl_project_version.version) + BASE_MODEL_VERSION
        )

    @staticmethod
    def _create_etl_project_version_from_pdt(
        etl_project: EtlProject,
        version: str,
        user: User,
        pdt_etl_project: EtlProjectCreatePdt,
    ) -> EtlProjectVersion:
        etl_project_version: EtlProjectVersion = (
            PydanticToAlchemy.create_alchemy_from_pydantic(
                pydantic_object=pdt_etl_project,
                alchemy_class=EtlProjectVersion,
                include_fields={
                    "author_name",
                    "author_email",
                    "user_id",
                    "schedule_interval",
                    "jira_task",
                },
            )
        )

        etl_project_version.version = version
        etl_project_version.etl_project = etl_project
        etl_project_version.status = EtlProjectStatus.DEVELOPING
        etl_project_version.users = [user]

        return etl_project_version

    @etl_project_router.post(path=route + "create")
    async def create(
        self,
        data: EtlProjectCreatePdt,
        comment: Optional[str] = Body(default=None),
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> EtlProjectFullPdt:
        """
        Возвращает etl project, полученный после создания
        Args:
            data: данные для создания ETL-проекта
            comment: комментарий при создании новой версии
            session_token: токен сессии
            db_session: сессия базы данных
        """

        hub = self.data_storage.find_record_by_id(
            session=db_session, class_=Hub, id=data.hub_id
        )
        if hub is None:
            raise DataNotFoundException(
                f"Hub with id=`{data.hub_id} was not found!`"
            )

        if (
            data.git_flow_type == GitFlowType.TWO_REPOS
            and data.project_type == ProjectType.AGGREGATES
        ):
            raise ThirdPartyServiceError(
                "Aggregates does not support git flow type with two repos"
            )

        if (
            data.git_flow_type == GitFlowType.TWO_REPOS
            and data.project_type == ProjectType.TARGETS
        ):
            raise ThirdPartyServiceError(
                "Targets does not support git flow type with two repos"
            )

        etl_project: EtlProject = self._get_or_create_etl_project_from_pdt(
            db_session,
            pdt_etl_project=data,
        )
        previous_version, new_version = self._get_new_etl_project_version(
            db_session,
            etl_project=etl_project,
        )

        user = (
            db_session.query(User).filter(User.user_id == data.user_id).first()
        )

        if user is None:
            backend_proxy_user = await self.current_user(session_token)
            user = User(
                user_id=backend_proxy_user.id,
                display_name=backend_proxy_user.display_name,
                email=backend_proxy_user.email,
            )

        etl_project_version: EtlProjectVersion = (
            self._create_etl_project_version_from_pdt(
                etl_project=etl_project,
                version=new_version,
                user=user,
                pdt_etl_project=data,
            )
        )

        db_session.add(etl_project_version)
        db_session.flush()

        if self.settings.use_git_manager:
            await self.process_git_manager_create_project(
                etl_project_version=etl_project_version,
                previous_project_version=previous_version,
            )

        await self.process_backend_api_create_project(
            etl_project_version=etl_project_version
        )

        with db_session.begin_nested():
            db_session.commit()

        if comment:
            event_history_extra_data = generate_comment_event_data(
                comment=comment
            )
        else:
            event_history_extra_data = None

        await self._apply_history_event(
            db_session=db_session,
            event_name="Создание ETL-проекта",
            etl_project_version_id=etl_project_version.id,
            old_value=None,
            new_value=etl_project_version.status.value,
            session_token=session_token,
            extra_data=event_history_extra_data,
        )

        return EtlProjectFullPdt.get_entity(
            self._get_detailed_etl_project(
                db_session, etl_project_version=etl_project_version
            )
        )

    async def process_backend_api_create_project(
        self, etl_project_version: EtlProjectVersion
    ) -> None:
        url = join_urls(
            self.settings.backend_uri_dev, "internal", "etl", "create"
        )
        json_body = {
            "name": etl_project_version.etl_project.name,
            "description": etl_project_version.etl_project.description,
            "author_name": etl_project_version.author_name,
            "author_email": etl_project_version.author_email,
            "general_hub_id": etl_project_version.etl_project.hub_id,
            "hub_name": etl_project_version.etl_project.hub.name,
            "jira_task": etl_project_version.jira_task,
            "cron": etl_project_version.schedule_interval,
            "general_etl_project_id": etl_project_version.etl_project.id,
            "version": etl_project_version.version,
        }

        async with aiohttp.ClientSession() as session:
            response = await session.post(url, json=json_body)
            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                raise ThirdPartyServiceError(
                    f"Error on the backend api side:\n{response_text}"
                )

            return await response.json()

    async def process_git_manager_create_project(
        self,
        etl_project_version: EtlProjectVersion,
        previous_project_version: Optional[str],
    ) -> None:
        if (
            etl_project_version.etl_project.git_flow_type
            == GitFlowType.ONE_REPO
        ):
            url = join_urls(
                self.settings.git_manager_uri,
                "internal",
                "v2",
                "etl_project",
                "branch",
            )
        else:
            url = join_urls(
                self.settings.git_manager_uri,
                "internal",
                "v1",
                "etl_project",
                "branch",
            )

        request_data = PdtProjectCreateParams(
            hub=etl_project_version.etl_project.hub.name,
            project=etl_project_version.etl_project.name,
            username=etl_project_version.author_name,
            email=etl_project_version.author_email,
            schedule_interval=etl_project_version.schedule_interval,
            description=etl_project_version.etl_project.description,
            jira_task=etl_project_version.jira_task,
            project_type=etl_project_version.etl_project.project_type,
            project_version=etl_project_version.version,
            previous_project_version=previous_project_version,
        )

        async with aiohttp.ClientSession() as session:
            response = await session.post(url, json=request_data.dict())

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                raise ThirdPartyServiceError(
                    f"Error on the git manager side:\n{response_text}"
                )

            response_model = ProjectGenerationResponse.parse_obj(
                await response.json()
            )

        etl_project_version.branch_name = response_model.branch_name

        if (
            etl_project_version.etl_project.git_flow_type
            == GitFlowType.ONE_REPO
        ):
            etl_project_version.git_prod_branch_uri = (
                response_model.git_branch_url
            )
        else:
            etl_project_version.git_dev_branch_uri = (
                response_model.git_branch_url
            )

    @etl_project_router.get(path=route + "{etl_id}/{status_type}/status_info")
    def status_info(
        self,
        etl_id: int,
        status_type: EtlProjectStatus,
        version: str,
        db_session: Session = Depends(db.get_session),
    ) -> EtlStatusInfoPdt:
        """
        Возвращает информацию, для заданного статуса для текущего etl project
        Args:
            etl_id: id etl_project, для которого необходимо получит информацию
            version: версия etl_project, для которой необходимо получит информацию
            status_type: тип статуса, для которого требуется сформировать информацию
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if status_type in {
            EtlProjectStatus.PRODUCTION,
            EtlProjectStatus.PROD_REVIEW,
        }:
            url = etl_project_version.git_prod_branch_uri
        else:
            url = etl_project_version.git_dev_branch_uri

        content = {
            "git_branch": {"name": etl_project_version.branch_name, "url": url}
        }

        return EtlStatusInfoPdt.get_entity(content)

    async def _send_project_to_git_airflow(
        self, etl_project_version: EtlProjectVersion
    ):
        url = join_urls(
            self.settings.git_manager_uri,
            "internal",
            "v2",
            "etl_project",
            "airflow",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.post(
                url,
                json={
                    "etl_project_version": jsonable_encoder(
                        EtlVersionInternalPdt.get_entity(etl_project_version)
                    )
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(f"Error on git manager: {response_text}")
                raise ThirdPartyServiceError(f"Backend error: {response_text}")

    @etl_project_router.post(path=route + "{etl_id}/airflow")
    async def send_to_airflow(
        self,
        etl_id: int,
        version: str = Body(embed=True),
        db_session: Session = Depends(db.get_session),
    ) -> None:
        """
        Отправляет etl project тестироваться в airflow
        Args:
            etl_id: id etl_project, который необходимо отправить на тест в airflow
            version: версия etl_project, которую необходимо отправить на тест в airflow
            db_session: сессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if etl_project_version.status not in {
            EtlProjectStatus.DEVELOPING,
            EtlProjectStatus.TESTING,
            EtlProjectStatus.PROD_REVIEW,
        }:
            raise StatusUpdateError(
                f"You cannot send project for testing to "
                f"Airflow with status `{etl_project_version.status.value}`"
            )

        if (
            etl_project_version.etl_project.git_flow_type
            != GitFlowType.ONE_REPO
        ):
            raise StatusUpdateError(
                "You cannot send project for testing to Airflow while you are working by old pipeline!"
            )

        if self.settings.use_git_manager:
            await self._send_project_to_git_airflow(etl_project_version)

        if (
            etl_project_version.etl_project.git_flow_type
            == GitFlowType.TWO_REPOS
        ):
            git_repo = self.settings.git_repo_dev
        else:
            git_repo = self.settings.git_repo_prod

        synchronize_data = SynchronizeEventRequest(
            etl_project_id=etl_id,
            etl_project_version=etl_project_version.version,
            etl_project_name=etl_project_version.etl_project.name,
            branch_name=f"refs/remotes/origin/{etl_project_version.branch_name}",
            jira_task=etl_project_version.jira_task,
            git_repo=git_repo,
            project_type=etl_project_version.etl_project.project_type,
        )

        self.put_msg(
            event_queue=self.synchronizer_event_queue, data=synchronize_data
        )

    @etl_project_router.delete(path=route + "{etl_id}/airflow")
    async def delete_from_airflow(
        self,
        etl_id: int,
        version: str = Body(embed=True),
        db_session: Session = Depends(db.get_session),
    ) -> None:
        """
        Удаляет etl project из ветки для airflow
        Args:
            etl_id: id etl_project, который необходимо удалить из ветки для airflow
            version: версия etl_project, которую необходимо удалить из ветки для airflow
            db_session: cессия базы данных
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            return None

        if (
            etl_project_version.etl_project.git_flow_type
            != GitFlowType.ONE_REPO
        ):
            raise StatusUpdateError(
                "Your project cannot be in new Airflow while you are working by old pipeline!"
            )

        if self.settings.use_git_manager:
            if not await self._delete_from_airflow_branch(etl_project_version):
                raise ThirdPartyServiceError(
                    "Backend error etl project did't delete from airflow branch"
                )

    @etl_project_router.get(path=route + "{etl_id}/history_event")
    def get_etl_project_history_event(
            self,
            etl_id: int,
            version: str,
            db_session: Session = Depends(db.get_session),
            limit: Optional[int] = None,
            offset: Optional[int] = None
    ) -> LimitOffsetPage[PdtHistoryEvent]:
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )
        if not etl_project_version:
            raise DataNotFoundException("ETL project was not founded!")

        history = self.data_storage.get_etl_project_history(
            db_session, etl_project_version, limit, offset
        )

        return LimitOffsetPage(items=[PdtHistoryEvent.get_entity(obj=h) for h in history], total=len(history))

    @etl_project_router.get(path=route + "{etl_id}/team")
    async def get_etl_project_team(
        self,
        etl_id: int,
        version: str,
        session_token: str = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> Page[PdtUserWithGroups]:
        """
        Метод, возвращающий список пользователей, входящих в состав команды ETL-проекта

        :param etl_id: Идентификатор ETL-проекта
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException("ETL project was not founded!")

        users: List[User] = etl_project_version.users

        backend_users = await asyncio.gather(*[self.backend_proxy_client.get_user(session_token, usr.user_id) for usr in users])

        response_users = []

        for usr, backend_usr in zip(users, backend_users):
            if backend_usr:
                response_users.append(PdtUserWithGroups.from_backend_user(backend_usr))
            else:
                response_users.append(PdtUserWithGroups.from_db_user(usr))

        return Page(items=response_users, total=len(users))

    @etl_project_router.put(path=route + "{etl_id}/team")
    async def update_etl_project_team(
        self,
        etl_id: int,
        data: List[PdtUser],
        version: str = Body(),
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> None:
        """
        Метод, обновляющий состав команды ETL-проекта

        :param etl_id: Идентификатор ETL-проекта
        :param version: Версия ETL-проекта
        :param data: Список пользователей, которые будут в составе команды ETL-проекта
        """
        if not data:
            raise NotEnoughDataException(
                "List of ETL project team cannot be empty!"
            )

        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException("ETL project was not founded!")

        new_users = self._get_users_list(db_session, users=data)

        await self._apply_history_event(
            db_session=db_session,
            event_name="Редактирование команды",
            etl_project_version_id=etl_project_version.id,
            old_value="\n".join(
                u.display_name for u in etl_project_version.users
            ),
            new_value="\n".join(u.display_name for u in new_users),
            session_token=session_token,
        )

        with db_session.begin_nested():
            etl_project_version.users = new_users
            db_session.add(etl_project_version)
            db_session.commit()

    @etl_project_router.get(path=route + "{etl_id}/check")
    def get_etl_project_check(
        self,
        etl_id: int,
        version: str,
        db_session: Session = Depends(db.get_session),
    ) -> Optional[PdtProjectState]:
        """
        Получение списка проверок ETL-проекта
        :param etl_id: Идентификатор ETL-проекта
        :param version: Версия ETL-проекта
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        general_check = (
            db_session.query(GeneralCheck)
            .filter(
                GeneralCheck.etl_project_version_id == etl_project_version.id
            )
            .order_by(GeneralCheck.created_timestamp.desc())
            .first()
        )

        if not general_check:
            return None

        project_transfer_request = first(
            self.data_storage.get_last_transfer_requests(
                db_session, versions_ids=(etl_project_version.id,)
            ),
            None,
        )

        if project_transfer_request:
            transfer_request_result = project_transfer_request.result
        else:
            transfer_request_result = None

        pdt_general_check = PdtGeneralCheck.get_entity(general_check)
        pdt_check_status = PdtProjectState(
            general_check=pdt_general_check,
            git_status=PdtGitStatus(
                result=transfer_request_result,
                message="",
            ),
        )

        return pdt_check_status

    @etl_project_router.get(path=route + "{etl_id}/check_list")
    def get_etl_project_checks(
        self,
        etl_id: int,
        version: str,
        db_session: Session = Depends(db.get_session),
    ) -> Page[PdtGeneralCheck]:
        """
        Получение списка проверок ETL-проекта
        :param etl_id: Идентификатор ETL-проекта
        :param version: Версия ETL-проекта
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            raise DataNotFoundException("ETL project was not founded!")

        checks = etl_project_version.checks
        pdt_checks = PdtGeneralCheck.get_entity(checks)

        return Page(items=pdt_checks, total=len(checks))

    @etl_project_router.post(path=route + "{etl_id}/check")
    def check_etl_project(
        self,
        etl_id: int,
        version: str = Body(embed=True),
        db_session: Session = Depends(db.get_session),
    ):
        """
        Запуск проверки ETL-проекта
        :param etl_id: Идентификатор ETL-проекта
        :param version: Версия ETL-проекта
        """
        etl_project_version: EtlProjectVersion = (
            self.data_storage.get_etl_project_version(
                db_session,
                etl_project_id=etl_id,
                etl_project_version=version,
            )
        )

        if not etl_project_version:
            return None

        self._run_check_etl_project(
            db_session,
            etl_project_version=etl_project_version,
            check_type=CheckType.TESTING,
        )

    @etl_project_router.get(path=route + "{etl_id}/permissions")
    async def get_user_permissions_for_project(
        self,
        version: str,
        etl_id: int,
        session_token: Optional[str] = Cookie(default=None),
        db_session: Session = Depends(db.get_session),
    ) -> EtlProjectUserPermissionsPdt:
        current_user = await self.current_user(session_token)
        etl_project_version = self.data_storage.get_etl_project_version(
            db_session, etl_project_id=etl_id, etl_project_version=version
        )
        permissions = get_permissions_for_user_with_project(
            current_user, etl_project_version
        )

        return permissions

    def _run_check_etl_project(
        self,
        db_session: Session,
        etl_project_version: EtlProjectVersion,
        check_type: CheckType = CheckType.REVIEW,
        user_data: Optional[UserRequestData] = None,
    ) -> None:
        with self.data_storage.check_lock:
            general_check = GeneralCheck(
                check_type=check_type, result=ProjectCheckResult.PROCESSING
            )

            etl_project_version.checks.append(general_check)

            with db_session.begin_nested():
                db_session.add(etl_project_version)
                db_session.commit()

            # TODO Придумать более качественный механизм
            if (
                etl_project_version.etl_project.git_flow_type
                == GitFlowType.TWO_REPOS
            ):
                git_repo = self.settings.git_repo_dev
            else:
                git_repo = self.settings.git_repo_prod

            check = CheckEventRequest(
                etl_project_id=etl_project_version.etl_project.id,
                etl_project_version=etl_project_version.version,
                etl_project_name=etl_project_version.etl_project.name,
                branch_name=f"refs/remotes/origin/{etl_project_version.branch_name}",
                jira_task=etl_project_version.jira_task,
                general_check_id=general_check.id,
                check_type=check_type,
                git_repo=git_repo,
                user_data=user_data,
                project_type=etl_project_version.etl_project.project_type,
            )

            self.put_msg(event_queue=self.checker_event_queue, data=check)

    async def _delete_from_airflow_branch(
        self, etl_project_version: EtlProjectVersion
    ) -> bool:
        url = join_urls(
            self.settings.git_manager_uri,
            "internal",
            "v2",
            "etl_project",
            "airflow",
        )

        async with aiohttp.ClientSession() as session:
            response = await session.delete(
                url,
                json={
                    "etl_project_version": jsonable_encoder(
                        EtlVersionInternalPdt.get_entity(etl_project_version)
                    )
                },
            )

            if not (200 <= response.status < 300):
                response_text = await response.text()
                self.logger.error(response_text)
                return False

        return True

    async def _apply_history_event(
        self,
        db_session: Session,
        event_name: str,
        etl_project_version_id: int,
        session_token: str,
        old_value=None,
        new_value=None,
        extra_data: Optional[List] = None,
    ) -> None:
        current_user = await self.current_user(session_token)

        if current_user:
            self.data_storage.add_history_event(
                db_session=db_session,
                name=event_name,
                etl_project_version_id=etl_project_version_id,
                new_value=new_value,
                old_value=old_value,
                author_name=current_user.display_name,
                extra_data=extra_data,
            )
