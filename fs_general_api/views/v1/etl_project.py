import asyncio
from functools import partial
from datetime import datetime
from typing import Optional, List

from fastapi import Body, Query
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from fs_common_lib.fs_general_api.data_types import (
    EtlProjectStatus,
    GitFlowType,
    ProjectType,
)
from fs_common_lib.fs_general_api.internal_dto import InternalPdtEtl
from fs_common_lib.fs_registry_api import join_urls, _get, _post, _delete
from fs_common_lib.fs_git_manager.internal_dto import (
    PdtProjectCreateParams,
    ProjectGenerationResponse,
)
from fs_common_lib.utils.cron_utils import get_description_by_schedule_interval

from fs_db.db_classes_general import EtlProject, ProjectTransferRequest

from fs_general_api.dto.etl_project import PdtEtl
from fs_general_api.dto.user import PdtUser
from fs_general_api.exceptions import (
    DataNotFoundException,
    ThirdPartyServiceError,
    StatusUpdateError,
    RecordAlreadyExistsException,
    ProjectModificationError,
    NotEnoughDataException,
)
from fs_general_api.views import BaseRouter

etl_project_router = InferringRouter()


@cbv(etl_project_router)
class EtlProjectServer(BaseRouter):
    __model_class__ = EtlProject

    route = "/etl/"

    @etl_project_router.get(path=route + "list", response_model=dict)
    async def get_list(
        self,
        hub_id: Optional[int] = None,
        user_id: Optional[int] = None,
        limit: Optional[int] = Query(default=20),
        offset: Optional[int] = Query(default=0),
        search: Optional[str] = None,
        status: Optional[EtlProjectStatus] = None,
    ) -> JSONResponse:
        """
        Возвращает JSONResponse, содержащий список etl проектов
        Args:
            hub_id: - в каких хабах искать проекты
            user_id: - проекты какого пользователя вернуть
            search: - запрос на поиск
            limit: - количество возвращаемых записей
            offset: - отступ, откуда необходимо брать необходимое число записей
            status: - статус с которым необходимо вернуть проекты
        """
        query = self.pg_connector.session.query(EtlProject)
        if hub_id is not None:
            query = query.filter(EtlProject.hub_id == hub_id)

        if user_id is not None:
            query = query.filter(EtlProject.user_id == user_id)

        if status is not None:
            query = query.filter(EtlProject.status == status)

        if search is not None:
            query = query.filter(
                EtlProject.name.ilike(f"%{search}%")
                | EtlProject.description.ilike(f"%{search}%")
            )

        total = query.count()

        query = query.limit(limit).offset(offset)

        dataset = query.all()
        pdt_etl = []

        for data in dataset:
            pdt_etl.append(self._data_enrichment(data=data))

        return JSONResponse(
            jsonable_encoder({"items": pdt_etl, "total": total})
        )

    def _data_enrichment(self, data: EtlProject) -> PdtEtl:
        pdt_etl = PdtEtl.get_entity(data)
        pdt_etl.schedule_interval_description = (
            get_description_by_schedule_interval(data.schedule_interval)
        )
        if data.jira_task is not None:
            pdt_etl.jira_task_url = self.settings.jira_uri + data.jira_task

        return pdt_etl

    @etl_project_router.get(path=route + "{etl_id}", response_model=dict)
    async def get_etl_project(self, etl_id: int) -> JSONResponse:
        """
        Возвращает etl project, полученный по etl_id
        Args:
            etl_id: id etl_project, который необходимо получить
        """
        dataset = self.get_by_id(etl_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Etl project by etl_id=`{etl_id}` didn't find!"
            )

        if dataset.status == EtlProjectStatus.DEVELOPING:
            dataset = self._update_etl_status(
                dataset=dataset,
                backend_url=self.settings.backend_uri_dev,
                status=EtlProjectStatus.TESTING,
            )
        elif dataset.status == EtlProjectStatus.PROD_REVIEW:
            dataset = self._update_etl_status(
                dataset=dataset,
                backend_url=self.settings.backend_uri_prod,
                status=EtlProjectStatus.PRODUCTION,
            )

        content = self._data_enrichment(data=dataset)

        return JSONResponse(jsonable_encoder(content))

    def _update_etl_status(
        self, dataset: EtlProject, backend_url: str, status: EtlProjectStatus
    ) -> EtlProject:
        url = join_urls(
            backend_url, "internal", "etl", dataset.id, "first_successful_run"
        )
        response = _get(url)
        if not (200 <= response.status_code < 300):
            self.logger.error(response.text)
            raise ThirdPartyServiceError(
                f"Error on the backend api side:\n{response.text}"
            )
        first_run_success_date = response.json()
        if first_run_success_date:
            dataset.status = status
            if status == EtlProjectStatus.TESTING:
                dataset.moved_to_testing_timestamp = first_run_success_date
            elif status == EtlProjectStatus.PRODUCTION:
                dataset.moved_to_production_timestamp = first_run_success_date
            else:
                raise NotImplementedError(
                    f"Updating for status={status} is not implemented!"
                )
            self.pg_connector.session.add(dataset)
            self.pg_connector.session.commit()
        return dataset

    @etl_project_router.put(path=route + "{etl_id}", response_model=dict)
    async def update_project(
        self, etl_id: int, user_id: int = Body(), description: str = Body()
    ) -> JSONResponse:
        """
        Обновляет etl project по etl_id
        Args:
            etl_id: id etl_project, который необходимо удалить
            user_id: id пользователя для проверки, может ли он удалить проект
            description: Описание проекта, на которое требуется поменять
        """
        dataset = self.get_by_id(etl_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Etl project by etl_id=`{etl_id}` didn't find!"
            )

        if dataset.user_id != user_id:
            self.logger.error(
                f"This is project of `{dataset.user_id}`, but user=`{user_id}` is trying to update this!"
            )
            raise ProjectModificationError("You cannot update this project!")

        dataset.description = description
        self.pg_connector.session.commit()

        content = self._data_enrichment(data=dataset)

        return JSONResponse(jsonable_encoder(content))

    @etl_project_router.delete(path=route + "{etl_id}")
    async def delete_project(
        self, etl_id: int, user_id: int = Body(embed=True)
    ):
        """
        Удаляет etl project по etl_id
        Args:
            etl_id: id etl_project, который необходимо удалить
            user_id: id пользователя для проверки, может ли он удалить проект
        """
        dataset: EtlProject = self.get_by_id(etl_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Etl project by etl_id=`{etl_id}` didn't find!"
            )

        if dataset.status != EtlProjectStatus.DEVELOPING:
            raise ProjectModificationError(
                f"You cannot delete project in status `{dataset.status}`"
            )
        if dataset.user_id != user_id:
            self.logger.error(
                f"This is project of `{dataset.user_id}`, but user=`{user_id}` is trying to delete this!"
            )
            raise ProjectModificationError("You cannot delete this project")

        delete_url = join_urls(
            self.settings.backend_uri_dev,
            "internal",
            "etl",
            etl_id,
            "by_general_id",
        )
        resp = _delete(delete_url)

        if self.settings.use_git_manager_for_deletion and dataset.jira_task:
            if dataset.git_flow_type == GitFlowType.ONE_REPO:
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
            self.logger.info("Start to delete project branch")
            event_loop = asyncio.get_event_loop()
            response = await event_loop.run_in_executor(self.tpe, partial(_delete, url=url, json={'jira_task': dataset.jira_task, 'project_name': dataset.name}))
            if not (200 <= response.status_code < 300):
                self.logger.error(response.text)
            self.logger.info("Start to delete project from airflow branch")
            await self._delete_from_airflow_branch(dataset)

        if 200 <= resp.status_code < 300:
            self.pg_connector.session.delete(dataset)
            self.pg_connector.session.commit()
        else:
            self.logger.error(f"Error on backend: {resp.content}")
            raise ThirdPartyServiceError(f"Backend error: {resp.content}")

    @etl_project_router.post(
        path=route + "{etl_id}/send_to_prod", response_model=dict
    )
    async def send_to_prod(
        self,
        etl_id: int,
        user_name: str = Body(default=None),
        author_name: str = Body(default=None),
        author_email: str = Body(default=None),
    ) -> JSONResponse:
        """
        Возвращает etl project, полученный по etl_id с обновлённым статусом
        Args:
            etl_id: id etl_project, который необходимо перевести в статус "Заявка на переход в прод" и вернуть объект
            user_name: логин автора, отправившего запрос на перенос проекта в прод
            author_name: имя автора, отправившего запрос в прод
            author_email: email автора, отправившего запрос в прод
        """
        dataset = self.get_by_id(etl_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Etl project by etl_id=`{etl_id}` didn't find!"
            )

        if dataset.status == EtlProjectStatus.DEVELOPING:
            raise StatusUpdateError(
                "You cannot send etl project to production without successful runs!"
            )
        elif dataset.status == EtlProjectStatus.PROD_REQUEST:
            raise StatusUpdateError(
                f"Etl project with id=`{etl_id}` already in review"
            )
        elif dataset.status in {
            EtlProjectStatus.PROD_REVIEW,
            EtlProjectStatus.PRODUCTION,
        }:
            raise StatusUpdateError(
                f"Etl project with id=`{etl_id}` already in production with status `{dataset.status.value}`"
            )

        dataset.status = EtlProjectStatus.PROD_REQUEST
        dataset.moved_to_prod_request_timestamp = datetime.now()

        transfer_request = ProjectTransferRequest(
            user_name=user_name,
            author_email=author_email,
            author_name=author_name,
            etl_project_id=etl_id,
        )

        self.pg_connector.session.add(dataset)
        self.pg_connector.session.add(transfer_request)
        self.pg_connector.session.commit()

        content = PdtEtl.get_entity(dataset)

        return JSONResponse(jsonable_encoder(content))

    @etl_project_router.post(path=route + "create", response_model=dict)
    async def create(
        self,
        name: str = Body(),
        description: Optional[str] = Body(default=None),
        user_id: Optional[int] = Body(default=None),
        author_name: str = Body(),
        author_email: str = Body(),
        hub_id: int = Body(),
        jira_task: str = Body(),
        cron: str = Body(),
        git_flow_type: GitFlowType = Body(default=GitFlowType.TWO_REPOS),
        project_type: ProjectType = Body(default=ProjectType.FEATURES),
    ) -> JSONResponse:
        """
        Возвращает etl project, полученный после создания
        Args:
            name: имя etl_project
            description: описание etl_project
            user_id: id пользователя, создавшего данный проект
            author_email: email etl_project - отправляется на создание в backend
            author_name: имя автора etl_project - отправляется на создание в backend
            hub_id: id хаба etl_project - отправляется на создание в backend
            cron: cron строка в какой момент должен запускаться etl_project
            jira_task: указатель на задачу в jira - отправляется на создание в backend
            project_type: тип выходных данных у etl-проекта (витрины или агрегаты)
            git_flow_type: тип git flow использумый в проекте (с одним или двумя репозиториями)
        """
        etl_project = self.pg_connector.find_record_by_name(
            clazz=EtlProject, name=name
        )
        if etl_project is not None:
            raise RecordAlreadyExistsException(
                f"General Etl Project with name=`{name} already exists!`"
            )
        # todo Ветку в git называем feature/jira_task и возвращать ссылку на git где будет автоматически заявка на ПР создаваться

        if (
            git_flow_type == GitFlowType.TWO_REPOS
            and project_type == ProjectType.AGGREGATES
        ):
            raise ThirdPartyServiceError(
                "Aggregates does not support git flow type with two repos"
            )

        etl_project = EtlProject(
            name=name,
            description=description,
            status=EtlProjectStatus.DEVELOPING,
            jira_task=jira_task,
            hub_id=hub_id,
            author_name=author_name,
            author_email=author_email,
            user_id=user_id,
            schedule_interval=cron,
            git_flow_type=git_flow_type,
            project_type=project_type.value,
        )

        self.pg_connector.session.add(etl_project)
        self.pg_connector.session.flush()

        try:
            url = join_urls(
                self.settings.backend_uri_dev, "internal", "etl", "create"
            )
            json_body = {
                "name": name,
                "description": description,
                "author_name": author_name,
                "author_email": author_email,
                "general_hub_id": hub_id,
                "hub_name": etl_project.hub.name,
                "jira_task": jira_task,
                "cron": cron,
                "general_etl_project_id": etl_project.id,
            }

            response = _post(url=url, json=json_body)

            if not (200 <= response.status_code < 300):
                self.logger.error(response.text)
                raise ThirdPartyServiceError(
                    f"Error on the backend api side:\n{response.text}"
                )

            backend_etl_project = response.json()

            if self.settings.use_git_manager:
                if git_flow_type == GitFlowType.ONE_REPO:
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

                params = PdtProjectCreateParams(
                    hub=backend_etl_project.get("hub").get("name"),
                    project=name,
                    username=author_name,
                    email=author_email,
                    schedule_interval=backend_etl_project.get(
                        "schedule_interval"
                    ),
                    description=description,
                    jira_task=jira_task,
                    project_type=project_type.value,
                )

                event_loop = asyncio.get_event_loop()
                response = await event_loop.run_in_executor(
                    self.tpe, partial(_post, url=url, json=params.dict())
                )
                if not (200 <= response.status_code < 300):
                    self.logger.error(response.text)
                else:
                    project_generation_response = (
                        ProjectGenerationResponse.parse_obj(response.json())
                    )
                    etl_project.branch_name = (
                        project_generation_response.branch_name
                    )
                    if git_flow_type == GitFlowType.ONE_REPO:
                        etl_project.git_prod_branch_uri = (
                            project_generation_response.git_branch_url
                        )
                    else:
                        etl_project.git_dev_branch_uri = (
                            project_generation_response.git_branch_url
                        )

            self.pg_connector.session.commit()
        except:
            self.pg_connector.session.rollback()
            raise
        pdt_etl = PdtEtl.get_entity(etl_project)

        return JSONResponse(jsonable_encoder(pdt_etl))

    @etl_project_router.get(
        path=route + "{etl_id}/{status_type}/status_info", response_model=dict
    )
    async def status_info(
        self, etl_id: int, status_type: EtlProjectStatus
    ) -> JSONResponse:
        """
        Возвращает информацию, для заданного статуса для текущего etl project
        Args:
            etl_id: id etl_project, для которого необходимо получит информацию
            status_type: тип статуса, для которого требуется сформировать информацию
        """
        etl_project = self.get_by_id(etl_id)

        if status_type in {
            EtlProjectStatus.PRODUCTION,
            EtlProjectStatus.PROD_REVIEW,
        }:
            url = etl_project.git_prod_branch_uri
        else:
            url = etl_project.git_dev_branch_uri

        content = {"git_branch": {"name": etl_project.branch_name, "url": url}}

        return JSONResponse(jsonable_encoder(content))

    @etl_project_router.post(path=route + "{etl_id}/airflow")
    async def send_to_airflow(self, etl_id: int):
        """
        Отправляет etl project тестироваться в airflow
        Args:
            etl_id: id etl_project, который необходимо отправить на тест в airflow
        """
        etl_project = self.get_by_id(etl_id)

        if etl_project.status not in {
            EtlProjectStatus.DEVELOPING,
            EtlProjectStatus.TESTING,
            EtlProjectStatus.PROD_REVIEW,
        }:
            raise StatusUpdateError(
                f"You cannot send project for testing to Airflow with status `{etl_project.status.value}`"
            )

        if etl_project.git_flow_type != GitFlowType.ONE_REPO:
            raise StatusUpdateError(
                f"You cannot send project for testing to Airflow while you are working by old pipeline!"
            )

        if self.settings.use_git_manager:
            url = join_urls(
                self.settings.git_manager_uri,
                "internal",
                "v2",
                "etl_project",
                "airflow",
            )
            event_loop = asyncio.get_event_loop()
            resp = await event_loop.run_in_executor(
                self.tpe,
                partial(
                    _post,
                    url=url,
                    json={
                        "etl_project": jsonable_encoder(
                            InternalPdtEtl.get_entity(etl_project)
                        )
                    },
                ),
            )
            if not (200 <= resp.status_code < 300):
                self.logger.error(f"Error on git manager: {resp.content}")
                raise ThirdPartyServiceError(f"Backend error: {resp.content}")

    @etl_project_router.delete(path=route + "{etl_id}/airflow")
    async def delete_from_airflow(self, etl_id: int):
        """
        Удаляет etl project из ветки для airflow
        Args:
            etl_id: id etl_project, который необходимо удалить из ветки для airflow
        """
        etl_project = self.get_by_id(etl_id)

        if etl_project.git_flow_type != GitFlowType.ONE_REPO:
            raise StatusUpdateError(
                f"Your project cannot be in new Airflow while you are working by old pipeline!"
            )

        if self.settings.use_git_manager:
            if not await self._delete_from_airflow_branch(etl_project):
                raise ThirdPartyServiceError(
                    f"Backend error etl project did't delete from airflow branch"
                )

    @etl_project_router.get(path=route + "{etl_id}/team", response_model=dict)
    async def get_etl_project_team(self, etl_id: int):
        """
        Метод, возвращающий список пользователей, входящих в состав команды ETL-проекта

        :param etl_id: Идентификатор ETL-проекта
        """
        etl_project_model = self.get_by_id(etl_id)

        if not etl_project_model:
            raise DataNotFoundException("ETL project was not founded!")

        users = etl_project_model.users
        total = len(users)
        pdt_users = PdtUser.get_entity(users)

        return JSONResponse(
            jsonable_encoder({"items": pdt_users, "total": total})
        )

    @etl_project_router.put(path=route + "{etl_id}/team")
    async def update_etl_project_team(self, etl_id: int, users: List[PdtUser]):
        """
        Метод, обновляющий состав команды ETL-проекта

        :param etl_id: Идентификатор ETL-проекта
        :param users: Список пользователей, которые будут в составе команды ETL-проекта
        """
        if not users:
            raise NotEnoughDataException(
                "List of ETL project team cannot be empty!"
            )

        etl_project_model = self.get_by_id(etl_id)

        if not etl_project_model:
            raise DataNotFoundException("ETL project was not founded!")

        etl_project_model.users = self._get_users_list(users)

        self.pg_connector.session.add(etl_project_model)
        self.pg_connector.session.commit()

    async def _delete_from_airflow_branch(
        self, etl_project: EtlProject
    ) -> bool:
        url = join_urls(
            self.settings.git_manager_uri,
            "internal",
            "v2",
            "etl_project",
            "airflow",
        )
        event_loop = asyncio.get_event_loop()
        resp = await event_loop.run_in_executor(
            self.tpe,
            partial(
                _delete,
                url=url,
                json={
                    "etl_project": jsonable_encoder(
                        InternalPdtEtl.get_entity(etl_project)
                    )
                },
            ),
        )
        if not (200 <= resp.status_code < 300):
            self.logger.error(f"Error on git manager: {resp.content}")
            return False
        return True
