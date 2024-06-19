import asyncio
from threading import Lock
from typing import List, Iterable, Optional, Tuple, Generator
from functools import wraps

from fs_common_lib.fs_backend_api.internal_dto import InternalPdtEtlRun
from fs_common_lib.fs_general_api.data_types import EtlProjectStatus
from fs_db.db_classes_general import (
    EtlProject,
    EtlProjectVersion,
    HistoryEvent,
    ProjectTransferRequest,
    User,
)
from fs_db.metadata_storage import MetadataStorage, Database
from sqlalchemy import func, create_engine, tuple_, or_
from sqlalchemy.orm import Session, joinedload, selectinload

from fs_general_api.config import settings


class MetadataStorageGeneral(MetadataStorage):
    def __init__(self):
        self.check_lock: Lock = Lock()

    @staticmethod
    def get_projects_by_list(
        db_session: Session, versions_data: Iterable[Tuple[int, str]]
    ) -> List[EtlProjectVersion]:
        if not versions_data:
            return []

        return (
            db_session.query(EtlProjectVersion)
            .options(joinedload(EtlProjectVersion.etl_project))
            .filter(
                tuple_(
                    EtlProjectVersion.etl_project_id, EtlProjectVersion.version
                ).in_(versions_data)
            )
            .all()
        )

    @staticmethod
    def get_last_transfer_requests(
        db_session: Session, versions_ids: Iterable
    ) -> List[ProjectTransferRequest]:
        if not versions_ids:
            return []

        subq = (
            db_session.query(
                func.max(ProjectTransferRequest.id).label("max_id")
            )
            .filter(
                ProjectTransferRequest.etl_project_version_id.in_(versions_ids)
            )
            .group_by(ProjectTransferRequest.etl_project_version_id)
            .subquery("t1")
        )

        return (
            db_session.query(ProjectTransferRequest)
            .options(selectinload(ProjectTransferRequest.etl_project_version))
            .filter(ProjectTransferRequest.id.in_(subq))
            .with_for_update().all()
        )

    @staticmethod
    def get_projects_to_transfer(
        db_session: Session,
    ) -> List[EtlProjectVersion]:
        """
        Получение списка проектов для трансфера
        """
        etl_projects_versions = (
            db_session.query(EtlProjectVersion)
            .join(EtlProjectVersion.project_transfer_requests)
            .options(selectinload(EtlProjectVersion.project_transfer_requests))
            .filter(
                EtlProjectVersion.status.in_([EtlProjectStatus.TESTING]),
                or_(
                    ProjectTransferRequest.result.is_(None),
                    ProjectTransferRequest.result == "RETRYING"
                ),
                ProjectTransferRequest.error_msg.is_(None),
            ).with_for_update().all()
        )

        return etl_projects_versions

    @staticmethod
    def get_projects_with_schedule_in_production(db_session: Session) -> List[EtlProjectVersion]:
        return (
            db_session.query(EtlProjectVersion)
            .options(joinedload(EtlProjectVersion.etl_project))
            .filter(
                EtlProjectVersion.schedule_interval.isnot(None),
                EtlProjectVersion.status == EtlProjectStatus.PRODUCTION,
            )
            .all()
        )

    @staticmethod
    def get_users_by_list_id(session: Session, ids: Iterable) -> List[User]:
        return session.query(User).filter(User.user_id.in_(ids)).all()

    @staticmethod
    def get_etl_project_history(
            db_session: Session,
            etl_project_version: EtlProjectVersion,
            limit: Optional[int] = None,
            offset: Optional[int] = None
    ) -> List[HistoryEvent]:
        query = (
            db_session.query(HistoryEvent)
            .filter(
                HistoryEvent.etl_project_version_id == etl_project_version.id
            )
            .order_by(HistoryEvent.created_timestamp.desc())
        )

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        return query.all()

    @staticmethod
    def add_history_event(
        db_session: Session,
        name: str,
        etl_project_version_id: int,
        author_name: Optional[str] = None,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        extra_data: Optional[List] = None,
    ):
        history_event = HistoryEvent(
            name=name,
            old_value=old_value,
            new_value=new_value,
            author=author_name,
            etl_project_version_id=etl_project_version_id,
            extra_data=extra_data,
        )

        with db_session.begin_nested():
            db_session.add(history_event)
            db_session.commit()

    @staticmethod
    def get_etl_project_version(
        db_session: Session, etl_project_id: int, etl_project_version: str
    ) -> Optional[EtlProjectVersion]:
        return (
            db_session.query(EtlProjectVersion)
            .join(EtlProjectVersion.etl_project)
            .options(joinedload(EtlProjectVersion.etl_project))
            .filter(
                EtlProject.id == etl_project_id,
                EtlProjectVersion.version == etl_project_version,
            )
            .first()
        )

    def turn_off_active_project_version(
        self,
        db_session: Session,
        new_etl_version_id: int,
        etl_project_id: int,
        author_name: str,
    ) -> Optional[EtlProjectVersion]:
        active_version: EtlProjectVersion = (
            db_session.query(EtlProjectVersion)
            .filter(
                EtlProjectVersion.id != new_etl_version_id,
                EtlProjectVersion.etl_project_id == etl_project_id,
                EtlProjectVersion.status == EtlProjectStatus.PRODUCTION,
            )
            .first()
        )

        if not active_version:
            return

        new_status = EtlProjectStatus.TURNED_OFF
        self.add_history_event(
            db_session=db_session,
            name='Изменение статуса',
            etl_project_version_id=active_version.id,
            old_value=active_version.status.value,
            new_value=new_status.value,
            author_name=author_name
        )

        active_version.status = new_status

        with db_session.begin_nested():
            db_session.add(active_version)
            db_session.commit()

        return active_version

    async def update_etl_project_status(
        self,
        db_session: Session,
        etl_project_version: EtlProjectVersion,
        etl_run: InternalPdtEtlRun,
        status: EtlProjectStatus,
        author_name: str,
    ):
        self.add_history_event(
            db_session=db_session,
            name='Изменение статуса',
            etl_project_version_id=etl_project_version.id,
            old_value=etl_project_version.status.value,
            new_value=status.value,
            author_name=author_name
        )
        etl_project_version.status = status

        if status == EtlProjectStatus.TESTING:
            etl_project_version.moved_to_testing_timestamp = etl_run.run_ts
        elif status == EtlProjectStatus.PROD_RELEASE:
            etl_project_version.moved_to_prod_release_timestamp = etl_run.run_ts
        elif status == EtlProjectStatus.PRODUCTION:
            etl_project_version.moved_to_prod_release_timestamp = etl_run.run_ts
            etl_project_version.moved_to_production_timestamp = etl_run.run_ts
        else:
            raise NotImplementedError(f"Updating for status={status} is not implemented!")

        with db_session.begin_nested():
            db_session.add(etl_project_version)
            db_session.commit()

        return etl_project_version


data_storage = MetadataStorageGeneral()

db = Database()
db.engine = create_engine(
    settings.connection_uri,
    connect_args={"options": f"-csearch_path={settings.schema_name}"},
)


def get_data_storage() -> MetadataStorageGeneral:
    return data_storage


def task_db_session(func_):
    is_coroutine = asyncio.iscoroutinefunction(func_)

    @wraps(func_)
    async def wrapper(*args, **kwargs):
        db_session_gen: Generator[Session, None, None] = db.get_session()

        try:
            # get sqlalchemy session
            db_session = next(db_session_gen, None)
            if is_coroutine:
                await func_(db_session=db_session, **kwargs)
            else:
                func_(db_session=db_session, **kwargs)
        finally:
            # close sqlalchemy session
            next(db_session_gen, None)

    return wrapper
