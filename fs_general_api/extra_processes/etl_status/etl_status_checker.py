import asyncio
from typing import Optional, List, Tuple
from collections import deque

import aiohttp
from sqlalchemy import tuple_
from sqlalchemy.orm import Session, joinedload
from fs_common_lib.fs_registry_api import join_urls
from fs_common_lib.fs_general_api.data_types import EtlProjectStatus
from fs_db.db_classes_general import EtlProjectVersion
from fs_common_lib.fs_logger.fs_logger import FsLoggerHandler

from fs_general_api.extra_processes.etl_status.definitions import PeriodParams, PeriodType, LastRunsForEtlProject
from fs_general_api.config import settings
from fs_general_api.db import data_storage

logger = FsLoggerHandler(
    __name__,
    level=settings.log_level,
    log_format=settings.log_format,
    datefmt=settings.datefmt).get_logger()


async def update_etl_projects_statuses(db_session: Session, dev_used_cache: deque, prod_used_cache: deque):
    period = PeriodParams(value=1, type=PeriodType.hour)
    logger.info(f"start updating etl project statuses")

    await _update_etl_projects_from_dev(db_session, period, dev_used_cache)
    await _update_etl_projects_status_from_prod(db_session, period, prod_used_cache)


async def _update_etl_projects_from_dev(db_session: Session, period: PeriodParams, used_cache: deque) -> None:
    projects_versions, last_runs_by_project = await _get_last_etl_project_runs(
        db_session, period, used_cache, settings.backend_uri_dev
    )

    logger.info(
        f"Projects versions for try updating status in dev: {[version.id for version in projects_versions]}, "
        f"with last runs: {last_runs_by_project}")

    tasks_for_update = []

    for version in projects_versions:
        project_run = last_runs_by_project[(version.etl_project_id, version.version)]

        if version.status == EtlProjectStatus.DEVELOPING and project_run.result == "SUCCESS":
            tasks_for_update.append(
                data_storage.update_etl_project_status(
                    db_session=db_session, etl_project_version=version, etl_run=project_run,
                    status=EtlProjectStatus.TESTING, author_name=version.author_name)
            )

    await asyncio.gather(*tasks_for_update)

    used_cache.extend([etl_run.id for etl_run in last_runs_by_project.values()])


async def _update_etl_projects_status_from_prod(db_session: Session, period: PeriodParams, used_cache: deque) -> None:
    projects_versions, last_runs_by_project = await _get_last_etl_project_runs(
        db_session, period, used_cache, settings.backend_uri_prod,
    )

    logger.info(
        f"Projects for try updating status in prod: {[version.id for version in projects_versions]}, "
        f"with last runs: {last_runs_by_project}")

    tasks_for_update = []

    for version in projects_versions:
        project_run = last_runs_by_project[(version.etl_project_id, version.version)]

        if project_run.result == "FAIL" and version.status == EtlProjectStatus.PROD_REVIEW:
            tasks_for_update.append(
                data_storage.update_etl_project_status(
                    db_session=db_session, etl_project_version=version, etl_run=project_run,
                    status=EtlProjectStatus.PROD_RELEASE, author_name=version.author_name)
            )
        elif project_run.result == "SUCCESS" and version.status in [
            EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.PROD_RELEASE
        ]:
            tasks_for_update.append(
                data_storage.update_etl_project_status(
                    db_session=db_session, etl_project_version=version, etl_run=project_run,
                    status=EtlProjectStatus.PRODUCTION, author_name=version.author_name)
            )
            tasks_for_update.append(
                _turn_off_active_project_version(db_session=db_session, etl_project_version=version)
            )

    await asyncio.gather(*tasks_for_update)

    used_cache.extend([etl_run.id for etl_run in last_runs_by_project.values()])


async def _get_last_etl_project_runs(db_session: Session, period: PeriodParams, used_cache: deque, backend_uri: str):
    last_runs = await _get_last_etl_runs_from_backend_api(period, backend_uri)
    ids_projects_for_update = [
        (run.etl_project_id, run.etl_project_version) for run in last_runs
        if run.etl_run.id not in used_cache
    ]

    last_runs_by_project = _to_dict_view(last_runs, ids_projects_for_update)

    projects_versions: List[EtlProjectVersion] = (
        db_session.query(EtlProjectVersion)
        .options(joinedload(EtlProjectVersion.etl_project))
        .filter(
            tuple_(
                EtlProjectVersion.etl_project_id, EtlProjectVersion.version
            ).in_(last_runs_by_project)
        )
        .all()
    )

    return projects_versions, last_runs_by_project


def _to_dict_view(runs: List[LastRunsForEtlProject], include: List[Tuple[int, str]]):
    return {
        (run.etl_project_id, run.etl_project_version): run.etl_run for run in runs
        if (run.etl_project_id, run.etl_project_version) in include
    }


async def _get_last_etl_runs_from_backend_api(period: PeriodParams, backend_uri: str) -> List[LastRunsForEtlProject]:
    url = join_urls(backend_uri, "internal", "etl", "get_last_etl_runs_for_projects")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"period": period.value, "period_type": period.type.value}) as response:
            if not (200 <= response.status < 300):
                logger.error(f"Bad response from backend uri {backend_uri}: {response.status}")
                raise Exception(f"can not get etl runs from backend_api: {url}")
            result = await response.json()

        last_runs = [LastRunsForEtlProject.parse_obj(run) for run in result]

    logger.info(f"last runs from backend_api: {backend_uri}: {result}")

    return last_runs


async def _disable_project_version_monitoring(etl_project_version: EtlProjectVersion):
    url = join_urls(
        settings.metric_manager_uri,
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
            logger.error(
               f"An error occurred while disabling monitoring for version "
               f"(etl_id: {etl_project_version.etl_project_id}, version: {etl_project_version.version}): "
               f"\n{response_text}"
            )

        return await response.json()


async def _turn_off_active_project_version(
    etl_project_version: EtlProjectVersion,
    db_session: Session,
):
    turned_off_version: Optional[EtlProjectVersion] = data_storage.turn_off_active_project_version(
        db_session,
        new_etl_version_id=etl_project_version.id,
        etl_project_id=etl_project_version.etl_project_id,
        author_name=etl_project_version.author_name,
    )
    if not turned_off_version:
        return None

    if settings.use_metric_manager:
        # Monitoring disabling is optional process, so we don`t need to wait for the task to complete
        asyncio.create_task(_disable_project_version_monitoring(turned_off_version))
