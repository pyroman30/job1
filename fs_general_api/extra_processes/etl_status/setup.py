from collections import deque

from sqlalchemy.orm import Session
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every

from fs_general_api.config import settings
from fs_general_api.db import task_db_session
from fs_general_api.extra_processes.etl_status.etl_status_checker import update_etl_projects_statuses


def setup_etl_status_task(app: FastAPI):
    dev_etl_run_cache = deque([], maxlen=100)
    prod_etl_run_cache = deque([], maxlen=100)

    @app.on_event("startup")
    @repeat_every(
        seconds=settings.etl_status_update_timeout * 60,
        raise_exceptions=False,
        wait_first=True,
    )
    @task_db_session
    async def update_etl_project_status(db_session: Session):
        await update_etl_projects_statuses(db_session, dev_etl_run_cache, prod_etl_run_cache)
