import asyncio
import datetime
import json
from dataclasses import dataclass

import asyncssh
from fs_common_lib.fs_registry_api.pydantic_classes import PdtDagRunConf, DagRunType
from fs_common_lib.fs_logger.fs_logger import FsLoggerHandler

from fs_general_api.config import settings

logger = FsLoggerHandler(__name__,
                         level=settings.log_level,
                         log_format=settings.log_format,
                         datefmt=settings.datefmt).get_logger()


@dataclass
class SshBackfillRequest:
    start_date: datetime.date
    end_date: datetime.date
    dag_id: str
    dag_run_type: DagRunType


class AirflowBackfillService:
    airflow_date_format = "%Y-%m-%d"

    def __init__(self, airflow_ssh_host: str, airflow_ssh_port: str, airflow_ssh_user: str, airflow_ssh_password: str):
        self._ssh_config = {
            "host": airflow_ssh_host,
            "port": airflow_ssh_port,
            "username": airflow_ssh_user,
            "password": airflow_ssh_password,
            "known_hosts": None,
        }

    async def start_backfill(self, backfill_request: SshBackfillRequest):
        start_date = backfill_request.start_date.strftime(self.airflow_date_format)
        end_date = backfill_request.end_date.strftime(self.airflow_date_format)
        conf_data_request = PdtDagRunConf(run_type=backfill_request.dag_run_type)
        conf_json_str = conf_data_request.json()

        async with asyncssh.connect(**self._ssh_config) as conn:
            cmd = f"airflow dags backfill --reset-dagruns -y --conf='{conf_json_str}' -s {start_date} -e {end_date} {backfill_request.dag_id}"
            logger.info(f"cmd for airflow: {cmd}")
            result = await conn.run(cmd, request_pty="force")

            logger.info(f"Backfill command result: {result.stdout}")

            if result.stderr:
                logger.error(f"Backfill command error: {result.stderr}")


class SshBackfillQueueHandler:
    def __init__(self, backfill_service: AirflowBackfillService):
        self._backfill_service = backfill_service
        self._workers = []

    async def start_workers(self, number: int, q: asyncio.Queue):
        workers = [asyncio.create_task(self._ssh_airflow_backfill_worker(q)) for _ in range(number)]
        self._workers.extend(workers)

    async def stop_workers(self):
        for worker in self._workers:
            worker.cancel()

    async def _ssh_airflow_backfill_worker(self, q: asyncio.Queue):
        while True:
            task = await q.get()
            logger.info(f"Backfill task got: {task}")

            try:
                await self._backfill_service.start_backfill(task)
            except Exception as e:
                logger.exception(f"exception while running backfill task: {e}")

            q.task_done()
            logger.info(f"Backfill task {task} completed")
