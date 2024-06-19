import os
import re
import time
import traceback
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty
from tempfile import TemporaryDirectory

import yaml

from fs_general_api.extra_processes.synchronizer.exceptions import (
    ProjectScanSynchronizerError,
)
from fs_general_api.extra_processes.synchronizer.definitions import (
    SynchronizeEventRequest,
    SynchronizeEventResponse,
)
from fs_general_api.extra_processes.mixins import GitProjectClonerMixin
from fs_general_api.config import GitConnProtocol


class EtlProjectSynchronizer(GitProjectClonerMixin, Process):
    """
    Класс для запуска синхронизации расписания запусков ETL-проекта в отдельном процессе.

    Синхронизация происходит между приоритетным расписанием указаным в `settings.yaml` и
    зафиксированным в таблицах `etl_projects` сервисов `fs_general_api` и `fs_backend_api`.

    """

    SETTINGS_FILE_NAME = "settings.yaml"
    CRON_EXPRESSION_PATTERN = (
        "^(\*|([0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])|\*\/([0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])) "
        "(\*|([0-9]|1[0-9]|2[0-3])|\*\/([0-9]|1[0-9]|2[0-3])) "
        "(\*|([1-9]|1[0-9]|2[0-9]|3[0-1])|\*\/([1-9]|1[0-9]|2[0-9]|3[0-1])) "
        "(\*|([1-9]|1[0-2])|\*\/([1-9]|1[0-2])) (\*|([0-6])|\*\/([0-6]))$"
    )

    def __init__(
        self,
        event_queue: Queue,
        ctrl_queue: Queue,
        ctrl_event: Event,
        git_conn_protocol: GitConnProtocol,
        git_username: str,
        git_password: str,
    ) -> None:
        Process.__init__(self)
        GitProjectClonerMixin.__init__(
            self, git_conn_protocol, git_username, git_password
        )

        self.event_queue: Queue = event_queue
        self.ctrl_event: Event = ctrl_event
        self.ctrl_queue: Queue = ctrl_queue

    def run(self) -> None:
        while self.ctrl_event.is_set():
            try:
                synchronize_request: SynchronizeEventRequest = (
                    self.event_queue.get_nowait()
                )
                self._process_synchronize(
                    synchronize_request=synchronize_request
                )
            except Empty:
                pass

            time.sleep(1)

    def _process_synchronize(
        self, synchronize_request: SynchronizeEventRequest
    ) -> None:
        synchronize_result = SynchronizeEventResponse(
            synchronize_event_request=synchronize_request
        )

        try:
            with TemporaryDirectory() as tempdir:
                git_path = Path(tempdir)
                project_path = git_path / synchronize_request.etl_project_name

                repo = self._clone_project(
                    project_path=git_path,
                    git_repo_url=synchronize_request.git_repo,
                )
                repo.git.checkout(synchronize_request.branch_name)

                schedule_interval = self._scan_project_for_schedule_interval(
                    project_path=project_path
                )

        except Exception as exc:
            synchronize_result.error_message = f"{exc} for ETL-project with id={synchronize_request.etl_project_id}\n{traceback.format_exc()}"
            self.ctrl_queue.put(synchronize_result)

        else:
            synchronize_result.schedule_interval = schedule_interval
            self.ctrl_queue.put(synchronize_result)

    def _scan_project_for_schedule_interval(self, project_path: Path) -> str:
        if self.SETTINGS_FILE_NAME not in os.listdir(project_path):
            raise ProjectScanSynchronizerError

        with open(
            os.path.join(project_path, self.SETTINGS_FILE_NAME), "r"
        ) as f:
            try:
                settings_mapping = yaml.safe_load(f)
            except yaml.YAMLError:
                raise ProjectScanSynchronizerError

        schedule_interval = settings_mapping.get("dag_settings", {}).get(
            "schedule_interval", ""
        )

        if not re.search(self.CRON_EXPRESSION_PATTERN, schedule_interval):
            raise ProjectScanSynchronizerError

        return schedule_interval
