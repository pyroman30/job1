import time
import traceback
from collections import defaultdict
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty
from tempfile import TemporaryDirectory
from typing import List

from fs_common_lib.fs_general_api.data_types import (
    ProjectCheckResult,
    SimpleCheckResult,
)

from fs_general_api.extra_processes.checker.check import (
    RequiredFilesCheck,
    CheckResult,
    CheckEventRequest,
    CheckEventResponse,
    ToPandasFilesContentCheck,
)
from fs_general_api.extra_processes.mixins import GitProjectClonerMixin
from fs_general_api.config import GitConnProtocol


class EtlProjectChecker(GitProjectClonerMixin, Process):
    """Класс для запуска проверок в отдельном процессе"""

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
        self.ctrl_queue: Queue = ctrl_queue
        self.ctrl_event: Event = ctrl_event

    def run(self) -> None:
        while self.ctrl_event.is_set():
            try:
                check_request: CheckEventRequest = self.event_queue.get_nowait()
                self._process_checks(check_request=check_request)
            except Empty:
                pass

            time.sleep(1)

    def _process_checks(self, check_request: CheckEventRequest) -> None:
        checks_res: List[CheckResult] = []

        checks_classes = (
            RequiredFilesCheck,
            ToPandasFilesContentCheck,
        )
        checks_statuses_count_mapping = defaultdict(int)

        try:
            with TemporaryDirectory() as tempdir:
                git_path = Path(tempdir)
                project_path = git_path / check_request.etl_project_name
                repo = self._clone_project(
                    project_path=git_path, git_repo_url=check_request.git_repo
                )
                repo.git.checkout(check_request.branch_name)

                # Здесь добавляем необходимые проверки для исходного кода ETL проектов
                for check_cls in checks_classes:
                    check_result = check_cls(
                        path=project_path,
                        project_type=check_request.project_type,
                    ).run()
                    checks_statuses_count_mapping[check_result.result] += 1
                    checks_res.append(check_result)

        except Exception as exc:
            checks_statuses_count_mapping[SimpleCheckResult.FAILED] += 1
            checks_res.append(
                CheckResult(
                    description=f"{exc}\n{traceback.format_exc()}",
                    result=SimpleCheckResult.FAILED,
                )
            )

        if checks_statuses_count_mapping[SimpleCheckResult.FAILED]:
            event_result = ProjectCheckResult.FAILED
        else:
            event_result = ProjectCheckResult.SUCCESS

        response = CheckEventResponse(
            result=event_result,
            checks=checks_res,
            check_event_request=check_request,
        )

        self.ctrl_queue.put(response)
