import mmap
import os
from abc import abstractmethod, ABC
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from fs_common_lib.fs_general_api.data_types import (
    ProjectCheckResult,
    CheckType,
    ProjectType,
    SimpleCheckResult,
)


@dataclass
class UserRequestData:
    user_name: str
    author_email: str
    author_name: str


@dataclass
class CheckResult:
    description: str
    result: SimpleCheckResult = SimpleCheckResult.FAILED


@dataclass
class CheckEventRequest:
    etl_project_id: int
    etl_project_version: str
    etl_project_name: str
    jira_task: str
    branch_name: str
    general_check_id: int
    check_type: CheckType
    git_repo: str
    project_type: ProjectType
    user_data: Optional[UserRequestData] = None


@dataclass
class CheckEventResponse:
    result: ProjectCheckResult
    checks: List[CheckResult]
    check_event_request: CheckEventRequest


class CheckInterface(ABC):
    def __init__(self, path: Path, project_type: ProjectType):
        self.path: Path = path
        self.project_type: ProjectType = project_type

    def run(self) -> CheckResult:
        """
        Метод для запуска проверки
        """
        if not self.path.exists() or not self.path.is_dir():
            return CheckResult(
                description="Отсутствует директория с проектом",
                result=SimpleCheckResult.FAILED,
            )

        return self._process_check()

    @abstractmethod
    def _process_check(self) -> CheckResult:
        ...


class FeatureDescriptionCheck(CheckInterface):
    """
    Проверка на наличие описания фичей
    """

    def _process_check(self) -> CheckResult:
        return CheckResult(
            description="Все фичи описаны", result=SimpleCheckResult.SUCCESS
        )


class RequiredFilesCheck(CheckInterface):
    """
    Проверка на наличие необходимых файлов в ETL-проекте
    """

    def _process_check(self) -> CheckResult:
        res = CheckResult(
            description="Структура ETL-проекта корректна",
            result=SimpleCheckResult.SUCCESS,
        )
        project_struct = os.listdir(self.path)

        ref_gen_items = {
            'settings.yaml',
            'requirements.txt',
            'features.yaml' if ProjectType(self.project_type) == ProjectType.FEATURES else (
                'aggregates.yaml' if ProjectType(self.project_type) == ProjectType.AGGREGATES else 'targets.yaml')
        }

        missed_items = []

        for item in ref_gen_items:
            if item not in project_struct:
                missed_items.append(item)

        if len(missed_items) > 0:
            res = CheckResult(
                description=f"В директории проекта отсутствуют следующие элементы: {', '.join(item for item in missed_items)}",
                result=SimpleCheckResult.FAILED,
            )

        return res


class ToPandasFilesContentCheck(CheckInterface):
    """
    Проверка на наличие метода `to_pandas` в исходном коде ETL-проекта

    NB: При необходимости добавить доп. проверку контенка файлов ETL-проекта можно
        преобразовать текущий класс в общий интерфейс `FilesContentCheckInterface`.
        Добавить больше абстракции в `_process_check`, а `_process_find` преобразовать в
        абстрактный NotImplemented метод.
    """

    SEARCHED_FILE_CONTENT = (".toPandas()",)

    def _process_find(self, filename: str) -> bool:
        with open(
            os.path.join(self.path, filename), "rb", 0
        ) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
            for token in self.SEARCHED_FILE_CONTENT:
                if s.find(token.encode()) != -1:
                    return True

            return False

    def _process_check(self) -> CheckResult:
        # ищем рекурсивно, пропускаем скрытые файлы и папки, в т.ч. .ipynb_checkpoints, пропускаем __pycache__
        # и пропускаем пустые файлы, чтобы mmap не поплохело
        python_files = [
            file_path
            for file_path in self.path.rglob("*.py")
            if str(file_path.absolute()).find("/.") < 0
            and str(file_path.absolute()).find("/__pycache__/") < 0
            and file_path.stat().st_size > 0
        ]

        files_with_match = []
        for python_file_path in python_files:
            if self._process_find(str(python_file_path.absolute())):
                files_with_match.append(str(python_file_path))

        if files_with_match:
            result = CheckResult(
                description=(
                    f"В исходном коде ETL-проекта (файлы: {', '.join(item for item in files_with_match)})"
                    f"обнаружено использование методов: {', '.join(item for item in self.SEARCHED_FILE_CONTENT)}. "
                    "Обращаем ваше внимание на то, что использование этих методов является нежелательным и "
                    "может привести к непредвиденным ошибкам при работе DAG."
                ),
                result=SimpleCheckResult.WARNING,
            )
        else:
            result = CheckResult(
                description="В исходном коде ETL-проекта не обнаружено использование нежелательных методов",
                result=SimpleCheckResult.SUCCESS,
            )

        return result
