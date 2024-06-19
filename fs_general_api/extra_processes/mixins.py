import re
from pathlib import Path
from typing import Union, Optional

from git import Repo

from fs_general_api.config import GitConnProtocol


class GitProjectClonerMixin:
    def __init__(
        self,
        git_conn_protocol: GitConnProtocol,
        git_username: str,
        git_password: str,
    ):
        self.git_conn_protocol: GitConnProtocol = git_conn_protocol
        self.git_username: str = git_username
        self.git_password: str = git_password

    def _clone_project(self, project_path: Path, git_repo_url: str) -> Repo:
        if (
            self.git_conn_protocol == GitConnProtocol.HTTP
            or self.git_conn_protocol == GitConnProtocol.HTTPS
        ):
            if self.git_username is None or self.git_password is None:
                raise ValueError(
                    f"settings git_username and git_password should be set"
                )

        return self._clone_repo_to_path(
            to_path=project_path,
            repo_url=git_repo_url,
            conn_protocol=self.git_conn_protocol,
            username=self.git_username,
            password=self.git_password,
        )

    @staticmethod
    def _clone_repo_to_path(
        to_path: Union[str, Path],
        repo_url: str,
        conn_protocol: GitConnProtocol,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Repo:
        if (
            conn_protocol == GitConnProtocol.HTTP
            or conn_protocol == GitConnProtocol.HTTPS
        ):
            assert username is not None
            assert password is not None
            password = re.escape(password)
            repo_url = (
                f"{conn_protocol.value}://{username}:{password}@{repo_url}"
            )
        elif conn_protocol == GitConnProtocol.SSH:
            repo_url = f"ssh://{repo_url}"
        else:
            assert False

        to_path = Path(to_path).absolute()
        repo = Repo.clone_from(url=repo_url, to_path=to_path)
        return repo
