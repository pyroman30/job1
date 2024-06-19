from typing import Optional, List
from pydantic import BaseSettings
from enum import Enum


class GitConnProtocol(Enum):
    HTTP = "http"
    HTTPS = "https"
    SSH = "ssh"


class Settings(BaseSettings):
    schema_name: str
    db_user: str
    db_pass: str
    db_host: str
    db_port: str
    db_name: str
    backend_uri_dev: str
    backend_uri_prod: str
    backend_proxy_url: str = "http://fs-backend-proxy:8080"
    metric_manager_uri: Optional[str] = None
    use_metric_manager: bool = False
    git_manager_uri: Optional[str] = None
    use_git_manager: bool = False
    use_git_manager_for_deletion: bool = False
    log_level: str = "INFO"
    log_format: str = "%(asctime)s.%(msecs)03d [%(levelname)s] - %(name)s - %(funcName)s - %(lineno)s - %(message)s"
    datefmt: str = "%Y-%m-%d %I:%M:%S"
    jira_uri: str = "https://jira.moscow.alfaintra.net/browse/"
    git_uri_prod: str = "https://git.moscow.alfaintra.net/projects/AFM/repos/feature_store_dags_prod/browse/"
    git_uri_dev: str = "https://git.moscow.alfaintra.net/projects/AFM/repos/features_dev/browse/"
    git_repo_dev: str = "git.moscow.alfaintra.net/scm/afm/features_dev.git"
    git_repo_prod: str = (
        "git.moscow.alfaintra.net/scm/afm/feature_store_dags_prod.git"
    )
    git_conn_protocol: GitConnProtocol = GitConnProtocol.HTTP
    git_username: Optional[str] = None
    git_password: Optional[str] = None
    etl_status_update_timeout: int = 2
    server_port: int = 8000

    retro_metric_start_version_require: str = "0.23.24"

    airflow_ssh_host_prod: str = "0.0.0.0"
    airflow_ssh_port_prod: int = 22
    airflow_ssh_username_prod: Optional[str] = "default"
    airflow_ssh_password_prod: Optional[str] = "default"

    backfill_ssh_session_max_number: int = 5

    pr_target_project_reviewers: List[str] = []

    @property
    def connection_uri(self):
        return f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
