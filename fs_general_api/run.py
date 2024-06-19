import os
from pathlib import Path

repo_path = Path(__file__).parent.absolute()

os.environ["PYTHONPATH"] = f"{repo_path.parent.absolute()};{repo_path}"
os.environ["DB_USER"] = "postgres"
os.environ["DB_PASS"] = "postgres"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_PORT"] = "5432"
os.environ["DB_NAME"] = "postgres"
os.environ["SCHEMA_NAME"] = "stable_general"
os.environ["LOG_LEVEL"] = "INFO"
os.environ[
    "LOG_FORMAT"
] = "%(asctime)s.%(msecs)03d [%(levelname)s] - %(name)s - %(funcName)s - %(lineno)s - %(message)s"
os.environ["DATEFMT"] = "%Y-%m-%d %I:%M:%S"
os.environ["GIT_MANAGER_URI"] = "http://127.0.0.1:8100"
os.environ["USE_GIT_MANAGER"] = "False"
os.environ["BACKEND_URI_DEV"] = "http://127.0.0.1:8000"
os.environ["BACKEND_URI_PROD"] = "http://127.0.0.1:8000"

os.system(f"python {repo_path}/server.py")
