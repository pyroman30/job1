import asyncio
from multiprocessing import Queue, Event

from fastapi import FastAPI

from fs_general_api.config import settings
from fs_general_api.event_handler import EventHandler
from fs_general_api.extra_processes import (
    EtlProjectChecker,
    EtlProjectSynchronizer,
)
from fs_general_api.extra_processes.etl_status.setup import setup_etl_status_task
from fs_general_api.extra_processes.ssh_executor.worker import AirflowBackfillService, SshBackfillQueueHandler
from fs_general_api.exceptions.handlers import add_exception_handlers
from fs_general_api.views import BaseRouter
from fs_general_api.views.healthcheck import healthcheck_router
from fs_general_api.views.internal.alert_recipient import (
    internal_alert_recipients_router,
)
from fs_general_api.views.internal.etl_project import (
    internal_etl_project_router,
)
from fs_general_api.views.v1.alert_recipient import (
    alert_recipients_router as v1_alert_recipients_router,
)
from fs_general_api.views.v1.etl_project import (
    etl_project_router as v1_etl_project_router,
)
from fs_general_api.views.v1.hub import hubs_router as v1_hub_router
from fs_general_api.views.v2.alert_recipient import (
    alert_recipients_router as v2_alert_recipients_router,
)
from fs_general_api.views.v2.etl_project import (
    etl_project_router as v2_etl_project_router,
)
from fs_general_api.views.v2.hub import hubs_router as v2_hub_router

app = FastAPI()


root = FastAPI()
v1 = FastAPI()
v2 = FastAPI()
internal = FastAPI()

root.include_router(healthcheck_router)

v1.include_router(v1_etl_project_router)
v1.include_router(v1_hub_router)
v1.include_router(v1_alert_recipients_router)

v2.include_router(v2_etl_project_router)
v2.include_router(v2_hub_router)
v2.include_router(v2_alert_recipients_router)

internal.include_router(internal_etl_project_router)
internal.include_router(internal_alert_recipients_router)

app.mount("/internal", internal)
# app.mount('/v1', v1)
app.mount("/v2", v2)
app.mount("/", root)

setup_etl_status_task(app)

add_exception_handlers(internal)
add_exception_handlers(v1)
add_exception_handlers(v2)
add_exception_handlers(root)


checker_event_queue = Queue()
checker_ctrl_event = Event()
checker_ctrl_event.set()

synchronizer_event_queue = Queue()
synchronizer_ctrl_event = Event()
synchronizer_ctrl_event.set()

event_handler_ctrl_queue = Queue()

ssh_backfill_request_queue = asyncio.Queue()

BaseRouter.checker_event_queue = checker_event_queue
BaseRouter.synchronizer_event_queue = synchronizer_event_queue
BaseRouter.backfill_request_queue = ssh_backfill_request_queue

event_handler = EventHandler(ctrl_queue=event_handler_ctrl_queue)

etl_project_checker = EtlProjectChecker(
    event_queue=checker_event_queue,
    ctrl_queue=event_handler_ctrl_queue,
    ctrl_event=checker_ctrl_event,
    git_conn_protocol=settings.git_conn_protocol,
    git_username=settings.git_username,
    git_password=settings.git_password,
)

etl_project_synchronizer = EtlProjectSynchronizer(
    event_queue=synchronizer_event_queue,
    ctrl_queue=event_handler_ctrl_queue,
    ctrl_event=synchronizer_ctrl_event,
    git_conn_protocol=settings.git_conn_protocol,
    git_username=settings.git_username,
    git_password=settings.git_password,
)

airflow_backfill_service = AirflowBackfillService(settings.airflow_ssh_host_prod,
                                                  settings.airflow_ssh_port_prod,
                                                  settings.airflow_ssh_username_prod,
                                                  settings.airflow_ssh_password_prod
                                                  )

backfill_executor = SshBackfillQueueHandler(airflow_backfill_service)


etl_project_synchronizer.start()
etl_project_checker.start()
event_handler.start()


def shutdown_processes():
    synchronizer_ctrl_event.clear()
    checker_ctrl_event.clear()
    event_handler.is_started = False


@app.on_event("shutdown")
async def shutdown_event():
    shutdown_processes()
    await backfill_executor.stop_workers()


@app.on_event("startup")
async def startup_event():
    await backfill_executor.start_workers(settings.backfill_ssh_session_max_number, ssh_backfill_request_queue)


if __name__ == "__main__":
    """
    uvicorn server:app --reload
    """

    import uvicorn

    uvicorn.run(
        "server:app", host="127.0.0.1", port=8001, log_level="info"
    )  # localhost
