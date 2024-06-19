from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from starlette.requests import Request

from fs_general_api.exceptions import (
    DataNotFoundException,
    ThirdPartyServiceError,
    StatusUpdateError,
    RecordAlreadyExistsException,
    ProjectModificationError,
    NotEnoughDataException,
)


def add_exception_handlers(_app: FastAPI):
    @_app.exception_handler(DataNotFoundException)
    async def data_not_found(request: Request, exc: DataNotFoundException):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND, content=str(exc)
        )

    @_app.exception_handler(RecordAlreadyExistsException)
    async def record_already_exists(
        request: Request, exc: RecordAlreadyExistsException
    ):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT, content=str(exc)
        )

    @_app.exception_handler(ThirdPartyServiceError)
    async def third_party_service_error(
        request: Request, exc: ThirdPartyServiceError
    ):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=str(exc)
        )

    @_app.exception_handler(StatusUpdateError)
    async def status_update_error(request: Request, exc: StatusUpdateError):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT, content=str(exc)
        )

    @_app.exception_handler(ProjectModificationError)
    async def project_modification_error(
        request: Request, exc: ProjectModificationError
    ):
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN, content=str(exc)
        )

    @_app.exception_handler(NotEnoughDataException)
    async def not_enough_data_exception(
        request: Request, exc: NotEnoughDataException
    ):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND, content=str(exc)
        )
