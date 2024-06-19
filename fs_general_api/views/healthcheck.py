from datetime import datetime

from fastapi import status
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter

from fs_general_api.config import settings

healthcheck_router = InferringRouter()


@cbv(healthcheck_router)
class HealthCheck:
    route = "/healthcheck"

    @healthcheck_router.get(path=route, status_code=status.HTTP_200_OK)
    async def get_status(self):
        return {
            "status": "healthy",
            "stand": settings.schema_name,
            "dt": f"{datetime.now()}",
        }
