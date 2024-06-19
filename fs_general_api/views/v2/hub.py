from typing import Optional

from fastapi.responses import JSONResponse
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi.encoders import jsonable_encoder
from fastapi import Depends
from sqlalchemy.orm import Session

from fs_db.db_classes_general import Hub

from fs_general_api.views import BaseRouter
from fs_general_api.exceptions import DataNotFoundException
from fs_general_api.dto.hub import PdtHub
from fs_general_api.db import db

hubs_router = InferringRouter()


@cbv(hubs_router)
class HubsServer(BaseRouter):
    __model_class__ = Hub

    route = "/hub/"

    @hubs_router.get(path=route + "list", response_model=dict)
    def get_list(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        session: Session = Depends(db.get_session),
    ) -> JSONResponse:
        """
        Возвращает JSONResponse, содержащий список hubs
        Args:
            limit: - количество возвращаемых записей
            offset: - отступ, откуда необходимо брать необходимое число записей
        """
        dataset = self.get_all(session)

        total = len(dataset)

        if offset is None:
            offset = 0
        if limit is None:
            limit = total

        pdt_dataset = PdtHub.get_entity(dataset[offset : offset + limit])

        return JSONResponse(
            jsonable_encoder({"items": pdt_dataset, "total": total})
        )

    @hubs_router.get(path=route + "{hub_id}", response_model=dict)
    def get_hub(
        self, hub_id: int, session: Session = Depends(db.get_session)
    ) -> JSONResponse:
        """
        Возвращает hub, полученный по hub_id
        hub_id: id хаба, который необходимо получить
        """
        dataset = self.get_by_id(session, hub_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Hub by hub_id=`{hub_id}` didn't find!"
            )

        content = PdtHub.get_entity(dataset)
        return JSONResponse(jsonable_encoder(content))
