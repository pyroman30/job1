from sqlalchemy.orm import Session
from fastapi import Depends
from fastapi.responses import JSONResponse
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi.encoders import jsonable_encoder

from fs_db.db_classes_general import AlertRecipient

from fs_common_lib.fs_general_api.data_types import AlertType

from fs_general_api.views import BaseRouter
from fs_general_api.db import db

internal_alert_recipients_router = InferringRouter()


@cbv(internal_alert_recipients_router)
class InternalAlertRecipientServer(BaseRouter):
    __model_class__ = AlertRecipient

    route = "/alerting"

    @internal_alert_recipients_router.get(
        path=route + "/list", response_model=dict
    )
    def get_list(
        self, hub_id: int = None, session: Session = Depends(db.get_session)
    ) -> JSONResponse:
        """
        Возвращает JSONResponse, содержащий список emails получателей
        Args:
            hub_id: - id Хаба
        """
        self.logger.info(f"hub_id - {hub_id}")
        query = session.query(AlertRecipient.email)

        if hub_id:
            query = query.filter(AlertRecipient.hub_id == hub_id)
        else:
            query = query.filter(AlertRecipient.alert_type == AlertType.ALL)

        emails = [email_t[0] for email_t in query.all()]

        return JSONResponse(jsonable_encoder(emails))
