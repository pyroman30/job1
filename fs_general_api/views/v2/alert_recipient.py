from typing import Optional

from fastapi.responses import JSONResponse
from fastapi import Body, Depends
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi.encoders import jsonable_encoder

from sqlalchemy.orm import Session
from fs_db.db_classes_general import AlertRecipient
from fs_common_lib.fs_general_api.data_types import AlertType

from fs_general_api.views import BaseRouter
from fs_general_api.exceptions import (
    RecordAlreadyExistsException,
    DataNotFoundException,
)
from fs_general_api.dto.alert_recipient import PdtAlertRecipient
from fs_general_api.db import db

alert_recipients_router = InferringRouter()


@cbv(alert_recipients_router)
class AlertRecipientServer(BaseRouter):
    __model_class__ = AlertRecipient

    route = "/admin/alerting"

    @alert_recipients_router.get(path=route + "/list", response_model=dict)
    def get_list(
        self,
        hub_id: int = None,
        alert_type: Optional[AlertType] = AlertType.ALL,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        session: Session = Depends(db.get_session),
    ) -> JSONResponse:
        """
        Возвращает JSONResponse, содержащий список получателей уведомлений
        Args:
            hub_id: - id Хаба
            alert_type: - тип уведомлени (Хаб или ВСЕ)
            limit: - количество возвращаемых записей
            offset: - отступ, откуда необходимо брать необходимое число записей
        """
        self.logger.info(f"hub_id - {hub_id}\nalert_type - {alert_type}")
        query = session.query(AlertRecipient)

        if alert_type == AlertType.HUB and hub_id:
            query = query.filter(AlertRecipient.hub_id == hub_id)
        elif alert_type == AlertType.HUB and not hub_id:
            query = query.filter(AlertRecipient.alert_type == AlertType.HUB)
        elif alert_type == AlertType.ALL:
            query = query.filter(AlertRecipient.alert_type == AlertType.ALL)

        total = query.count()

        if offset is None:
            offset = 0
        if limit is None:
            limit = total

        query = query.limit(limit).offset(offset)
        dataset = query.all()
        pdt_dataset = PdtAlertRecipient.get_entity(dataset)
        self.logger.info(pdt_dataset)

        return JSONResponse(
            jsonable_encoder({"items": pdt_dataset, "total": total})
        )

    @alert_recipients_router.post(path=route, response_model=dict)
    def add_new_alert_recipient(
        self, alert: dict = Body(), session: Session = Depends(db.get_session)
    ) -> JSONResponse:
        """
        Возвращает alert_recipient, полученный после создания
        Args:
            alert: объект, содержащий словарь с атрибутами получателя уведомлений
                hub_id: - id Хаба
                alert_type: - тип уведомлени (Хаб или ВСЕ)
                display_name: - отображаемое имя
                email: - почта получателя уведомлений
                description: - описание
        """
        alert = PdtAlertRecipient.get_entity(alert.get("alert"))

        alert_recipient = self.get_all(
            session, filter_={"display_name": alert.display_name}
        )

        if alert_recipient:
            raise RecordAlreadyExistsException(
                f"Alert recipient with name=`{alert.display_name} already exists!`"
            )

        alert_recipient = AlertRecipient(
            hub_id=alert.hub_id,
            alert_type=alert.alert_type,
            display_name=alert.display_name,
            email=alert.email,
            description=alert.description,
        )

        session.add(alert_recipient)
        session.commit()

        pdt_alert_recipient = PdtAlertRecipient.get_entity(alert_recipient)

        return JSONResponse(jsonable_encoder(pdt_alert_recipient))

    @alert_recipients_router.delete(path=route + "/{alert_recipient_id}")
    def delete_alert_recipient(
        self,
        alert_recipient_id: int,
        session: Session = Depends(db.get_session),
    ):
        """
        Удаляет alert_recipient, по его id
        Args:
            alert_recipient_id: id получателя уведомлений
        """
        dataset = self.get_by_id(session, alert_recipient_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Alert recipient by alert_recipient_id=`{alert_recipient_id}` didn't find!"
            )

        session.delete(dataset)
        session.commit()

    @alert_recipients_router.put(
        path=route + "/{alert_recipient_id}", response_model=dict
    )
    def update_project(
        self,
        alert_recipient_id: int,
        alert: dict = Body(),
        session: Session = Depends(db.get_session),
    ) -> JSONResponse:
        """
        Обновляет alert_recipient по его id
        Возвращает обновленный alert_recipient
        Args:
            alert_recipient_id: id получателя уведомлений
            alert: объект, содержащий словарь с атрибутами получателя уведомлений
                hub_id: - id Хаба
                alert_type: - тип уведомлени (Хаб или ВСЕ)
                display_name: - отображаемое имя
                email: - почта получателя уведомлений
                description: - описание
        """
        dataset = self.get_by_id(session, alert_recipient_id)

        if dataset is None:
            raise DataNotFoundException(
                f"Alert recipient by alert_recipient_id=`{alert_recipient_id}` didn't find!"
            )
        alert = PdtAlertRecipient.get_entity(alert.get("alert"))
        dataset.hub_id = alert.hub_id
        dataset.alert_type = alert.alert_type
        dataset.display_name = alert.display_name
        dataset.email = alert.email
        dataset.description = alert.description

        session.commit()
        pdt_alert_recipient = PdtAlertRecipient.get_entity(dataset)

        return JSONResponse(jsonable_encoder(pdt_alert_recipient))
