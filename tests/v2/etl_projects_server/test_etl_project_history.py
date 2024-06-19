import pytest

from fs_db.db_classes_general import EtlProjectVersion, HistoryEvent


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlHistoryEventsView:
    url = "v2/etl/{etl_id}/history_event"

    def test_success(
        self,
        db,
        client,
        etl_project_1_version_2: EtlProjectVersion,
        history_event_1: HistoryEvent,
        history_event_2: HistoryEvent,
    ):
        response = client.get(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            params={"version": etl_project_1_version_2.version},
        )

        assert response.status_code == 200

        assert {
            "author": history_event_1.author,
            "created_timestamp": history_event_1.created_timestamp.isoformat(),
            "extra_data": history_event_1.extra_data,
            "id": history_event_1.id,
            "name": history_event_1.name,
            "new_value": history_event_1.new_value,
            "old_value": history_event_1.old_value,
        } in response.json()["items"]

        assert {
            "author": history_event_2.author,
            "created_timestamp": history_event_2.created_timestamp.isoformat(),
            "extra_data": history_event_2.extra_data,
            "id": history_event_2.id,
            "name": history_event_2.name,
            "new_value": history_event_2.new_value,
            "old_value": history_event_2.old_value,
        } in response.json()["items"]
