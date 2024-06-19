import pytest

from fs_db.db_classes_general import EtlProjectVersion


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
    "etl_project_1_version_2",
    "etl_project_2_version_1",
)
class TestEtlSendToProdView:
    url = "v2/etl/{etl_id}/send_to_prod"

    def test_success(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
    ):
        response = client.post(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            json={
                "version": etl_project_2_version_1.version,
                "data": {
                    "user_name": "user_name",
                    "author_name": "author_name",
                    "author_email": "author_email",
                },
            },
        )

        assert response.status_code == 200

    def test_transfer_already_exists(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        transfer_request_1,
    ):
        response = client.post(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            json={
                "version": etl_project_2_version_1.version,
                "data": {
                    "user_name": "user_name",
                    "author_name": "author_name",
                    "author_email": "author_email",
                },
            },
        )

        assert response.status_code == 409
        assert (
            response.json()
            == "Transfer to production already requested for this project!"
        )

    def test_general_check_processing(
        self,
        db,
        client,
        etl_project_2_version_1: EtlProjectVersion,
        general_check_1,
    ):
        response = client.post(
            self.url.format(etl_id=etl_project_2_version_1.etl_project_id),
            json={
                "version": etl_project_2_version_1.version,
                "data": {
                    "user_name": "user_name",
                    "author_name": "author_name",
                    "author_email": "author_email",
                },
            },
        )

        assert response.status_code == 409
        assert (
            response.json() == "Pre-checks already requested for this project!"
        )

    def test_version_not_in_testing(
        self,
        db,
        client,
        etl_project_1_version_2: EtlProjectVersion,
    ):
        response = client.post(
            self.url.format(etl_id=etl_project_1_version_2.etl_project_id),
            json={
                "version": etl_project_1_version_2.version,
                "data": {
                    "user_name": "user_name",
                    "author_name": "author_name",
                    "author_email": "author_email",
                },
            },
        )

        assert response.status_code == 409
        assert (
            response.json()
            == "You cannot send etl project to production without successful runs!"
        )
