import pytest

from fs_common_lib.fs_registry_api import join_urls

from fs_general_api.config import settings
from fs_general_api.views.v2.dto.etl_project import EtlProjectUserPermissionsPdt


@pytest.mark.usefixtures(
    "etl_project_1_version_1",
)
class TestEtlUserPermissionForProject:
    url = "v2/etl/{etl_id}/permissions"

    def test_get_permissions(self, client, etl_project_1_version_1, mock_aioresponse):
        backend_proxy_url = join_urls(
            settings.backend_proxy_url,
            "user-info"
        )

        mock_aioresponse.get(backend_proxy_url, status=200, payload={"roles": ["DATA_SCIENTIST"], "id": 1})

        resp = client.get(self.url.format(etl_id=etl_project_1_version_1.etl_project_id), params={"version": etl_project_1_version_1.version})

        answer = resp.json()
        perms = EtlProjectUserPermissionsPdt.parse_obj(answer)

        assert perms.delete_etl_project is False




