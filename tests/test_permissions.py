from typing import List, Tuple

import pytest
from fs_common_lib.fs_backend_proxy.pdt import BackendProxyUser
from fs_common_lib.fs_backend_proxy.data_types import FsRoleTypes
from fs_db.db_classes_general import EtlProjectVersion, EtlProjectStatus, EtlProject

from fs_general_api.permissions import get_permissions_for_user_with_project, is_update_etl_project, is_edit_team, \
    is_start_autocheck, is_start_retro_calc, is_update_datamart, is_send_to_review, is_start_code_sync_in_mdp, is_delete_etl_project


def _generate_test_cases_for_roles_with_statuses(roles: List[FsRoleTypes], statues: List[EtlProjectStatus],
                                                 is_owner: bool):
    cases = []

    for role in roles:
        for status in statues:
            cases.append((role, is_owner, status))

    return cases


class TestUserPermission:
    all_roles = [FsRoleTypes(e.value) for e in FsRoleTypes if FsRoleTypes(e.value) != FsRoleTypes.FS_ADMIN]

    @staticmethod
    def get_user(role: FsRoleTypes):
        return BackendProxyUser(id=5, groups={role})

    @staticmethod
    def get_etl_project(owner_id: int, status: EtlProjectStatus) -> EtlProjectVersion:
        return EtlProjectVersion(
            user_id=owner_id,
            status=status
        )

    @staticmethod
    def get_etl_project_version_with_main_project(owner_id: int, status: EtlProjectStatus, etl_project: EtlProject):
        version = EtlProjectVersion(user_id=owner_id, status=status)
        etl_project.versions.append(version)
        return version

    def test_get_full_permissions(self, etl_project_1):
        user = self.get_user(FsRoleTypes.DATA_SCIENTIST)
        etl_project = self.get_etl_project_version_with_main_project(owner_id=user.id, status=EtlProjectStatus.PRODUCTION, etl_project=etl_project_1)
        permissions = get_permissions_for_user_with_project(user, etl_project)
        assert permissions.update_datamart is True
        assert permissions.start_retro_calc is True
        assert permissions.update_etl_project is True
        assert permissions.delete_etl_project is False
        assert permissions.edit_team is True
        assert permissions.start_auto_check is False
        assert permissions.start_code_sync_in_mdp is False
        assert permissions.send_to_review is False
        assert permissions.create_new_version is True


    roles_for_update_etl_project = [FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.MLOPS_ENGINEER]
    statuses_for_update_etl_project = [EtlProjectStatus.PRODUCTION]
    roles_without_access_for_update_etl_project = list(set(all_roles) - set(roles_for_update_etl_project))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_update_etl_project, statuses_for_update_etl_project, True))
    def test_update_etl_project(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_update_etl_project(user, etl_project) is True

    roles_for_delete_etl_project = [FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS]
    statuses_for_delete_etl_project = [EtlProjectStatus.DEVELOPING]
    roles_without_access_for_delete_etl_project = list(set(all_roles) - set(roles_for_delete_etl_project))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_delete_etl_project, statuses_for_delete_etl_project, True))
    def test_delete_etl_project(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_delete_etl_project(user, etl_project) is True

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_delete_etl_project, statuses_for_delete_etl_project, False))
    def test_delete_etl_project_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_delete_etl_project(user, etl_project) is False

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_delete_etl_project, statuses_for_delete_etl_project, False))
    def test_delete_etl_project_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_delete_etl_project(user, etl_project) is False

    roles_for_sync_code_in_mdp = [FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS,]
    statuses_for_sync_code_in_mdp = [EtlProjectStatus.DEVELOPING, EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.TESTING]
    roles_without_access_for_sync_code_in_mdp = list(set(all_roles) - set(roles_for_sync_code_in_mdp))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_sync_code_in_mdp, statuses_for_sync_code_in_mdp, True))
    def test_start_code_sync_in_mdp(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_start_code_sync_in_mdp(user, etl_project) is True

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_sync_code_in_mdp, statuses_for_sync_code_in_mdp, False))
    def test_start_code_sync_in_mdp_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_start_code_sync_in_mdp(user, etl_project) is False

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_sync_code_in_mdp, statuses_for_sync_code_in_mdp, False))
    def test_start_code_sync_in_mdp_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_start_code_sync_in_mdp(user, etl_project) is False

    roles_for_send_to_review = [FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.MLOPS_ENGINEER]
    statuses_for_send_to_review = [EtlProjectStatus.TESTING]
    roles_without_access_for_send_to_review = list(set(all_roles) - set(roles_for_send_to_review))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_send_to_review, statuses_for_send_to_review, True))
    def test_send_to_review(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_send_to_review(user, etl_project) is True

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_send_to_review, statuses_for_send_to_review, False))
    def test_send_to_review_for_not_owner(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_send_to_review(user, etl_project) is False

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_send_to_review, statuses_for_send_to_review, False))
    def test_send_to_review_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_send_to_review(user, etl_project) is False

    roles_for_absolute_update_cassandra = []
    statuses_for_absolute_update_cassandra = [EtlProjectStatus.PRODUCTION, EtlProjectStatus.PROD_REVIEW]
    roles_without_access_for_update_cassandra = list(set(all_roles) - set(roles_for_absolute_update_cassandra))


    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_update_cassandra, statuses_for_absolute_update_cassandra, False))
    def test_update_cassandra_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_update_datamart(user, etl_project) is False


    roles_for_edit_team = [FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.MLOPS_ENGINEER]
    statuses_for_edit_team = [EtlProjectStatus.DEVELOPING, EtlProjectStatus.PRODUCTION, EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.PROD_RELEASE]
    roles_without_access_for_edit_team = list(set(all_roles) - set(roles_for_edit_team))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_edit_team, statuses_for_edit_team, True))
    def test_edit_team(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_edit_team(user, etl_project) is True

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_edit_team, statuses_for_edit_team, False))
    def test_edit_team_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_edit_team(user, etl_project) is False

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_edit_team, statuses_for_edit_team, False))
    def test_edit_team_for_not_owner(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_edit_team(user, etl_project) is False

    roles_for_retro_calculating = [FsRoleTypes.DATA_SCIENTIST]
    roles_for_absolute_retro_calculating = [FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DE]
    statuses_for_retro_calculating = [EtlProjectStatus.PRODUCTION]
    roles_wihout_access_for_retro_calc = list(set(all_roles) - set(roles_for_retro_calculating) - set(roles_for_absolute_retro_calculating))

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_for_retro_calculating, statuses_for_retro_calculating, True))
    def test_retro_calculating(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_start_retro_calc(user, etl_project) is True

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_wihout_access_for_retro_calc, statuses_for_retro_calculating, False))
    def test_retro_calculating_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_start_retro_calc(user, etl_project) is False


    roles_for_autocheck = [FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS,
                           FsRoleTypes.MLOPS_ENGINEER, FsRoleTypes.DATA_ENGINEER]
    statuses_for_autocheck = [EtlProjectStatus.TESTING]

    roles_without_access_for_autocheck = set(all_roles) - set(roles_for_autocheck)

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(
        roles_for_autocheck, statuses_for_autocheck, True))
    def test_start_autocheck(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=user.id, status=status)
        assert is_start_autocheck(user, etl_project) is is_owner

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(
        roles_for_autocheck,
        statuses_for_autocheck, True))
    def test_start_autocheck_for_not_owner(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_start_autocheck(user, etl_project) is False

    @pytest.mark.parametrize("role,is_owner,status", _generate_test_cases_for_roles_with_statuses(roles_without_access_for_autocheck, statuses_for_autocheck, False))
    def test_start_autocheck_for_not_owner_without_access(self, role, is_owner, status):
        user = self.get_user(role)
        etl_project = self.get_etl_project(owner_id=1, status=status)
        assert is_start_autocheck(user, etl_project) is False
