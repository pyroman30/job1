from typing import Set

from fs_common_lib.fs_backend_proxy.pdt import BackendProxyUser
from fs_db.db_classes_general import EtlProjectVersion, EtlProjectStatus
from fs_common_lib.fs_backend_proxy.data_types import FsRoleTypes

from fs_general_api.views.v2.dto.etl_project import EtlProjectUserPermissionsPdt


def get_permissions_for_user_with_project(user: BackendProxyUser,
                                          project: EtlProjectVersion) -> EtlProjectUserPermissionsPdt:
    return EtlProjectUserPermissionsPdt(update_datamart=is_update_datamart(user, project),
                                        start_retro_calc=is_start_retro_calc(user, project),
                                        update_etl_project=is_update_etl_project(user, project),
                                        delete_etl_project=is_delete_etl_project(user, project),
                                        edit_team=is_edit_team(user, project),
                                        start_auto_check=is_start_autocheck(user, project),
                                        start_code_sync_in_mdp=is_start_code_sync_in_mdp(user, project),
                                        send_to_review=is_send_to_review(user, project),
                                        create_new_version=is_create_new_version(user, project)
                                        )


def is_update_datamart(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.MLOPS_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS}
    absolute_roles = set()

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner)) or _is_user_has_valid_group(user, absolute_roles)
    is_status_ok = project.status in {EtlProjectStatus.PRODUCTION}

    return is_role_ok and is_status_ok


def is_start_retro_calc(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS}
    absolute_roles = set()

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner)) or _is_user_has_valid_group(user, absolute_roles)
    is_status_ok = project.status in {EtlProjectStatus.PRODUCTION}

    return is_role_ok and is_status_ok


def is_delete_etl_project(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    return _is_owner(user, project) and project.status in {EtlProjectStatus.DEVELOPING}


def is_edit_team(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.MLOPS_ENGINEER}

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner))
    is_status_ok = project.status in {EtlProjectStatus.DEVELOPING, EtlProjectStatus.TESTING, EtlProjectStatus.PRODUCTION, EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.PROD_RELEASE}

    return is_role_ok and is_status_ok


def is_start_autocheck(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.MLOPS_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS}

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner))
    is_status_ok = project.status in {EtlProjectStatus.DEVELOPING, EtlProjectStatus.TESTING}

    return is_role_ok and is_status_ok


def is_start_code_sync_in_mdp(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.MLOPS_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS}

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner))
    is_status_ok = project.status in {EtlProjectStatus.DEVELOPING, EtlProjectStatus.TESTING, EtlProjectStatus.PROD_REVIEW}

    return is_role_ok and is_status_ok


def is_send_to_review(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.MLOPS_ENGINEER, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.DATA_ENGINEER, FsRoleTypes.TEAM_LEAD_DS, FsRoleTypes.DATA_SCIENTIST}

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner))
    is_status_ok = project.status == EtlProjectStatus.TESTING

    return is_role_ok and is_status_ok


def is_update_etl_project(user: BackendProxyUser, project: EtlProjectVersion) -> bool:

    is_role_ok = _is_admin(user) or _is_owner(user, project)
    is_status_ok = project.status in {EtlProjectStatus.DEVELOPING, EtlProjectStatus.TESTING, EtlProjectStatus.PRODUCTION, EtlProjectStatus.PROD_REVIEW, EtlProjectStatus.PROD_RELEASE}

    return is_role_ok and is_status_ok


def is_create_new_version(user: BackendProxyUser, project: EtlProjectVersion) -> bool:
    roles_for_owner = {FsRoleTypes.DATA_ENGINEER, FsRoleTypes.DATA_SCIENTIST, FsRoleTypes.TEAM_LEAD_DE, FsRoleTypes.TEAM_LEAD_DS}
    absolute_roles = set()

    is_role_ok = _is_admin(user) or (_is_owner(user, project) and _is_user_has_valid_group(user, roles_for_owner)) or _is_user_has_valid_group(user, absolute_roles)
    is_status_ok = project.status == EtlProjectStatus.PRODUCTION

    is_new_versions_already_exists = len([version for version in project.etl_project.versions if version.status not in (EtlProjectStatus.PRODUCTION, EtlProjectStatus.TURNED_OFF)]) > 0

    return is_role_ok and is_status_ok and not is_new_versions_already_exists


def _is_admin(user: BackendProxyUser):
    return FsRoleTypes.FS_ADMIN in user.groups


def _is_user_has_valid_group(user: BackendProxyUser, groups: Set[FsRoleTypes]) -> bool:
    return bool(set(user.groups) & groups)


def _is_owner(user: BackendProxyUser, project: EtlProjectVersion):
    return user.id == project.user_id