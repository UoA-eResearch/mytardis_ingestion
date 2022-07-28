# pylint: disable=logging-fstring-interpolation
"""Helper functions to parse users and groups appropriately"""
import logging
from typing import List, Tuple

from src.blueprints import GroupACL, UserACL, Username

logger = logging.getLogger(__name__)


def set_access_controls(
    combined_names: List[Username | str],
    download_names: List[str],
    sensitive_names: List[str],
) -> List[Tuple[str, bool, bool, bool]]:
    """Helper function to set the access controls for combined list of
    users/groups.

    Args:
        combined_names: a set of names of users/groups that is complete for this
            ingestion object.
        download_names: a list of names of users/groups that have download access
        sensitive_names: a list of names of users/groups that have sensitive access

    Returns:
        A list of tuples containing the access controls.
    """
    return_list = []
    for name in combined_names:
        download = False
        sensitive = False
        if name in download_names:
            download = True
        if name in sensitive_names:
            sensitive = True
        return_list.append((name, False, download, sensitive))
    return return_list


def parse_groups_and_users_from_separate_access(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    cleaned_input: dict,
) -> Tuple[List[UserACL], List[GroupACL]]:
    """A helper function to parse a set of separate users and groups into a single
    list of users and groups with access levels included

    The output format is (user/group, isOwner, hasDownload, hasSensitive)

    Args:
        cleaned_dict: a dictionary after the keys have been normalised

    Returns:
        A tuple containing the lists of users and groups with access levels
    """
    groups = []
    users = []
    read_groups = []
    read_users = []
    download_groups = []
    download_users = []
    sensitive_groups = []
    sensitive_users = []
    if "principal_investigator" in cleaned_input.keys():
        if "admin_users" in cleaned_input.keys():
            cleaned_input["admin_users"].append(cleaned_input["principal_investigator"])
        else:
            cleaned_input["admin_users"] = [cleaned_input["principal_investigator"]]
    if "admin_users" in cleaned_input.keys():
        for user in set(cleaned_input.pop("admin_users")):
            try:
                users.append((Username(user), True, True, True))
            except ValueError:
                logger.warning(
                    (
                        f"Unable to add user {user} to the list of "
                        "admin users due to a malformed username"
                    ),
                    exc_info=True,
                )
                continue
    if "admin_groups" in cleaned_input.keys():
        for group in set(cleaned_input.pop("admin_groups")):
            groups.append((group, True, True, True))
    try:
        read_users = cleaned_input.pop("read_users")
    except KeyError:  # not present in the dictionary so ignore
        pass
    try:
        read_groups = cleaned_input.pop("read_groups")
    except KeyError:  # not present in the dictionary so ignore
        pass
    try:
        download_users = cleaned_input.pop("download_users")
    except KeyError:  # not present in the dictionary so ignore
        pass
    try:
        download_groups = cleaned_input.pop("download_groups")
    except KeyError:  # not present in the dictionary so ignore
        pass
    try:
        sensitive_users = cleaned_input.pop("sensitive_users")
    except KeyError:  # not present in the dictionary so ignore
        pass
    try:
        sensitive_groups = cleaned_input.pop("sensitive_groups")
    except KeyError:  # not present in the dictionary so ignore
        pass
    combined_users = list(set(read_users + download_users + sensitive_users))
    combined_usernames = []
    for user in combined_users:
        try:
            combined_usernames.append(Username(user))
        except ValueError:
            logger.warning(
                (
                    "While processing users for access control "
                    f"User {user} is a malformed Username and has "
                    "been removed from the list"
                ),
                exc_info=True,
            )
            continue
    combined_groups = list(set(read_groups + download_groups + sensitive_groups))
    users.extend(
        set_access_controls(combined_usernames, download_users, sensitive_users)
    )
    groups.extend(
        set_access_controls(combined_groups, download_groups, sensitive_groups)
    )
    validated_users = []
    validated_groups = []
    for user in sorted(users):
        validated_users.append(
            UserACL(
                user=user[0],
                is_owner=user[1],
                can_download=user[2],
                see_sensitive=user[3],
            )
        )
    for group in sorted(groups):
        validated_groups.append(
            GroupACL(
                group=group[0],
                is_owner=group[1],
                can_download=group[2],
                see_sensitive=group[3],
            )
        )
    return (validated_users, validated_groups)
