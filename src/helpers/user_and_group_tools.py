# pylint: disable=logging-fstring-interpolation
"""Helper functions to parse users and groups appropriately"""
import logging
from typing import List, Tuple

from src.blueprints import Username

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
