"""Performs the prospecting process

Prospecting checks files and metadata files for any potential issues.
"""

# ---Imports
import copy
import logging

from src.profiles import output_manager as om
from src.profiles import profile_selector
from src.prospectors.common_file_checks import CommonDirectoryTreeChecks
from src.prospectors.metadata_check import MetadataCheck
from typing import Optional

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Prospector:
    """This class Prospector is used to prospect a given directory for files.

    Attributes:
        profile_sel (profile_selector.ProfileSelector): selected profile module
    """

    def __init__(
        self,
        profile: str,
    ) -> None:
        self.profile_sel = profile_selector.ProfileSelector(profile)

    def prospect_directory(
        self,
        path: str,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """
        This method prospect_directory is used to prospect a given directory for files.

        Parameters:
            path (str): Path to directory.
            recursive (bool): Whether to recursively search for files.
            out_man (om.OutputManager): Output manager instance.

        Returns:
            om.OutputManager: Output manager instance.
        """

        if not out_man:
            new_out_man = om.OutputManager()
        else:
            new_out_man = copy.deepcopy(out_man)
        cmn_dir_tree_c = CommonDirectoryTreeChecks()
        metadata_c = MetadataCheck()

        out = cmn_dir_tree_c.perform_common_file_checks(path, recursive)
        rej_list = out[0]
        new_out_man.add_files_to_ignore(rej_list)

        new_out_man = metadata_c.check_metadata_in_path(
            self.profile_sel, path, recursive, new_out_man
        )

        return new_out_man
