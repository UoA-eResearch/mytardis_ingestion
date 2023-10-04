# pylint: disable=logging-fstring-interpolation
"""Performs the prospecting process

Prospecting checks files and metadata files for any potential issues.
"""

# ---Imports
import logging
from pathlib import Path
from typing import Optional, Union

from src.config.singleton import Singleton
from src.extraction_output_manager import output_manager as om
from src.prospectors.abstract_custom_prospector import AbstractCustomProspector
from src.prospectors.common_file_checks import CommonDirectoryTreeChecks

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Prospector(metaclass=Singleton):
    """This class Prospector is used to prospect a given directory for files.

    Attributes:
        profile_sel (profile_selector.ProfileSelector): selected profile module
    """

    def __init__(
        self,
        custom_prospector: Union[AbstractCustomProspector, None],
    ) -> None:
        self.custom_prospector = custom_prospector

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
            out_man = om.OutputManager()

        cmn_dir_tree_c = CommonDirectoryTreeChecks()

        out = cmn_dir_tree_c.perform_common_file_checks(path, recursive)
        rej_list = out[0]
        rej_list_pth = [Path(filestring) for filestring in rej_list]
        out_man.add_files_to_ignore(rej_list_pth)

        if self.custom_prospector:
            out_man_fnl = self.custom_prospector.prospect(
                Path(path), recursive, out_man
            )
        else:
            out_man_fnl = out_man
            logger.info("No custom prospector set, thus will not be used")

        logger.info("prospecting complete")
        logger.info(f"ignored dirs = {out_man_fnl.dirs_to_ignore}")
        logger.info(f"ignored files = {out_man_fnl.files_to_ignore}")
        logger.info(f"files to ingest = {out_man_fnl.metadata_files_to_ingest_dict}")
        logger.info(f"output dict = {out_man_fnl.output_dict}")

        return out_man_fnl
