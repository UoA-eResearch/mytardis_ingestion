# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

"""Defines the methodology to inspect metadata and perform related checks on a path.
"""


# ---Imports
import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from src.extraction_output_manager import output_manager as om
# from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc
from src.profiles.abi_music.prospector_helpers.dataset_prospector import (
    DatasetProspector,
)
from src.profiles.abi_music.prospector_helpers.directory_prospector import (
    DirectoryProspector,
)
from src.profiles.abi_music.prospector_helpers.metadata_prospector import (
    MetadataProspector,
)

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class CustomProspector:
    """Profile-specific prospector class

    Each profile has a custom prospector whose behaviour is based on the
    requirements of the researcher
    """

    def __init__(
        self,
    ) -> None:
        """Do not modify this method"""
        return None

    def prospect(
        self,
        path: str,
        recursive: bool,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """Prospects metadata in a path

        Args:
            path (str): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        if not out_man:
            out_man = om.OutputManager()

        # Write the main inspection implementation here
        logger.info("Checking for corresponding raw-processed folder pairs")
        dset_prosp = DatasetProspector()
        dset_prosp.check_for_raw_and_processed_folder_pairs(path)

        logger.info(
            "Checking for whether .json metadata file matches the file's path prescribed in its metadata"
        )
        dir_prosp = DirectoryProspector()
        out_man1, metadata_fp_list = dir_prosp.check_json_folder_path_mismatch(
            path, out_man
        )

        logger.info("Checking files outside dataset folders")
        out_man2: om.OutputManager = dir_prosp.check_for_files_outside_dataset(
            path, out_man1
        )

        logger.info("Checking metadata files for their required fields")
        md_prosp = MetadataProspector()
        out_man3: om.OutputManager = md_prosp.check_metadata_for_mining(
            out_man2, metadata_fp_list
        )

        return out_man3
