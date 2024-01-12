# pylint: disable-all
# type: ignore
# noqa
# nosec

"""Prospects the datasets for checking raw and processed folders.
"""


# ---Imports
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.extraction import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DatasetProspector:
    """Prospects the datasets for checking raw and processed folders."""

    def __init__(
        self,
    ) -> None:
        return None

    def check_for_raw_and_processed_folder_pairs(
        self,
        path: str,
    ) -> None:
        """Checks whether there is a pair of corresponding raw and processed folders,
        and log any missing folders that complete the pair

        Args:
            path (str): the path of the folders to check for corresponding pairs
        """
        dsets = []
        for root, dirs, files in os.walk(path):
            rel_path = os.path.relpath(root, path)
            if rel_path == "":
                continue
            elif rel_path.count(os.sep) != 2:
                continue

            dsets.append(rel_path)

        dsets_lut = dict.fromkeys(dsets)
        used_lut: dict[str, int] = {}
        for item in dsets:
            dset_basename = item.rsplit("-")[0]
            if dset_basename in used_lut:
                if amc.RAW_FOLDER_SUFFIX in item:
                    used_lut[dset_basename] += 1
                elif amc.DECONV_FOLDER_SUFFIX in item:
                    used_lut[dset_basename] += 2
                continue
            dsets_lut = {k: v for k, v in dsets_lut.items() if not dset_basename in k}
            if amc.RAW_FOLDER_SUFFIX in item:
                used_lut[dset_basename] = 1
            elif amc.DECONV_FOLDER_SUFFIX in item:
                used_lut[dset_basename] = 2
            else:
                logger.warning(
                    "folder {0} does not have a conforming suffix. Should end in one of {1}".format(
                        item, amc.FOLDER_SUFFIX_KEYS
                    )
                )

        for key in used_lut.keys():
            if used_lut[key] == 1:
                logger.warning("missing processed folder for {0}".format(key))
            elif used_lut[key] == 2:
                logger.warning("missing raw folder for {0}".format(key))
