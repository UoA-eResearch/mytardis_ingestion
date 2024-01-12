# pylint: disable=logging-fstring-interpolation
# pylint: disable-all
# type: ignore
# noqa
# nosec

"""Performs the mining process

The mining process involves generating metadata files for MyTardis ingestion.
"""


# ---Imports
import logging
from pathlib import Path
from typing import Optional

from src.extraction import output_manager as om
from src.miners.abstract_custom_miner import AbstractCustomMiner
from src.utils.types.singleton import Singleton

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Miner(metaclass=Singleton):
    """Miner class for mining files according to a profile"""

    def __init__(
        self,
        custom_miner: AbstractCustomMiner,
    ) -> None:
        """Initialize the miner object.

        Args:
            profile (str): Name of the profile to use for mining.
        """
        self.custom_miner = custom_miner

    def mine_directory(
        self,
        path: str,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """Mine a directory according to the selected profile

        Args:
            path (str): Path of the directory to mine.
            recursive (bool): Whether to recursively mine subdirectories.
            out_man (om.OutputManager): Output Manager to store the results.

        Returns:
            om.OutputManager: Output Manager with the mined results.
        """
        if not out_man:
            out_man = om.OutputManager()

        if self.custom_miner:
            out_man_fnl: om.OutputManager = self.custom_miner.mine(
                path, recursive, out_man
            )
        else:
            out_man_fnl = out_man
            logger.info("No custom miner set, thus will not be used")

        logger.info("mining complete")
        logger.info(f"ignored dirs = {out_man_fnl.dirs_to_ignore}")
        logger.info(f"ignored files = {out_man_fnl.files_to_ignore}")
        logger.info(f"files to ingest = {out_man_fnl.metadata_files_to_ingest_dict}")
        logger.info(f"output dict = {out_man_fnl.output_dict}")

        return out_man_fnl
